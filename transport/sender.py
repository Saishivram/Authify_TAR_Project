"""
transport/sender.py
===================
UDP Sender process for Phase 4 of the TAR protocol.

Responsibilities:
1. Reads canonical records from input JSONL file.
2. Binds local UDP socket (default 127.0.0.1:9001) targeting receiver (127.0.0.1:9004).
3. Bounded START handshake.
4. Integrated Transmission & Loss Recovery engine:
   - Paced sending bounded by send_window (default 200).
   - RecoveryCoordinator tracking per-record states (QUEUED, IN_FLIGHT, BUFFERED, REPAIR_PENDING, DELIVERED).
   - RACK loss detection with adaptive reordering window.
   - PTO tail loss probing with fresh packet IDs without marking transmissions lost.
   - Fallback RTO with exponential backoff on complete channel stall.
   - Dispatches repairs with fresh packet IDs and incremented attempts.
5. Gap-Aware Teardown:
   - Sends FIN and awaits FIN_ACK.
   - If receiver responds with SACK containing missing gaps, sender services repairs and re-transmits FIN.
6. Measures throughput, duration, retransmissions, and max datagram sizes.
"""

import argparse
import json
import os
import select
import socket
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from transport.packet import (
    DEFAULT_RECEIVE_BUFFER_SIZE,
    MAX_DATAGRAM_BYTES,
    AckPacket,
    DataPacket,
    ErrorPacket,
    FinAckPacket,
    FinPacket,
    InvalidPacketError,
    OversizedDatagramError,
    ProbePacket,
    StartAckPacket,
    StartPacket,
    deserialize_packet,
    serialize_packet,
)
from transport.recovery import RecordState, RecoveryCoordinator
from transport.rtt import RttEstimator


class ConnectionHandshakeError(Exception):
    """Raised when the session START/START_ACK handshake fails."""
    pass


class ConnectionTeardownError(Exception):
    """Raised when the session FIN/FIN_ACK teardown fails."""
    pass


class UdpSender:
    """
    Standard-library UDP socket sender for TAR transport with RACK, PTO, SACK recovery.
    """

    def __init__(
        self,
        target_host: str = "127.0.0.1",
        target_port: int = 9004,
        bind_host: str = "127.0.0.1",
        bind_port: int = 9001,
        max_retries: int = 5,
        retry_timeout: float = 0.5,
        send_window: int = 200,
        rtt_estimator: Optional[RttEstimator] = None,
    ):
        self.target_addr = (target_host, target_port)
        self.bind_addr = (bind_host, bind_port)
        self.max_retries = max_retries
        self.retry_timeout = retry_timeout
        self.send_window = send_window

        self.sock: Optional[socket.socket] = None
        self.next_packet_id: int = 1

        # Recovery engine
        self.rtt_estimator = rtt_estimator or RttEstimator(
            initial_rto=min(0.35, retry_timeout),
            min_rto=min(0.2, retry_timeout),
            max_rto=0.55,
        )
        self.coordinator = RecoveryCoordinator(rtt_estimator=self.rtt_estimator)

        # Performance & Reliability Metrics
        self.sent_records: int = 0
        self.retransmissions_count: int = 0
        self.probes_count: int = 0
        self.spurious_retransmissions_count: int = 0
        self.total_data_bytes_sent: int = 0
        self.max_actual_datagram_size: int = 0
        self.start_retries_used: int = 0
        self.fin_retries_used: int = 0
        self.state: str = "IDLE"
        self.stop_requested: bool = False
        self.session_start_time: float = 0.0
        self.expected_records: int = 0

    def start_socket(self) -> None:
        """Initializes and binds the local UDP socket."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except OSError:
                pass
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        except OSError:
            pass
        try:
            self.sock.bind(self.bind_addr)
        except OSError as exc:
            print(f"[Sender] Bind to {self.bind_addr} failed ({exc}); binding to ephemeral port.")
            self.sock.bind((self.bind_addr[0], 0))

    def close(self) -> None:
        """Closes the UDP socket."""
        if self.sock:
            self.sock.close()
            self.sock = None

    def stop(self) -> None:
        """Signals the sender to stop cleanly."""
        self.stop_requested = True
        self.state = "STOPPED"
        self.close()

    def get_metrics(self) -> Dict[str, Any]:
        """Returns live or final sender transmission metrics."""
        now = time.monotonic()
        data_duration = max(0.000001, now - self.session_start_time) if self.session_start_time > 0 else 0.0
        throughput = round(self.sent_records / data_duration, 2) if data_duration > 0.05 else 0.0
        return {
            "status": self.state,
            "sent_records": self.sent_records,
            "expected_records": self.expected_records,
            "retransmissions_count": self.retransmissions_count,
            "probes_count": self.probes_count,
            "spurious_retransmissions_count": self.spurious_retransmissions_count,
            "total_data_bytes_sent": self.total_data_bytes_sent,
            "max_actual_datagram_size_bytes": self.max_actual_datagram_size,
            "in_flight_count": getattr(self.coordinator, "in_flight_count", 0),
            "receiver_delivered_through": getattr(self.coordinator, "receiver_delivered_through", 0),
            "srtt_ms": round(self.rtt_estimator.srtt * 1000.0, 2) if self.rtt_estimator and self.rtt_estimator.srtt is not None else 0.0,
            "rto_ms": round(self.rtt_estimator.rto * 1000.0, 2) if self.rtt_estimator and self.rtt_estimator.rto is not None else 200.0,
            "throughput_rec_per_sec": throughput,
        }

    def _handshake_start(self, session_id: str, expected_records: int) -> StartAckPacket:
        """Executes bounded retries for START until START_ACK is received."""
        start_pkt = StartPacket(
            connection_id=session_id,
            expected_records=expected_records,
            initial_rate=2000,
        )
        raw_start = serialize_packet(start_pkt)
        self.max_actual_datagram_size = max(self.max_actual_datagram_size, len(raw_start))

        for attempt in range(1, self.max_retries + 1):
            print(f"[Sender] Sending START (attempt {attempt}/{self.max_retries}) for session '{session_id}'...")
            self.sock.sendto(raw_start, self.target_addr)
            self.sock.settimeout(self.retry_timeout)

            try:
                raw_ack, _ = self.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                pkt = deserialize_packet(raw_ack)

                if isinstance(pkt, StartAckPacket) and pkt.connection_id == session_id:
                    print(f"[Sender] Handshake SUCCESS: Received START_ACK (status={pkt.status}).")
                    self.start_retries_used = attempt - 1
                    return pkt
                elif isinstance(pkt, ErrorPacket) and pkt.connection_id == session_id:
                    raise ConnectionHandshakeError(f"Receiver rejected START: {pkt.code} - {pkt.message}")

            except (socket.timeout, ConnectionResetError):
                print(f"[Sender] START timeout or reset ({self.retry_timeout}s). Retrying...")
                continue
            except (InvalidPacketError, OversizedDatagramError) as exc:
                print(f"[Sender] Received invalid packet during handshake: {exc}")
                continue

        raise ConnectionHandshakeError(
            f"Failed to establish session after {self.max_retries} START attempts"
        )

    def _send_data_packet(
        self,
        session_id: str,
        record_id: int,
        attempt: int,
        payload: Any,
    ) -> int:
        """Serializes and transmits a DataPacket."""
        pid = self.next_packet_id
        self.next_packet_id += 1

        pkt = DataPacket(
            connection_id=session_id,
            packet_id=pid,
            record_id=record_id,
            attempt=attempt,
            payload=payload,
        )
        raw_bytes = serialize_packet(pkt, max_bytes=MAX_DATAGRAM_BYTES)
        self.max_actual_datagram_size = max(self.max_actual_datagram_size, len(raw_bytes))
        self.total_data_bytes_sent += len(raw_bytes)

        now = time.monotonic()
        self.sock.sendto(raw_bytes, self.target_addr)
        self.coordinator.on_packet_sent(
            packet_id=pid,
            record_id=record_id,
            attempt=attempt,
            sent_at=now,
            is_probe=False,
        )
        return pid

    def _send_probe_packet(self, session_id: str, record_id: int) -> int:
        """Serializes and transmits a ProbePacket."""
        pid = self.next_packet_id
        self.next_packet_id += 1

        rec = self.coordinator.records.get(record_id)
        attempt = (rec.attempt_count if rec else 1)
        payload = (rec.payload if rec else None)
        self.probes_count += 1

        pkt = ProbePacket(
            connection_id=session_id,
            packet_id=pid,
            record_id=record_id,
            payload=payload,
            attempt=attempt,
            probe_number=self.probes_count,
        )
        raw_bytes = serialize_packet(pkt, max_bytes=MAX_DATAGRAM_BYTES)
        now = time.monotonic()
        self.sock.sendto(raw_bytes, self.target_addr)
        self.coordinator.on_packet_sent(
            packet_id=pid,
            record_id=record_id,
            attempt=attempt,
            sent_at=now,
            is_probe=True,
        )
        return pid

    def _drain_incoming_acks(self, session_id: str) -> Optional[FinAckPacket]:
        """Reads all pending incoming packets non-blocking."""
        fin_ack: Optional[FinAckPacket] = None
        now = time.monotonic()

        while True:
            try:
                r, _, _ = select.select([self.sock], [], [], 0.0)
                if not r:
                    break
                raw_bytes, _ = self.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                pkt = deserialize_packet(raw_bytes)

                if pkt.connection_id != session_id:
                    continue

                if isinstance(pkt, AckPacket):
                    self.coordinator.on_ack_received(pkt, now)
                elif isinstance(pkt, FinAckPacket):
                    fin_ack = pkt
                elif isinstance(pkt, ErrorPacket):
                    print(f"[Sender] Error packet from receiver: {pkt.code} - {pkt.message}")

            except (socket.timeout, BlockingIOError, ConnectionResetError):
                break
            except (InvalidPacketError, OversizedDatagramError) as exc:
                print(f"[Sender] Warning: received malformed packet: {exc}")
                continue

        return fin_ack

    def _teardown_fin(self, session_id: str, final_record_id: int, total_records: int) -> FinAckPacket:
        """
        Executes FIN teardown with repair support if receiver returns SACK indicating gaps.
        """
        fin_timeout = max(0.5, self.rtt_estimator.rto)

        for attempt in range(1, self.max_retries + 1):
            fin_pid = self.next_packet_id
            self.next_packet_id += 1
            fin_pkt = FinPacket(
                connection_id=session_id,
                packet_id=fin_pid,
                final_record_id=final_record_id,
                total_packets=total_records,
            )
            raw_fin = serialize_packet(fin_pkt)
            self.max_actual_datagram_size = max(self.max_actual_datagram_size, len(raw_fin))

            print(f"[Sender] Sending FIN (attempt {attempt}/{self.max_retries}, final_record_id={final_record_id})...")
            self.sock.sendto(raw_fin, self.target_addr)

            deadline = time.monotonic() + fin_timeout

            while time.monotonic() < deadline:
                remaining = max(0.01, deadline - time.monotonic())
                try:
                    self.sock.settimeout(min(0.1, remaining))
                    raw_ack, _ = self.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                    pkt = deserialize_packet(raw_ack)

                    if pkt.connection_id != session_id:
                        continue

                    if isinstance(pkt, FinAckPacket):
                        print(
                            f"[Sender] Teardown SUCCESS: Received FIN_ACK "
                            f"(delivered_through={pkt.receiver_delivered_through}, "
                            f"total_delivered={pkt.total_delivered_records})."
                        )
                        self.fin_retries_used = attempt - 1
                        return pkt

                    elif isinstance(pkt, AckPacket):
                        # Receiver indicates gaps remain
                        now = time.monotonic()
                        self.coordinator.on_ack_received(pkt, now)

                        # If repairs were queued, service them immediately
                        while self.coordinator.repair_queue:
                            r_id = self.coordinator.repair_queue.pop(0)
                            rec = self.coordinator.records.get(r_id)
                            if rec and rec.state in (RecordState.REPAIR_PENDING, RecordState.IN_FLIGHT):
                                self._send_data_packet(
                                    session_id=session_id,
                                    record_id=r_id,
                                    attempt=rec.attempt_count + 1,
                                    payload=rec.payload,
                                )
                                self.retransmissions_count += 1

                    elif isinstance(pkt, ErrorPacket):
                        raise ConnectionTeardownError(f"Receiver rejected FIN: {pkt.code} - {pkt.message}")

                except socket.timeout:
                    continue
                except (InvalidPacketError, OversizedDatagramError, ConnectionResetError):
                    continue

            print(f"[Sender] FIN attempt {attempt} timed out ({fin_timeout:.2f}s). Retrying...")

        raise ConnectionTeardownError(
            f"Failed to receive FIN_ACK after {self.max_retries} FIN attempts"
        )

    def transmit_file(
        self,
        input_file_path: str,
        session_id: Optional[str] = None,
        pace_interval: float = 0.0001,
    ) -> Dict[str, Any]:
        """
        Reads records and executes full transmission with RACK/SACK loss recovery.
        """
        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Input records file not found: {input_file_path}")

        records: List[Dict[str, Any]] = []
        with open(input_file_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    records.append(json.loads(stripped))

        if not records:
            raise ValueError(f"No records found in {input_file_path}")

        session = session_id or f"session-{int(time.time())}"
        expected_records = len(records)
        final_record_id = records[-1]["record_id"]

        # Register records with RecoveryCoordinator
        self.coordinator = RecoveryCoordinator(rtt_estimator=self.rtt_estimator)
        for rec in records:
            self.coordinator.register_record(rec["record_id"], rec["payload"])

        if not self.sock:
            self.start_socket()

        try:
            session_start_time = time.monotonic()
            self.session_start_time = session_start_time
            self.expected_records = expected_records
            self.state = "RUNNING"
            self.stop_requested = False

            # 1. Session Handshake
            self._handshake_start(session, expected_records)

            # 2. Transmission & Recovery Loop
            print(f"[Sender] Starting transmission of {expected_records} records to {self.target_addr}...")
            data_start_time = time.monotonic()
            self.sent_records = 0
            self.total_data_bytes_sent = 0
            self.retransmissions_count = 0
            self.probes_count = 0
            next_record_to_send = 1

            loop_idle_start: Optional[float] = None
            max_stall_allowance = 30.0  # Max 30s stall allowance before failing

            while True:
                if self.stop_requested:
                    print(f"[Sender] Stop requested. Aborting transmission.")
                    self.state = "STOPPED"
                    break

                now = time.monotonic()

                # A. Drain incoming ACKs
                self._drain_incoming_acks(session)

                # Check completion: all records contiguously delivered at receiver
                if self.coordinator.receiver_delivered_through >= expected_records:
                    break

                # B. Check Timeouts (RACK loss deadlines, Fallback RTO, PTO)
                records_to_repair, probe_record_id = self.coordinator.check_timeouts(now)

                # Dispatch pending repairs
                while self.coordinator.repair_queue:
                    repair_rid = self.coordinator.repair_queue.pop(0)
                    rec = self.coordinator.records.get(repair_rid)
                    if rec and rec.state in (RecordState.REPAIR_PENDING, RecordState.IN_FLIGHT):
                        if rec.attempt_count >= RecoveryCoordinator.MAX_RETRIES:
                            rec.state = RecordState.FAILED
                            raise RuntimeError(f"Retry limit exceeded for record {repair_rid}")
                        self._send_data_packet(
                            session_id=session,
                            record_id=repair_rid,
                            attempt=rec.attempt_count + 1,
                            payload=rec.payload,
                        )
                        self.retransmissions_count += 1

                # Dispatch PTO probe if scheduled
                if probe_record_id is not None:
                    self._send_probe_packet(session, probe_record_id)

                # C. Transmit fresh records if send window permits
                in_flight = self.coordinator.in_flight_count

                sent_in_batch = 0
                while in_flight < self.send_window and next_record_to_send <= expected_records:
                    rec_data = records[next_record_to_send - 1]
                    self._send_data_packet(
                        session_id=session,
                        record_id=rec_data["record_id"],
                        attempt=1,
                        payload=rec_data["payload"],
                    )
                    self.sent_records += 1
                    next_record_to_send += 1
                    in_flight = self.coordinator.in_flight_count
                    sent_in_batch += 1

                    if pace_interval > 0 and sent_in_batch % 20 == 0:
                        time.sleep(pace_interval)

                # D. If all records sent and none in flight, await remaining ACKs or FIN
                if next_record_to_send > expected_records and in_flight == 0:
                    if self.coordinator.receiver_delivered_through >= expected_records:
                        break

                # E. Safety idle timeout
                if in_flight > 0:
                    loop_idle_start = None
                else:
                    if loop_idle_start is None:
                        loop_idle_start = now
                    elif (now - loop_idle_start) > max_stall_allowance:
                        raise RuntimeError(f"Transmission loop stalled for > {max_stall_allowance}s")

                # If window is saturated or records are finished, wait briefly
                if in_flight >= self.send_window or next_record_to_send > expected_records:
                    time.sleep(0.0005)

            data_end_time = time.monotonic()
            data_duration = max(0.000001, data_end_time - data_start_time)

            # 3. Session Teardown
            self._teardown_fin(session, final_record_id, expected_records)
            session_end_time = time.monotonic()
            session_duration = max(0.000001, session_end_time - session_start_time)

            data_records_per_sec = round(self.sent_records / data_duration, 2)
            session_records_per_sec = round(self.sent_records / session_duration, 2)
            kb_per_sec = round((self.total_data_bytes_sent / 1024) / data_duration, 2)

            metrics = {
                "session_id": session,
                "input_file": input_file_path,
                "target_address": f"{self.target_addr[0]}:{self.target_addr[1]}",
                "sent_records": self.sent_records,
                "retransmissions_count": self.retransmissions_count,
                "probes_count": self.probes_count,
                "total_data_bytes_sent": self.total_data_bytes_sent,
                "max_actual_datagram_size_bytes": self.max_actual_datagram_size,
                "sender_data_loop_duration_seconds": round(data_duration, 4),
                "sender_data_loop_throughput_records_per_second": data_records_per_sec,
                "full_session_duration_seconds": round(session_duration, 4),
                "full_session_throughput_records_per_second": session_records_per_sec,
                "transmission_duration_seconds": round(data_duration, 4),
                "achieved_records_per_second": data_records_per_sec,
                "throughput_kb_per_second": kb_per_sec,
                "start_retries_count": self.start_retries_used,
                "fin_retries_count": self.fin_retries_used,
                "status": "COMPLETED",
            }
            self.state = "COMPLETED"
            return metrics

        except Exception as exc:
            self.state = "FAILED"
            raise exc
        finally:
            self.close()


def main():
    parser = argparse.ArgumentParser(description="TAR Protocol UDP Sender (Phase 4)")
    parser.add_argument("--input", type=str, default="data/records_2000.jsonl", help="Input JSONL path")
    parser.add_argument("--target-host", type=str, default="127.0.0.1", help="Target receiver host")
    parser.add_argument("--target-port", type=int, default=9004, help="Target receiver port")
    parser.add_argument("--bind-port", type=int, default=9001, help="Local bind port (default: 9001)")
    parser.add_argument("--session-id", type=str, default=None, help="Optional session connection ID")
    parser.add_argument("--pace-interval", type=float, default=0.0001, help="Pacing interval (seconds)")
    parser.add_argument("--window", type=int, default=200, help="Send window limit (default: 200)")
    args = parser.parse_args()

    sender = UdpSender(
        target_host=args.target_host,
        target_port=args.target_port,
        bind_port=args.bind_port,
        send_window=args.window,
    )

    try:
        metrics = sender.transmit_file(
            input_file_path=args.input,
            session_id=args.session_id,
            pace_interval=args.pace_interval,
        )
        print("\n=== SENDER TRANSMISSION REPORT ===")
        print(json.dumps(metrics, indent=2))
        sys.exit(0)
    except Exception as exc:
        print(f"\n[Sender] Fatal error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
