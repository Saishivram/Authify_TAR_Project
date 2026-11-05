"""
transport/receiver.py
=====================
UDP Receiver process for Phase 4 of the TAR protocol.

Responsibilities:
1. Binds standard UDP socket to 127.0.0.1:9004.
2. Reads incoming datagrams with recvfrom(65535) and enforces 1,200-byte wire MTU.
3. SO_RCVBUF configured for high-throughput packet bursts.
4. Idempotent START handshake.
5. Out-of-order reassembly and gap buffering via ReorderBuffer (capacity 5,000).
6. In-order pass-through: In-order packets are always accepted even at capacity.
7. SACK range tracking with dual watermarks and 32-range memory cap via SackTracker.
8. Immediate ACK on gaps and PROBE packets; delayed ACK (10 packets or 5 ms) on sequential flows.
9. Gap-aware FIN teardown: Withholds FIN_ACK and emits SACK if gaps exist; acknowledges
   and enters 2.0s linger once all records are contiguously delivered.
10. Measures empirical delivery gap (max delivery stall) and tracks all reliability metrics.
"""

import argparse
import json
import math
import os
import socket
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from schemas.record_schema import validate_record
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
from transport.reorder_buffer import ReorderBuffer
from transport.sack import SackTracker


class UdpReceiver:
    """
    Standard-library UDP socket receiver for TAR transport with SACK and ReorderBuffer.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9004,
        output_path: str = "data/received_records.jsonl",
        buffer_size: int = DEFAULT_RECEIVE_BUFFER_SIZE,
        reorder_capacity: int = 5000,
        enable_fin_repair: bool = False,
    ):
        self.host = host
        self.port = port
        self.output_path = output_path
        self.buffer_size = buffer_size
        self.reorder_capacity = reorder_capacity
        self.enable_fin_repair = enable_fin_repair

        self.sock: Optional[socket.socket] = None
        self.state: str = "IDLE"  # IDLE, RECEIVING, COMPLETED, FAILED
        self.session_id: Optional[str] = None
        self.expected_records: int = 0
        self.expected_record_id: int = 1

        # SACK Tracker & Reorder Buffer
        self.sack_tracker = SackTracker(max_ranges=32)
        self.reorder_buffer = ReorderBuffer(max_capacity=reorder_capacity)

        # Separate metric counters
        self.received_records: int = 0
        self.seen_record_ids: Set[int] = set()
        self.duplicate_records: List[int] = []
        self.missing_records: List[int] = []
        self.out_of_order_records: int = 0
        self.oversized_datagrams_count: int = 0
        self.max_actual_datagram_size: int = 0
        self.probes_received_count: int = 0
        self.acks_sent_count: int = 0

        # ACK scheduling state
        self.unacked_packet_count: int = 0
        self.first_unacked_time: Optional[float] = None
        self.last_ack_sent_time: float = 0.0

        # Delivery gap timing (monotonic)
        self.session_start_time: float = 0.0
        self.delivery_timestamps: List[float] = []
        self.fin_received_time: float = 0.0
        self.max_delivery_gap: float = 0.0

        self.output_file = None
        self.cached_fin_ack_bytes: Optional[bytes] = None
        self.last_sender_addr: Optional[Tuple[str, int]] = None
        self.validation_failure_reason: Optional[str] = None
        self.stop_requested: bool = False

    def start_socket(self) -> None:
        """Initializes and binds the UDP socket."""
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
        self.sock.bind((self.host, self.port))

    def close(self) -> None:
        """Flushes files and closes socket."""
        if self.output_file and not self.output_file.closed:
            self.output_file.flush()
            self.output_file.close()
        if self.sock:
            self.sock.close()
            self.sock = None

    def stop(self) -> None:
        """Signals the receiver to stop cleanly."""
        self.stop_requested = True
        self.state = "STOPPED"
        self.close()

    def _send_ack(self, addr: Tuple[str, int]) -> None:
        """Constructs and dispatches an AckPacket reflecting current SACK tracker state."""
        now = time.monotonic()
        if self.first_unacked_time is not None:
            delay_ms = max(0.0, (now - self.first_unacked_time) * 1000.0)
        else:
            delay_ms = 0.0

        ack_pkt = self.sack_tracker.create_ack_packet(
            connection_id=self.session_id or "unknown",
            receiver_delivered_through=self.reorder_buffer.receiver_delivered_through,
            ack_delay_ms=delay_ms,
        )
        raw_ack = serialize_packet(ack_pkt, max_bytes=MAX_DATAGRAM_BYTES)
        if self.sock:
            self.sock.sendto(raw_ack, addr)
        self.acks_sent_count += 1
        self.unacked_packet_count = 0
        self.first_unacked_time = None
        self.last_ack_sent_time = now

    def _handle_start(self, pkt: StartPacket, addr: Tuple[str, int]) -> None:
        """Handles session establishment idempotently."""
        self.last_sender_addr = addr

        if self.state == "IDLE":
            self.session_id = pkt.connection_id
            self.expected_records = pkt.expected_records
            self.expected_record_id = 1
            self.received_records = 0
            self.seen_record_ids.clear()
            self.duplicate_records.clear()
            self.missing_records.clear()
            self.out_of_order_records = 0
            self.probes_received_count = 0
            self.acks_sent_count = 0
            self.validation_failure_reason = None

            self.sack_tracker = SackTracker(max_ranges=32)
            self.reorder_buffer = ReorderBuffer(max_capacity=self.reorder_capacity)
            self.delivery_timestamps.clear()
            self.session_start_time = time.monotonic()

            os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)
            self.output_file = open(self.output_path, "w", encoding="utf-8")
            self.state = "RECEIVING"

            ack = StartAckPacket(
                connection_id=pkt.connection_id,
                expected_records=pkt.expected_records,
                status="READY",
            )
            raw_ack = serialize_packet(ack)
            self.sock.sendto(raw_ack, addr)
            print(f"[Receiver] Session '{pkt.connection_id}' initialized. Expecting {pkt.expected_records} records.")

        elif self.state == "RECEIVING" and pkt.connection_id == self.session_id:
            ack = StartAckPacket(
                connection_id=pkt.connection_id,
                expected_records=self.expected_records,
                status="READY",
            )
            self.sock.sendto(serialize_packet(ack), addr)
            print(f"[Receiver] Duplicate START received for active session '{pkt.connection_id}'. Re-sent START_ACK.")

        elif self.state == "COMPLETED" and pkt.connection_id == self.session_id:
            if self.cached_fin_ack_bytes:
                self.sock.sendto(self.cached_fin_ack_bytes, addr)

        else:
            err = ErrorPacket(
                connection_id=pkt.connection_id,
                code="SESSION_CONFLICT",
                message=f"Receiver currently in state {self.state} for session '{self.session_id}'",
            )
            self.sock.sendto(serialize_packet(err), addr)

    def _handle_data(self, pkt: DataPacket, addr: Tuple[str, int]) -> None:
        """Validates, reorders, and persists an incoming DATA packet."""
        self.last_sender_addr = addr

        if self.state != "RECEIVING" or pkt.connection_id != self.session_id:
            return

        rid = pkt.record_id

        # Validate payload schema
        is_valid, err_msg = validate_record({"record_id": rid, "payload": pkt.payload})
        if not is_valid:
            print(f"[Receiver] Schema validation error on record {rid}: {err_msg}")
            return

        # Process through ReorderBuffer
        delivered_packets, status = self.reorder_buffer.process_data_packet(pkt)

        # Full drop protection
        if status == "FULL_DROPPED":
            print(f"[Receiver] Dropped OOO packet {pkt.packet_id} (buffer full at capacity {self.reorder_buffer.max_capacity})")
            return

        # Record transmission packet_id in SackTracker
        self.sack_tracker.record_received_packet(pkt.packet_id)

        now = time.monotonic()

        # Handle delivered records
        if delivered_packets:
            for p in delivered_packets:
                self.seen_record_ids.add(p.record_id)
                self.delivery_timestamps.append(now)
                record_obj = {"record_id": p.record_id, "payload": p.payload}
                self.output_file.write(json.dumps(record_obj, ensure_ascii=False) + "\n")
                self.output_file.flush()
                self.received_records += 1

        if status in ("DUPLICATE_DELIVERED", "DUPLICATE_BUFFERED"):
            self.duplicate_records.append(rid)
        elif status == "BUFFERED":
            self.out_of_order_records += 1
            # Out-of-order gap detected: Send immediate SACK to trigger fast recovery
            self._send_ack(addr)
            return

        # Delayed ACK scheduling for contiguous stream
        self.unacked_packet_count += 1
        if self.first_unacked_time is None:
            self.first_unacked_time = now

        if self.unacked_packet_count >= 10 or (now - self.first_unacked_time) >= 0.005:
            self._send_ack(addr)

    def _handle_probe(self, pkt: ProbePacket, addr: Tuple[str, int]) -> None:
        """Handles a ProbePacket: records transmission and replies immediately with SACK."""
        self.last_sender_addr = addr
        if pkt.connection_id != self.session_id:
            return

        self.probes_received_count += 1
        self.sack_tracker.record_received_packet(pkt.packet_id)

        # If probe carries payload for a missing record, process it to repair immediately
        if pkt.payload is not None and pkt.record_id > 0:
            data_pkt = DataPacket(
                connection_id=pkt.connection_id,
                packet_id=pkt.packet_id,
                record_id=pkt.record_id,
                attempt=pkt.attempt,
                payload=pkt.payload,
            )
            delivered_packets, _ = self.reorder_buffer.process_data_packet(data_pkt)
            if delivered_packets:
                now = time.monotonic()
                for p in delivered_packets:
                    self.seen_record_ids.add(p.record_id)
                    self.delivery_timestamps.append(now)
                    record_obj = {"record_id": p.record_id, "payload": p.payload}
                    self.output_file.write(json.dumps(record_obj, ensure_ascii=False) + "\n")
                    self.output_file.flush()
                    self.received_records += 1

        # Immediate SACK feedback
        self._send_ack(addr)

    def _calculate_max_delivery_gap(self) -> float:
        """Calculates Delta t_delivery_max across start, inter-record, and tail phases."""
        if not self.delivery_timestamps:
            return 0.0

        # Phase 1: Start gap
        dt_start = max(0.0, self.delivery_timestamps[0] - self.session_start_time)

        # Phase 2: Inter-delivery gaps
        dt_k_max = 0.0
        for i in range(1, len(self.delivery_timestamps)):
            dt = max(0.0, self.delivery_timestamps[i] - self.delivery_timestamps[i - 1])
            if dt > dt_k_max:
                dt_k_max = dt

        # Phase 3: Tail gap
        dt_tail = 0.0
        if self.fin_received_time >= self.delivery_timestamps[-1]:
            dt_tail = max(0.0, self.fin_received_time - self.delivery_timestamps[-1])

        self.max_delivery_gap = max(dt_start, dt_k_max, dt_tail)
        return self.max_delivery_gap

    def _handle_fin(self, pkt: FinPacket, addr: Tuple[str, int]) -> None:
        """
        Handles FIN with gap discovery and strict verification.
        If gaps exist, withholds FIN_ACK and returns SACK.
        If all records are contiguous, flushes output and returns FIN_ACK.
        """
        self.last_sender_addr = addr

        if self.state == "COMPLETED" and pkt.connection_id == self.session_id:
            if self.cached_fin_ack_bytes:
                self.sock.sendto(self.cached_fin_ack_bytes, addr)
                print(f"[Receiver] Duplicate FIN received for completed session '{pkt.connection_id}'. Re-sent FIN_ACK.")
            return

        if self.state != "RECEIVING" or pkt.connection_id != self.session_id:
            return

        self.fin_received_time = time.monotonic()

        # Check for missing gaps in delivery
        if (
            self.reorder_buffer.receiver_delivered_through < pkt.final_record_id
            or not self.reorder_buffer.is_empty
            or self.received_records < self.expected_records
        ):
            if self.enable_fin_repair:
                # Missing records exist! Withhold FIN_ACK and immediately return SACK
                print(
                    f"[Receiver] FIN received with missing gaps (delivered_through="
                    f"{self.reorder_buffer.receiver_delivered_through}/{pkt.final_record_id}, "
                    f"buffered={self.reorder_buffer.buffered_count}). Withholding FIN_ACK and sending SACK..."
                )
                self._send_ack(addr)
                return

        # Strict delivery verification
        errors = []
        if self.received_records != self.expected_records:
            errors.append(
                f"Record count mismatch: expected {self.expected_records}, received {self.received_records}"
            )
        if pkt.final_record_id != self.expected_records:
            errors.append(
                f"Final record ID mismatch: FIN declared {pkt.final_record_id}, expected {self.expected_records}"
            )
        if self.seen_record_ids:
            if min(self.seen_record_ids) != 1 or max(self.seen_record_ids) != pkt.final_record_id:
                errors.append(
                    f"ID boundary mismatch: seen min={min(self.seen_record_ids)}, max={max(self.seen_record_ids)}"
                )

        if errors:
            self.state = "FAILED"
            self.validation_failure_reason = "; ".join(errors)
            print(f"[Receiver] DELIVERY VERIFICATION FAILED ON FIN: {self.validation_failure_reason}")
            err_pkt = ErrorPacket(
                connection_id=pkt.connection_id,
                code="INCOMPLETE_DELIVERY",
                message=self.validation_failure_reason,
            )
            self.sock.sendto(serialize_packet(err_pkt), addr)
            if self.output_file and not self.output_file.closed:
                self.output_file.close()
            return

        # Verification successful: flush, cache FIN_ACK, send to sender
        if self.output_file and not self.output_file.closed:
            self.output_file.flush()
            self.output_file.close()

        self._calculate_max_delivery_gap()

        fin_ack = FinAckPacket(
            connection_id=pkt.connection_id,
            final_record_id=pkt.final_record_id,
            receiver_delivered_through=pkt.final_record_id,
            total_delivered_records=self.received_records,
        )
        self.cached_fin_ack_bytes = serialize_packet(fin_ack)
        self.state = "COMPLETED"
        self.sock.sendto(self.cached_fin_ack_bytes, addr)
        print(f"[Receiver] All {self.received_records} records verified in order! Sent FIN_ACK.")

    def run_until_complete(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Runs the receiver loop until session completes, fails, or times out.
        Returns final metrics dictionary.
        """
        if not self.sock:
            self.start_socket()

        print(f"[Receiver] Listening on {self.host}:{self.port}...")
        self.sock.settimeout(0.01)  # 10ms poll granularity for fast ACK timer servicing

        start_time = time.monotonic()

        try:
            while self.state != "COMPLETED" and self.state != "FAILED":
                if self.stop_requested:
                    print(f"[Receiver] Stop requested. Shutting down.")
                    self.state = "STOPPED"
                    break

                now = time.monotonic()
                if timeout and (now - start_time) > timeout:
                    print(f"[Receiver] Global timeout of {timeout}s reached.")
                    break

                # Service delayed ACK timer
                if (
                    self.unacked_packet_count > 0
                    and self.first_unacked_time is not None
                    and (now - self.first_unacked_time) >= 0.005
                    and self.last_sender_addr
                ):
                    self._send_ack(self.last_sender_addr)

                try:
                    raw_bytes, addr = self.sock.recvfrom(self.buffer_size)
                except socket.timeout:
                    continue
                except OSError:
                    break

                actual_bytes = len(raw_bytes)
                self.max_actual_datagram_size = max(self.max_actual_datagram_size, actual_bytes)

                if actual_bytes > MAX_DATAGRAM_BYTES:
                    self.oversized_datagrams_count += 1
                    print(
                        f"[Receiver] REJECTED oversized datagram: {actual_bytes} bytes "
                        f"(exceeds {MAX_DATAGRAM_BYTES} bytes MTU limit) from {addr}"
                    )
                    continue

                try:
                    packet = deserialize_packet(raw_bytes, max_bytes=MAX_DATAGRAM_BYTES)
                except (OversizedDatagramError, InvalidPacketError) as exc:
                    print(f"[Receiver] Malformed datagram rejected: {exc}")
                    continue

                if packet.type == "START":
                    self._handle_start(packet, addr)
                elif packet.type == "DATA":
                    self._handle_data(packet, addr)
                elif packet.type == "PROBE":
                    self._handle_probe(packet, addr)
                elif packet.type == "FIN":
                    self._handle_fin(packet, addr)
                elif packet.type == "ERROR":
                    print(f"[Receiver] Error received from {addr}: {packet.code} - {packet.message}")

            # Bounded 2.0-second linger period
            if self.state == "COMPLETED":
                linger_until = time.monotonic() + 2.0
                print(f"[Receiver] Delivery verified! Entering bounded 2.0s linger period for duplicate FIN absorption...")
                while time.monotonic() < linger_until:
                    if self.stop_requested or not self.sock:
                        break
                    remaining = max(0.01, linger_until - time.monotonic())
                    try:
                        self.sock.settimeout(min(0.2, remaining))
                        raw_bytes, addr = self.sock.recvfrom(self.buffer_size)
                        pkt = deserialize_packet(raw_bytes)
                        if pkt.type == "FIN":
                            self._handle_fin(pkt, addr)
                    except socket.timeout:
                        continue
                    except (OSError, InvalidPacketError, OversizedDatagramError):
                        continue
                print("[Receiver] 2.0s linger period elapsed. Shutting down cleanly.")

        finally:
            self.close()

        return self.get_metrics()

    def get_metrics(self) -> Dict[str, Any]:
        """Returns comprehensive delivery and performance metrics."""
        return {
            "status": self.state,
            "session_id": self.session_id,
            "expected_records": self.expected_records,
            "received_records": self.received_records,
            "receiver_delivered_through": self.reorder_buffer.receiver_delivered_through,
            "reorder_buffer_remaining": self.reorder_buffer.buffered_count,
            "missing_records_count": len(self.missing_records),
            "missing_records_sample": self.missing_records[:20],
            "duplicate_records_count": len(self.duplicate_records),
            "duplicate_records_sample": self.duplicate_records[:20],
            "out_of_order_records_count": self.out_of_order_records,
            "probes_received_count": self.probes_received_count,
            "acks_sent_count": self.acks_sent_count,
            "max_delivery_gap_seconds": round(self.max_delivery_gap, 4),
            "oversized_datagrams_rejected": self.oversized_datagrams_count,
            "max_actual_datagram_size_bytes": self.max_actual_datagram_size,
            "output_file": self.output_path,
            "validation_failure_reason": self.validation_failure_reason,
        }


def main():
    parser = argparse.ArgumentParser(description="TAR Protocol UDP Receiver (Phase 4)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9004, help="Bind port (default: 9004)")
    parser.add_argument("--output", type=str, default="data/received_records.jsonl", help="Output JSONL path")
    parser.add_argument("--timeout", type=float, default=60.0, help="Max wait timeout in seconds (default: 60.0)")
    parser.add_argument("--capacity", type=int, default=5000, help="Reorder buffer capacity (default: 5000)")
    parser.add_argument("--enable-fin-repair", action="store_true", help="Enable gap-aware SACK repair on FIN")
    args = parser.parse_args()

    receiver = UdpReceiver(
        host=args.host,
        port=args.port,
        output_path=args.output,
        reorder_capacity=args.capacity,
        enable_fin_repair=args.enable_fin_repair,
    )

    try:
        metrics = receiver.run_until_complete(timeout=args.timeout)
        print("\n=== RECEIVER FINAL METRICS ===")
        print(json.dumps(metrics, indent=2))
        if metrics["status"] == "COMPLETED":
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n[Receiver] Interrupted by user.")
        receiver.close()
        sys.exit(130)


if __name__ == "__main__":
    main()
