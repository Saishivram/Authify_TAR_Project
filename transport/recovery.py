"""
transport/recovery.py
=====================
Recovery coordinator and per-record state machine for TAR.

Features:
- Per-record 7-state machine:
  QUEUED -> IN_FLIGHT -> BUFFERED_AT_RECEIVER -> DELIVERED
  IN_FLIGHT / REPAIR_IN_FLIGHT -> REPAIR_PENDING -> REPAIR_IN_FLIGHT
  REPAIR_PENDING -> FAILED (on retry exhaustion > 10 attempts)
  Feedback Cancellation: Any ACK matching a transmission or cumulative watermark
  cancels and dequeues pending repairs.
- RACK loss detection: Causal delivery time reference + adaptive reordering window.
- Separate PTO probe scheduler: PTO expiration emits a probe with fresh packet_id
  without marking outstanding transmissions as lost.
- Fallback RTO timer: Armed on oldest unacknowledged packet; on timeout marks lost,
  backs off RTO, and transitions record to REPAIR_PENDING.
- Spurious retransmission detection & 16-flight clean decay engine.
- Bounded memory retirement: Prunes completed records and obsolete packet IDs.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from transport.packet import AckPacket, DataPacket, ProbePacket
from transport.rtt import RttEstimator


class RecordState(str, Enum):
    QUEUED = "QUEUED"
    IN_FLIGHT = "IN_FLIGHT"
    BUFFERED_AT_RECEIVER = "BUFFERED_AT_RECEIVER"
    REPAIR_PENDING = "REPAIR_PENDING"
    REPAIR_IN_FLIGHT = "REPAIR_IN_FLIGHT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


@dataclass
class TransmissionAttempt:
    packet_id: int
    record_id: int
    attempt: int
    sent_at: float
    is_probe: bool = False
    lost: bool = False
    acknowledged: bool = False


@dataclass
class RecordTracker:
    record_id: int
    payload: Any
    state: RecordState = RecordState.QUEUED
    attempt_count: int = 0
    latest_attempt_time: float = 0.0
    last_probe_or_repair_time: float = 0.0
    attempts: List[int] = field(default_factory=list)  # List of packet_ids


class RecoveryCoordinator:
    """
    Coordinates loss detection, repair scheduling, PTO probing, and state machine transitions.
    """

    MAX_RETRIES: int = 10
    MAX_ATTEMPTS_HISTORY: int = 10000

    def __init__(self, rtt_estimator: Optional[RttEstimator] = None):
        self.rtt_estimator = rtt_estimator or RttEstimator()
        self.records: Dict[int, RecordTracker] = {}
        self.transmissions: Dict[int, TransmissionAttempt] = {}
        self.unacked_transmissions: Dict[int, TransmissionAttempt] = {}
        self.repair_queue: List[int] = []  # List of record_ids waiting for repair
        self.receiver_delivered_through: int = 0
        self.last_applied_delivery_watermark: int = 0
        self.latest_delivered_tx_time: float = 0.0
        self.in_flight_count: int = 0

        # RTO & PTO timers
        self.rto_start_time: Optional[float] = None
        self.last_ack_recv_time: Optional[float] = None
        self.pto_count: int = 0

        # Recovery flight tracking for 16-flight clean decay
        self.active_flight_end_packet: Optional[int] = None
        self.flight_had_spurious: bool = False
        self.flight_unacked_pids: Set[int] = set()

    def _set_record_state(self, rec: RecordTracker, new_state: RecordState) -> None:
        """Sets record state and maintains in_flight_count in O(1)."""
        old_state = rec.state
        if old_state == new_state:
            return
        in_flight_states = (RecordState.IN_FLIGHT, RecordState.REPAIR_IN_FLIGHT)
        was_in_flight = old_state in in_flight_states
        is_in_flight = new_state in in_flight_states
        if not was_in_flight and is_in_flight:
            self.in_flight_count += 1
        elif was_in_flight and not is_in_flight:
            self.in_flight_count = max(0, self.in_flight_count - 1)
        rec.state = new_state

    def register_record(self, record_id: int, payload: Any) -> RecordTracker:
        """Registers a new logical record in QUEUED state."""
        tracker = RecordTracker(record_id=record_id, payload=payload, state=RecordState.QUEUED)
        self.records[record_id] = tracker
        return tracker

    def on_packet_sent(
        self,
        packet_id: int,
        record_id: int,
        attempt: int,
        sent_at: float,
        is_probe: bool = False,
    ) -> None:
        """Records transmission of a packet."""
        attempt_record = TransmissionAttempt(
            packet_id=packet_id,
            record_id=record_id,
            attempt=attempt,
            sent_at=sent_at,
            is_probe=is_probe,
        )
        self.transmissions[packet_id] = attempt_record
        self.unacked_transmissions[packet_id] = attempt_record

        rec = self.records.get(record_id)
        if rec:
            rec.attempt_count = max(rec.attempt_count, attempt)
            rec.latest_attempt_time = sent_at
            rec.attempts.append(packet_id)

            if is_probe:
                rec.last_probe_or_repair_time = sent_at
            elif attempt > 1:
                self._set_record_state(rec, RecordState.REPAIR_IN_FLIGHT)
                rec.last_probe_or_repair_time = sent_at
                # Remove from repair queue if present
                if record_id in self.repair_queue:
                    self.repair_queue.remove(record_id)
            else:
                self._set_record_state(rec, RecordState.IN_FLIGHT)

        # Initialize timers if not running
        if self.rto_start_time is None:
            self.rto_start_time = sent_at
        if self.last_ack_recv_time is None:
            self.last_ack_recv_time = sent_at

    def on_ack_received(self, ack: AckPacket, now: float) -> List[int]:
        """
        Processes an incoming AckPacket.

        Updates:
        - receiver_delivered_through -> transitions records to DELIVERED and cancels repairs.
        - SACK ranges & cumulative watermark -> marks attempts acknowledged.
        - Checks for spurious retransmissions.
        - Updates RTT estimator.
        - Runs RACK loss detection.
        - Returns list of newly declared lost record_ids.
        """
        self.last_ack_recv_time = now
        self.pto_count = 0

        # 1. Update receiver_delivered_through (Delivered application watermark)
        if ack.receiver_delivered_through > self.receiver_delivered_through:
            self.receiver_delivered_through = ack.receiver_delivered_through
            self._apply_delivery_watermark(ack.receiver_delivered_through)

        # 2. Collect acknowledged packet IDs from cumulative and SACK ranges
        acked_pids: List[int] = []
        for pid in list(self.unacked_transmissions.keys()):
            # Cumulative ACK
            if pid <= ack.ack_received_through_packet_id:
                acked_pids.append(pid)
                continue
            # Discontiguous SACK ranges
            for high, low in ack.ack_ranges:
                if low <= pid <= high:
                    acked_pids.append(pid)
                    break

        for pid in acked_pids:
            tx = self.unacked_transmissions.pop(pid, None)
            if not tx:
                tx = self.transmissions.get(pid)
            if not tx:
                continue
            tx.acknowledged = True

            # RTT Sample: Unambiguous transmissions only (attempt == 1 and not previously declared lost)
            if tx.attempt == 1 and not tx.lost and not tx.is_probe:
                self.rtt_estimator.update_rtt(tx.sent_at, now, ack.ack_delay_ms)

            # Spurious Retransmission Check:
            # If tx was previously declared lost, and a later replacement was sent
            rec = self.records.get(tx.record_id)
            if tx.lost and rec and rec.attempt_count > tx.attempt:
                self.rtt_estimator.on_spurious_retransmission()
                self.flight_had_spurious = True

            # Advance RACK reference time
            if tx.sent_at > self.latest_delivered_tx_time:
                self.latest_delivered_tx_time = tx.sent_at

            # Update Record State
            if rec and rec.state != RecordState.DELIVERED:
                if rec.record_id <= self.receiver_delivered_through:
                    self._set_record_state(rec, RecordState.DELIVERED)
                else:
                    # Transmission acknowledged, but not contiguous at receiver yet
                    self._set_record_state(rec, RecordState.BUFFERED_AT_RECEIVER)

                # Cancel any pending repair
                if rec.record_id in self.repair_queue:
                    self.repair_queue.remove(rec.record_id)

        # Reset or update RTO timer
        self._reset_rto_timer()

        # 3. RACK Loss Detection
        newly_lost_records = self._run_rack_loss_detection(now)

        # 4. Check Recovery Flight Completion
        self._check_flight_progress()

        # 5. Prune obsolete memory
        self._prune_history()

        return newly_lost_records

    def _apply_delivery_watermark(self, delivered_through: int) -> None:
        """Transitions all records <= delivered_through to DELIVERED and cancels repairs in O(delta)."""
        start_id = max(1, self.last_applied_delivery_watermark + 1)
        for r_id in range(start_id, delivered_through + 1):
            rec = self.records.get(r_id)
            if rec:
                if rec.state != RecordState.DELIVERED:
                    self._set_record_state(rec, RecordState.DELIVERED)
                    if r_id in self.repair_queue:
                        self.repair_queue.remove(r_id)
        self.last_applied_delivery_watermark = max(self.last_applied_delivery_watermark, delivered_through)

    def _run_rack_loss_detection(self, now: float) -> List[int]:
        """
        Runs RACK loss detection:
        Any unacknowledged transmission sent earlier than latest_delivered_tx_time
        whose loss deadline (tx.sent_at + rtt + reordering_window) has elapsed is declared lost.
        Operates on self.unacked_transmissions for O(W) performance.
        """
        if self.latest_delivered_tx_time <= 0:
            return []

        rtt = self.rtt_estimator.srtt or 0.05
        reordering_window = self.rtt_estimator.get_reordering_window()
        newly_lost_records: List[int] = []

        for pid, tx in list(self.unacked_transmissions.items()):
            if not tx.lost and tx.sent_at < self.latest_delivered_tx_time:
                loss_deadline = tx.sent_at + rtt + reordering_window
                if now >= loss_deadline:
                    tx.lost = True
                    rec = self.records.get(tx.record_id)
                    if rec and rec.state in (RecordState.IN_FLIGHT, RecordState.REPAIR_IN_FLIGHT):
                        # Transition to REPAIR_PENDING or FAILED
                        if rec.attempt_count >= self.MAX_RETRIES:
                            self._set_record_state(rec, RecordState.FAILED)
                        else:
                            self._set_record_state(rec, RecordState.REPAIR_PENDING)
                            if rec.record_id not in self.repair_queue:
                                self.repair_queue.append(rec.record_id)
                                newly_lost_records.append(rec.record_id)
                                self._start_recovery_flight(pid)

        return newly_lost_records

    def check_timeouts(self, now: float) -> Tuple[List[int], Optional[int]]:
        """
        Checks RACK loss deadlines, Fallback RTO, and PTO timers.

        Returns:
            (records_to_repair, probe_record_id_or_none)
        """
        # Service any RACK loss deadlines that expired
        rack_lost = self._run_rack_loss_detection(now)
        records_to_repair: List[int] = list(rack_lost)
        probe_record_id: Optional[int] = None

        unacked_active_txs = [tx for tx in self.unacked_transmissions.values() if not tx.lost]
        if not unacked_active_txs:
            return (records_to_repair, None)

        rto_duration = self.rtt_estimator.rto

        # 1. Fallback RTO: Checks all unacknowledged transmissions whose timer expired
        timed_out_txs = [tx for tx in unacked_active_txs if (now - tx.sent_at) >= rto_duration]

        if timed_out_txs:
            # Fallback RTO expired for at least one transmission in flight
            self.rtt_estimator.backoff_rto()

            for tx in timed_out_txs:
                tx.lost = True

                rec = self.records.get(tx.record_id)
                if rec and rec.state in (RecordState.IN_FLIGHT, RecordState.REPAIR_IN_FLIGHT):
                    if rec.attempt_count >= self.MAX_RETRIES:
                        self._set_record_state(rec, RecordState.FAILED)
                    else:
                        self._set_record_state(rec, RecordState.REPAIR_PENDING)
                        if rec.record_id not in self.repair_queue:
                            self.repair_queue.append(rec.record_id)
                        if rec.record_id not in records_to_repair:
                            records_to_repair.append(rec.record_id)
                        self._start_recovery_flight(tx.packet_id)

            self._reset_rto_timer()
            return (records_to_repair, None)

        # 2. PTO (Probe Timeout): Tail loss detection
        # Max 2 PTO probes with exponential backoff before fallback RTO
        if self.pto_count < 2:
            pto_duration = self.rtt_estimator.get_pto() * (2 ** self.pto_count)
            oldest_tx = min(unacked_active_txs, key=lambda t: t.sent_at)
            last_activity = max(self.last_ack_recv_time or 0.0, oldest_tx.sent_at)

            if (now - last_activity) >= pto_duration:
                # Check newest active unacknowledged transmission
                newest_tx = max(unacked_active_txs, key=lambda t: t.sent_at)
                rec = self.records.get(newest_tx.record_id)
                if rec and rec.state in (RecordState.IN_FLIGHT, RecordState.REPAIR_IN_FLIGHT):
                    # Suppress duplicate probes within 1 * SRTT (min 200ms)
                    srtt = max(0.2, self.rtt_estimator.srtt or 0.2)
                    if (now - rec.last_probe_or_repair_time) >= srtt:
                        # PTO expiration triggers probe WITHOUT marking lost and WITHOUT REPAIR_PENDING!
                        probe_record_id = rec.record_id
                        self.pto_count += 1
                        self.last_ack_recv_time = now  # Reset PTO window

        return (records_to_repair, probe_record_id)

    def _reset_rto_timer(self) -> None:
        """Arms RTO for the oldest active unacknowledged packet or disarms if all acknowledged."""
        active = [t for t in self.unacked_transmissions.values() if not t.lost]
        if active:
            self.rto_start_time = min(t.sent_at for t in active)
        else:
            self.rto_start_time = None

    def _start_recovery_flight(self, triggered_pid: int) -> None:
        """Starts a recovery flight tracking clean decay progression."""
        if self.active_flight_end_packet is None:
            max_pid = max(self.transmissions.keys()) if self.transmissions else triggered_pid
            self.active_flight_end_packet = max_pid
            self.flight_had_spurious = False
            self.flight_unacked_pids = {
                pid for pid, tx in self.transmissions.items()
                if pid <= max_pid and not tx.acknowledged
            }

    def _check_flight_progress(self) -> None:
        """Checks if current recovery flight has completed."""
        if self.active_flight_end_packet is None:
            return

        # Check if all flight packets are acknowledged or declared lost
        still_unresolved = False
        for pid in list(self.flight_unacked_pids):
            tx = self.transmissions.get(pid)
            if tx and (tx.acknowledged or tx.lost):
                self.flight_unacked_pids.remove(pid)
            else:
                still_unresolved = True

        if not still_unresolved:
            # Flight ended
            if self.flight_had_spurious:
                self.rtt_estimator.on_unclean_flight()
            else:
                self.rtt_estimator.on_clean_flight()

            self.active_flight_end_packet = None
            self.flight_unacked_pids.clear()
            self.flight_had_spurious = False

    def _prune_history(self) -> None:
        """Prunes old acknowledged transmission history to maintain O(1) bounded memory."""
        if len(self.transmissions) > self.MAX_ATTEMPTS_HISTORY:
            # Remove earliest acknowledged transmissions
            pids_to_remove = [
                pid for pid, tx in self.transmissions.items()
                if tx.acknowledged and tx.record_id <= self.receiver_delivered_through
            ]
            # Keep at least half
            remove_count = len(self.transmissions) - (self.MAX_ATTEMPTS_HISTORY // 2)
            for pid in pids_to_remove[:remove_count]:
                del self.transmissions[pid]
                self.unacked_transmissions.pop(pid, None)
