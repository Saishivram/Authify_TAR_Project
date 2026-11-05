"""
tests/test_recovery.py
======================
Unit tests for RecoveryCoordinator: per-record state machine, RACK loss detection,
PTO probe separation, repair cancellation, Fallback RTO, and spurious retransmission detection.
"""

import unittest
from transport.packet import AckPacket
from transport.recovery import (
    RecordState,
    RecoveryCoordinator,
)
from transport.rtt import RttEstimator


def make_ack(
    delivered_through: int,
    received_through: int = 0,
    ranges: list = None,
    delay_ms: float = 1.0,
) -> AckPacket:
    ranges = ranges or []
    largest = received_through
    if ranges:
        largest = max(largest, max(r[0] for r in ranges))
    floor = received_through
    if ranges:
        floor = max(floor, ranges[-1][1] - 1)
    return AckPacket(
        connection_id="test-conn",
        largest_packet_id=largest,
        ack_received_through_packet_id=received_through,
        ack_range_floor_packet_id=floor,
        ack_ranges=ranges,
        ack_delay_ms=delay_ms,
        receiver_delivered_through=delivered_through,
    )


class TestRecoveryCoordinator(unittest.TestCase):
    def test_record_state_lifecycle_clean_delivery(self):
        coordinator = RecoveryCoordinator()
        rec = coordinator.register_record(1, {"data": "rec1"})
        self.assertEqual(rec.state, RecordState.QUEUED)

        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        self.assertEqual(rec.state, RecordState.IN_FLIGHT)

        # ACK arrives confirming contiguous delivery at receiver
        ack = make_ack(delivered_through=1, received_through=1)
        coordinator.on_ack_received(ack, now=100.02)
        self.assertEqual(rec.state, RecordState.DELIVERED)
        self.assertEqual(len(coordinator.repair_queue), 0)

    def test_record_state_buffered_at_receiver(self):
        coordinator = RecoveryCoordinator()
        rec1 = coordinator.register_record(1, {"data": "rec1"})
        rec2 = coordinator.register_record(2, {"data": "rec2"})

        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        coordinator.on_packet_sent(packet_id=2, record_id=2, attempt=1, sent_at=100.01)

        # Packet 2 arrives at receiver (packet 1 dropped)
        # SACK range: [[2, 2]], receiver_delivered_through=0
        ack = make_ack(delivered_through=0, received_through=0, ranges=[[2, 2]])
        coordinator.on_ack_received(ack, now=100.03)

        # Record 2 transmission received, but cannot be delivered yet because gap 1 exists
        self.assertEqual(rec2.state, RecordState.BUFFERED_AT_RECEIVER)
        self.assertNotIn(2, coordinator.repair_queue)

        # Once record 1 is delivered, record 2 automatically becomes DELIVERED
        ack2 = make_ack(delivered_through=2, received_through=2)
        coordinator.on_ack_received(ack2, now=100.05)
        self.assertEqual(rec1.state, RecordState.DELIVERED)
        self.assertEqual(rec2.state, RecordState.DELIVERED)

    def test_repair_cancellation_by_ack(self):
        coordinator = RecoveryCoordinator()
        rec1 = coordinator.register_record(1, {"data": "rec1"})
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)

        # Artificially queue record 1 for repair
        rec1.state = RecordState.REPAIR_PENDING
        coordinator.repair_queue.append(1)

        # ACK arrives matching original attempt
        ack = make_ack(delivered_through=0, received_through=0, ranges=[[1, 1]])
        coordinator.on_ack_received(ack, now=100.04)

        # Must cancel repair and transition to BUFFERED_AT_RECEIVER
        self.assertEqual(rec1.state, RecordState.BUFFERED_AT_RECEIVER)
        self.assertNotIn(1, coordinator.repair_queue)

    def test_rack_loss_detection(self):
        rtt_est = RttEstimator(initial_rto=1.0)
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec1 = coordinator.register_record(1, {"data": "rec1"})
        rec2 = coordinator.register_record(2, {"data": "rec2"})

        # Send packet 1 at 100.0, packet 2 at 100.01
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        coordinator.on_packet_sent(packet_id=2, record_id=2, attempt=1, sent_at=100.01)

        # Packet 2 is ACKed at 100.03 -> establishes latest_delivered_tx_time = 100.01
        # Loss deadline for packet 1: tx.sent_at (100.0) + SRTT (~0.019) + reo_wnd (~0.0095) = 100.0285 < 100.03
        ack = make_ack(delivered_through=0, received_through=0, ranges=[[2, 2]])
        lost = coordinator.on_ack_received(ack, now=100.03)

        self.assertIn(1, lost)
        self.assertEqual(rec1.state, RecordState.REPAIR_PENDING)
        self.assertIn(1, coordinator.repair_queue)

    def test_pto_probing_separation(self):
        rtt_est = RttEstimator(min_rto=0.5, initial_rto=1.0)
        # Seed an RTT measurement to establish SRTT and floored PTO (0.2s)
        rtt_est.update_rtt(send_time=50.0, recv_time=50.010)
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec = coordinator.register_record(1, {"data": "rec1"})
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)

        # Let PTO expire (PTO duration = 0.2s floor, but RTO is 0.5s)
        repairs, probe_record_id = coordinator.check_timeouts(now=100.25)

        # Must trigger a probe for record 1
        self.assertEqual(probe_record_id, 1)
        self.assertEqual(repairs, [])

        # Crucial Requirement: Record remains IN_FLIGHT, NOT marked lost, NOT in repair queue!
        self.assertEqual(rec.state, RecordState.IN_FLIGHT)
        self.assertFalse(coordinator.transmissions[1].lost)
        self.assertNotIn(1, coordinator.repair_queue)

    def test_fallback_rto(self):
        rtt_est = RttEstimator(initial_rto=0.5, max_rto=4.0)
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec = coordinator.register_record(1, {"data": "rec1"})
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)

        # Advance time by RTO (0.5s)
        repairs, probe = coordinator.check_timeouts(now=100.55)

        # Fallback RTO must mark lost, back off RTO, and queue repair
        self.assertEqual(repairs, [1])
        self.assertIsNone(probe)
        self.assertTrue(coordinator.transmissions[1].lost)
        self.assertEqual(rec.state, RecordState.REPAIR_PENDING)
        self.assertIn(1, coordinator.repair_queue)
        self.assertEqual(rtt_est.rto, 1.0)  # Doubled from 0.5 to 1.0

    def test_spurious_retransmission_detection(self):
        coordinator = RecoveryCoordinator()
        rec = coordinator.register_record(1, {"data": "rec1"})

        # Initial attempt 1
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        # Declared lost by RTO
        coordinator.check_timeouts(now=101.5)
        self.assertTrue(coordinator.transmissions[1].lost)

        # Repair attempt 2 dispatched
        coordinator.on_packet_sent(packet_id=2, record_id=1, attempt=2, sent_at=101.6)

        initial_mult = coordinator.rtt_estimator.reordering_window_multiplier
        # Delayed ACK for packet 1 arrives!
        ack = make_ack(delivered_through=0, received_through=0, ranges=[[1, 1]])
        coordinator.on_ack_received(ack, now=101.7)

        # Spurious retransmission detected: multiplier increments!
        self.assertGreater(coordinator.rtt_estimator.reordering_window_multiplier, initial_mult)
        self.assertEqual(coordinator.rtt_estimator.clean_flight_count, 0)

    def test_retry_exhaustion_leads_to_failed(self):
        coordinator = RecoveryCoordinator()
        rec = coordinator.register_record(1, {"data": "rec1"})

        # Record reaches 10 attempts
        coordinator.on_packet_sent(packet_id=10, record_id=1, attempt=10, sent_at=100.0)
        # RTO expires
        coordinator.check_timeouts(now=102.0)

        self.assertEqual(rec.state, RecordState.FAILED)


if __name__ == "__main__":
    unittest.main()
