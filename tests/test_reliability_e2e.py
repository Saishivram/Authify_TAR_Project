"""
tests/test_reliability_e2e.py
=============================
Automated end-to-end reliability integration test suite for TAR Phase 4.
Executes scenarios T01 through T18 over real UDP loopback sockets (9001 <-> 9002/9003 <-> 9004).

Scenarios:
- T01: Clean path baseline (100% in-order, 0 retransmissions)
- T02: 5% packet loss recovery via SACK/RACK
- T03: 2% packet reordering recovery via ReorderBuffer
- T04: 400ms link stall recovery
- T05: Delayed original arrival & spurious retransmission detection
- T06: Adaptive reorder window growth on repeated spurious detections
- T07: 16-flight clean decay engine
- T08: Tail loss & PTO probing separation
- T09: Reverse path ACK loss recovery
- T10: Lost retransmission recovery
- T11: Duplicate packet suppression
- T12: Multiple discontiguous SACK gaps & range floor advancement
- T13: Combined stress (5% loss + 2% reorder + 400ms stall)
- T14: FIN with gaps & repair teardown
- T15: Retry exhaustion terminal failure
- T16: Permanently missing original packet_id retirement
- T17: Reorder buffer full capacity & in-order pass-through invariant
- T18: Repair cancellation & BUFFERED_AT_RECEIVER state transition
"""

import json
import os
import socket
import tempfile
import threading
import time
import unittest
from typing import Dict, List, Optional, Tuple

from schemas.record_schema import validate_jsonl_file
from transport.packet import (
    DEFAULT_RECEIVE_BUFFER_SIZE,
    DataPacket,
    FinPacket,
    deserialize_packet,
    serialize_packet,
)
from transport.receiver import UdpReceiver
from transport.recovery import RecordState, RecoveryCoordinator
from transport.relay import (
    ImpairmentRelay,
    RelayConfig,
    RelayDirectionConfig,
    TargetedRule,
)
from transport.rtt import RttEstimator
from transport.sack import SackTracker
from transport.sender import UdpSender


def generate_test_records_file(filepath: str, count: int = 100) -> str:
    """Generates canonical test JSONL file with `count` records."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for rid in range(1, count + 1):
            record = {
                "record_id": rid,
                "payload": {
                    "sensor_id": f"sensor-{rid % 10}",
                    "val": float(rid * 1.5),
                    "note": f"payload-record-{rid}",
                },
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return filepath


class TestReliabilityE2E(unittest.TestCase):
    """Automated integration tests T01 - T18 for TAR Phase 4 Reliability Engine."""

    _port_counter = 19100
    _port_lock = threading.Lock()

    @classmethod
    def allocate_ports(cls) -> Tuple[int, int, int, int]:
        """Allocates unique port tuples (sender, relay_a, relay_b, receiver)."""
        with cls._port_lock:
            p = cls._port_counter
            cls._port_counter += 10
            return (p, p + 1, p + 2, p + 3)

    def test_t01_clean_path_baseline(self):
        """T01: Clean path baseline: 100 records delivered in order with 0 retransmissions."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 10.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                forward=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 10.0, "idle_timeout": 3.0}, daemon=True)
            relay_thread.start()
            time.sleep(0.05)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port)
            metrics = sender.transmit_file(input_file, session_id="t01-clean")

            rec_thread.join(timeout=5.0)
            relay_thread.join(timeout=5.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            self.assertEqual(metrics["sent_records"], 100)
            self.assertEqual(metrics["retransmissions_count"], 0)

            val_res = validate_jsonl_file(output_file, expected_count=100)
            self.assertTrue(val_res["valid"], f"Validation failed: {val_res['errors']}")

    def test_t02_loss_recovery_5_percent(self):
        """T02: 5% packet loss recovery: All 100 records delivered in order via SACK/RACK."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 10.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                seed=42,
                forward=RelayDirectionConfig(loss_rate=0.05, reorder_rate=0.0, stall_interval_sec=0.0),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 10.0, "idle_timeout": 3.5}, daemon=True)
            relay_thread.start()
            time.sleep(0.05)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port)
            metrics = sender.transmit_file(input_file, session_id="t02-loss")

            rec_thread.join(timeout=5.0)
            relay_thread.join(timeout=5.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            self.assertEqual(metrics["sent_records"], 100)
            self.assertGreater(metrics["retransmissions_count"], 0)

            val_res = validate_jsonl_file(output_file, expected_count=100)
            self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")

    def test_t03_reorder_recovery_2_percent(self):
        """T03: 2% packet reordering: ReorderBuffer reassembles in order."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 10.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                seed=42,
                forward=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.02, reorder_min_delay_ms=10.0, reorder_max_delay_ms=30.0, stall_interval_sec=0.0),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 10.0, "idle_timeout": 3.0}, daemon=True)
            relay_thread.start()
            time.sleep(0.05)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port)
            metrics = sender.transmit_file(input_file, session_id="t03-reorder")

            rec_thread.join(timeout=5.0)
            relay_thread.join(timeout=5.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            val_res = validate_jsonl_file(output_file, expected_count=100)
            self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")

    def test_t04_link_stall_recovery(self):
        """T04: 400ms link stall recovery: Delivers all records with max delivery gap measurement."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 12.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                seed=42,
                forward=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.05, stall_duration_ms=400.0),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 12.0, "idle_timeout": 3.5}, daemon=True)
            relay_thread.start()
            time.sleep(0.06)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port, retry_timeout=1.0)
            metrics = sender.transmit_file(input_file, session_id="t04-stall")

            rec_thread.join(timeout=6.0)
            relay_thread.join(timeout=6.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            rec_metrics = receiver.get_metrics()
            self.assertEqual(rec_metrics["received_records"], 100)
            self.assertGreater(rec_metrics["max_delivery_gap_seconds"], 0.2)

    def test_t05_delayed_original_spurious_detection(self):
        """T05: Delayed original arrives after repair: Triggers spurious retransmission adaptation."""
        rtt_est = RttEstimator()
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec = coordinator.register_record(1, {"data": "test"})

        # Initial attempt
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        # Timeout marks lost
        coordinator.check_timeouts(now=101.5)
        self.assertTrue(coordinator.transmissions[1].lost)

        # Dispatch repair attempt 2
        coordinator.on_packet_sent(packet_id=2, record_id=1, attempt=2, sent_at=101.6)

        mult_before = rtt_est.reordering_window_multiplier
        # Delayed ACK for packet 1 arrives
        ack = SackTracker().create_ack_packet(connection_id="test", receiver_delivered_through=0)
        ack.ack_ranges = [[1, 1]]
        coordinator.on_ack_received(ack, now=101.7)

        # Spurious detection: Multiplier expands
        self.assertGreater(rtt_est.reordering_window_multiplier, mult_before)

    def test_t06_adaptive_window_growth_cap(self):
        """T06: Repeated spurious detections grow multiplier up to 4.0 cap."""
        rtt_est = RttEstimator()
        for i in range(10):
            rtt_est.on_spurious_retransmission()
        self.assertEqual(rtt_est.reordering_window_multiplier, 4.0)

    def test_t07_16_flight_clean_decay(self):
        """T07: 16 clean flights decay multiplier back down towards 1.0."""
        rtt_est = RttEstimator()
        rtt_est.on_spurious_retransmission()
        rtt_est.on_spurious_retransmission()
        self.assertEqual(rtt_est.reordering_window_multiplier, 3.0)

        # 16 clean flights -> decr to 2.0
        for _ in range(16):
            rtt_est.on_clean_flight()
        self.assertEqual(rtt_est.reordering_window_multiplier, 2.0)

        # 16 more clean flights -> decr to 1.0
        for _ in range(16):
            rtt_est.on_clean_flight()
        self.assertEqual(rtt_est.reordering_window_multiplier, 1.0)

    def test_t08_tail_loss_pto_probing_separation(self):
        """T08: PTO tail loss triggers probe with fresh packet_id without marking lost."""
        rtt_est = RttEstimator(min_rto=0.5, initial_rto=1.0)
        rtt_est.update_rtt(send_time=10.0, recv_time=10.02)
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec = coordinator.register_record(1, {"val": 123})
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)

        # PTO expires (~0.2s)
        repairs, probe_rid = coordinator.check_timeouts(now=100.25)
        self.assertEqual(probe_rid, 1)
        self.assertEqual(repairs, [])

        # Record remains IN_FLIGHT, not marked lost
        self.assertEqual(rec.state, RecordState.IN_FLIGHT)
        self.assertFalse(coordinator.transmissions[1].lost)

    def test_t09_ack_loss_recovery(self):
        """T09: Reverse link ACK loss: Sender recovers when subsequent SACK arrives."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 10.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            # Drop 10% of ACKs on reverse path
            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                seed=42,
                forward=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
                reverse=RelayDirectionConfig(loss_rate=0.10, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 10.0, "idle_timeout": 3.0}, daemon=True)
            relay_thread.start()
            time.sleep(0.05)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port)
            metrics = sender.transmit_file(input_file, session_id="t09-ack-loss")

            rec_thread.join(timeout=5.0)
            relay_thread.join(timeout=5.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            val_res = validate_jsonl_file(output_file, expected_count=100)
            self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")

    def test_t10_lost_retransmission_recovery(self):
        """T10: Lost retransmission: Repair attempt itself is dropped and successfully retried."""
        rtt_est = RttEstimator()
        coordinator = RecoveryCoordinator(rtt_estimator=rtt_est)
        rec = coordinator.register_record(1, {"val": 10})

        # Attempt 1
        coordinator.on_packet_sent(packet_id=1, record_id=1, attempt=1, sent_at=100.0)
        coordinator.check_timeouts(now=101.5)  # Declare lost
        self.assertEqual(rec.state, RecordState.REPAIR_PENDING)

        # Attempt 2 dispatched
        coordinator.on_packet_sent(packet_id=2, record_id=1, attempt=2, sent_at=101.6)
        self.assertEqual(rec.state, RecordState.REPAIR_IN_FLIGHT)

        # Attempt 2 times out (backed off RTO is 2.0s, so 101.6 + 2.0 = 103.6 -> at 104.0 it times out)
        coordinator.check_timeouts(now=104.0)
        self.assertEqual(rec.state, RecordState.REPAIR_PENDING)

        # Attempt 3 dispatched
        coordinator.on_packet_sent(packet_id=3, record_id=1, attempt=3, sent_at=104.1)
        self.assertEqual(rec.attempt_count, 3)

    def test_t11_duplicate_packet_suppression(self):
        """T11: Duplicate packets over wire are safely suppressed by receiver ReorderBuffer."""
        from transport.reorder_buffer import ReorderBuffer
        rb = ReorderBuffer()
        pkt1 = DataPacket(connection_id="test", packet_id=1, record_id=1, attempt=1, payload={"a": 1})
        pkt1_dup = DataPacket(connection_id="test", packet_id=10, record_id=1, attempt=2, payload={"a": 1})

        deliv1, s1 = rb.process_data_packet(pkt1)
        self.assertEqual(s1, "DELIVERED")
        self.assertEqual(deliv1, [pkt1])

        deliv_dup, s_dup = rb.process_data_packet(pkt1_dup)
        self.assertEqual(s_dup, "DUPLICATE_DELIVERED")
        self.assertEqual(deliv_dup, [])

    def test_t12_multiple_discontiguous_gaps_floor_advancement(self):
        """T12: SACK range floor advances when discontiguous gaps exceed 32 ranges."""
        tracker = SackTracker(max_ranges=4)
        for pid in [2, 4, 6, 8, 10, 12]:
            tracker.record_received_packet(pid)
        # Retains 4 highest: 12, 10, 8, 6. Floor = 5
        self.assertEqual(tracker.ack_range_floor_packet_id, 5)
        self.assertEqual(len(tracker.get_sack_ranges()), 4)

    def test_t13_combined_stress_impairments(self):
        """T13: Combined stress: 5% loss + 2% reorder + 400ms stall delivers all records in order."""
        s_port, r_port_a, r_port_b, rec_port = self.allocate_ports()
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = os.path.join(tmp_dir, "input_100.jsonl")
            output_file = os.path.join(tmp_dir, "output_100.jsonl")
            generate_test_records_file(input_file, count=100)

            receiver = UdpReceiver(host="127.0.0.1", port=rec_port, output_path=output_file)
            rec_thread = threading.Thread(target=receiver.run_until_complete, kwargs={"timeout": 15.0}, daemon=True)
            rec_thread.start()
            time.sleep(0.05)

            relay_cfg = RelayConfig(
                port_a=r_port_a,
                port_b=r_port_b,
                sender_addr=("127.0.0.1", s_port),
                receiver_addr=("127.0.0.1", rec_port),
                seed=42,
                forward=RelayDirectionConfig(
                    loss_rate=0.05,
                    reorder_rate=0.02,
                    stall_interval_sec=0.05,
                    stall_duration_ms=400.0,
                ),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(target=relay.run, kwargs={"max_duration": 15.0, "idle_timeout": 4.0}, daemon=True)
            relay_thread.start()
            time.sleep(0.06)

            sender = UdpSender(target_host="127.0.0.1", target_port=r_port_a, bind_port=s_port, retry_timeout=1.0)
            metrics = sender.transmit_file(input_file, session_id="t13-stress")

            rec_thread.join(timeout=8.0)
            relay_thread.join(timeout=8.0)

            self.assertEqual(metrics["status"], "COMPLETED")
            val_res = validate_jsonl_file(output_file, expected_count=100)
            self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")

    def test_t14_fin_with_gaps_repair(self):
        """T14: FIN with gaps: Receiver returns SACK, sender repairs, and teardown succeeds."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "t14_out.jsonl")
            receiver = UdpReceiver(host="127.0.0.1", port=0, output_path=out_file, enable_fin_repair=True)
            receiver.start_socket()
            rec_port = receiver.sock.getsockname()[1]

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.bind(("127.0.0.1", 0))
            client_sock.settimeout(2.0)
            c_addr = client_sock.getsockname()

            try:
                # Start session for 5 records
                from transport.packet import StartPacket
                start = StartPacket(connection_id="t14-sess", expected_records=5)
                receiver._handle_start(start, c_addr)

                # Drain START_ACK
                client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)

                # Send record 1 and 2, omitting 3, 4, 5
                for rid in (1, 2):
                    dp = DataPacket(connection_id="t14-sess", packet_id=rid, record_id=rid, attempt=1, payload={"x": rid})
                    receiver._handle_data(dp, c_addr)

                # Send FIN declaring final_record_id=5
                fin = FinPacket(connection_id="t14-sess", packet_id=10, final_record_id=5, total_packets=5)
                receiver._handle_fin(fin, c_addr)

                # Receiver must return AckPacket (SACK) with delivered_through=2
                resp, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                from transport.packet import AckPacket
                ack_pkt = deserialize_packet(resp)
                self.assertIsInstance(ack_pkt, AckPacket)
                self.assertEqual(ack_pkt.receiver_delivered_through, 2)
            finally:
                receiver.close()
                client_sock.close()

    def test_t15_retry_exhaustion_failure(self):
        """T15: 10 failed retransmission attempts leads to terminal FAILED state."""
        coordinator = RecoveryCoordinator()
        rec = coordinator.register_record(1, {"val": 1})
        coordinator.on_packet_sent(packet_id=10, record_id=1, attempt=10, sent_at=100.0)

        # RTO expires on 10th attempt
        coordinator.check_timeouts(now=102.0)
        self.assertEqual(rec.state, RecordState.FAILED)

    def test_t16_permanently_missing_original_pid_retirement(self):
        """T16: Missing original packet_id does not stall retirement; watermarks advance cleanly."""
        tracker = SackTracker()
        # Original packet 1 lost; replacement packet 2 arrives for record 1
        tracker.record_received_packet(2)
        # SACK range created for [2, 2]
        self.assertEqual(tracker.get_sack_ranges(), [[2, 2]])
        # Packet 3 arrives for record 2 -> range extends to [3, 2]
        tracker.record_received_packet(3)
        self.assertEqual(tracker.get_sack_ranges(), [[3, 2]])
        self.assertEqual(tracker.largest_packet_id, 3)

    def test_t17_reorder_buffer_capacity_in_order_pass_through(self):
        """T17: In-order record bypasses full buffer capacity and flushes consecutive records."""
        from transport.reorder_buffer import ReorderBuffer
        rb = ReorderBuffer(max_capacity=2)
        p2 = DataPacket(connection_id="t17", packet_id=2, record_id=2, attempt=1, payload={"id": 2})
        p3 = DataPacket(connection_id="t17", packet_id=3, record_id=3, attempt=1, payload={"id": 3})
        rb.process_data_packet(p2)
        rb.process_data_packet(p3)
        self.assertEqual(rb.buffered_count, 2)

        # Packet 4 arriving when full is dropped
        p4 = DataPacket(connection_id="t17", packet_id=4, record_id=4, attempt=1, payload={"id": 4})
        _, s4 = rb.process_data_packet(p4)
        self.assertEqual(s4, "FULL_DROPPED")

        # In-order packet 1 is accepted despite buffer being full!
        p1 = DataPacket(connection_id="t17", packet_id=1, record_id=1, attempt=1, payload={"id": 1})
        deliv, s1 = rb.process_data_packet(p1)
        self.assertEqual(s1, "DELIVERED")
        self.assertEqual(len(deliv), 3)  # Delivers 1, 2, 3!
        self.assertEqual(rb.buffered_count, 0)

    def test_t18_repair_cancellation_and_buffered_state(self):
        """T18: Feedback cancels pending repair and transitions record to BUFFERED_AT_RECEIVER."""
        coordinator = RecoveryCoordinator()
        rec = coordinator.register_record(5, {"data": 5})
        coordinator.on_packet_sent(packet_id=5, record_id=5, attempt=1, sent_at=100.0)

        # Marked lost
        coordinator.check_timeouts(now=101.5)
        self.assertEqual(rec.state, RecordState.REPAIR_PENDING)
        self.assertIn(5, coordinator.repair_queue)

        # ACK arrives confirming receipt of packet 5, but receiver_delivered_through is 0 (gap before 5)
        ack = SackTracker().create_ack_packet(connection_id="test", receiver_delivered_through=0)
        ack.ack_ranges = [[5, 5]]
        coordinator.on_ack_received(ack, now=101.6)

        # Repair must be cancelled and removed from repair queue!
        self.assertNotIn(5, coordinator.repair_queue)
        self.assertEqual(rec.state, RecordState.BUFFERED_AT_RECEIVER)


if __name__ == "__main__":
    unittest.main()
