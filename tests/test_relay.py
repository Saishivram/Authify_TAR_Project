"""
test_relay.py
=============
Automated test suite for the TAR Network Impairment Relay (Phase 3).

Covers all 13 planned test cases:
- TR01: Zero-impairment clean bidirectional forwarding.
- TR02: Deterministic loss repeatability and statistical rate compliance.
- TR03: Packet reordering with delay jitter and out-of-order metric tracking.
- TR04: 400 ms link stall queue and release.
- TR05: Preservation of configured reordering across stall boundary.
- TR06: Exact overlapping stall scenario (10.0s -> 10.4s -> 10.43s).
- TR07: Previously queued packets shifted cleanly during stall.
- TR08: Independent forward and reverse scheduling (reverse traffic not blocked by forward stall).
- TR09: Combined impairments stress test (loss + reordering + stall).
- TR10: Targeted fault injection (drop specific packet_id, delay specific packet_id).
- TR11: Targeted stall affecting only the configured direction.
- TR12: Malformed and oversized datagram safety.
- TR13: Complete 3-process end-to-end clean integration test over UDP loopback sockets.
- TR14: Reproduction of manual stall configuration (--loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42).
"""

import json
import os
import random
import socket
import tempfile
import threading
import time
import unittest
from typing import List, Optional

from schemas.record_schema import validate_jsonl_file
from transport.packet import (
    DEFAULT_RECEIVE_BUFFER_SIZE,
    DataPacket,
    FinAckPacket,
    FinPacket,
    StartAckPacket,
    StartPacket,
    deserialize_packet,
    serialize_packet,
)
from transport.receiver import UdpReceiver
from transport.relay import (
    DirectionScheduler,
    ImpairmentRelay,
    RelayConfig,
    RelayDirectionConfig,
    TargetedRule,
    peek_packet_info,
)
from transport.sender import UdpSender


class MockClock:
    """Controllable monotonic clock for deterministic unit tests."""
    def __init__(self, start_time: float = 1000.0):
        self.current_time = start_time

    def __call__(self) -> float:
        return self.current_time

    def advance(self, delta_sec: float) -> float:
        self.current_time += delta_sec
        return self.current_time


class TestRelayUnitAndIntegration(unittest.TestCase):
    """Comprehensive test suite for the Network Impairment Relay."""

    def test_tr01_zero_impairment_regression(self):
        """TR01: Verify 100% clean bidirectional forwarding with zero impairments."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(
            loss_rate=0.0,
            reorder_rate=0.0,
            stall_interval_sec=0.0,
            stall_duration_ms=400.0,
        )
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)

        packets = [
            serialize_packet(DataPacket(connection_id="s1", packet_id=i, record_id=i, payload={"v": f"msg-{i}"}))
            for i in range(1, 21)
        ]

        dummy_addr = ("127.0.0.1", 9004)
        for pkt in packets:
            scheduler.handle_packet(pkt, dummy_addr, None, now=clock())

        # Advance clock and pop
        clock.advance(0.001)
        dispatched = scheduler.pop_eligible(clock())

        self.assertEqual(len(dispatched), 20)
        for i, item in enumerate(dispatched, start=1):
            self.assertEqual(item.raw_bytes, packets[i - 1])
            self.assertEqual(item.packet_info["packet_id"], i)

        metrics = scheduler.metrics
        self.assertEqual(metrics["packets_received"], 20)
        self.assertEqual(metrics["packets_forwarded"], 20)
        self.assertEqual(metrics["packets_dropped_loss"], 0)
        self.assertEqual(metrics["packets_dispatched_out_of_order"], 0)
        self.assertEqual(metrics["stalls_triggered"], 0)

    def test_tr02_deterministic_loss_repeatability(self):
        """TR02: Verify identical loss decisions with same seed and statistical compliance."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.10, reorder_rate=0.0, stall_interval_sec=0.0)

        def run_loss_simulation(seed: int, count: int) -> List[int]:
            rng = random.Random(seed)
            sch = DirectionScheduler("FORWARD", cfg, rng, time_func=clock)
            dropped_ids = []
            dummy_addr = ("127.0.0.1", 9004)
            for i in range(1, count + 1):
                raw = serialize_packet(DataPacket(connection_id="s1", packet_id=i, record_id=i, payload={"v": i}))
                res = sch.handle_packet(raw, dummy_addr, None, now=clock())
                if res is None:
                    dropped_ids.append(i)
            return dropped_ids

        # 1. Exact repeatability with same seed
        run1 = run_loss_simulation(seed=42, count=500)
        run2 = run_loss_simulation(seed=42, count=500)
        self.assertEqual(run1, run2, "Runs with identical seed must produce identical drop sequences")
        self.assertGreater(len(run1), 0, "Loss rate > 0 must drop packets")

        # 2. Divergence with different seed
        run3 = run_loss_simulation(seed=999, count=500)
        self.assertNotEqual(run1, run3, "Runs with differing seeds must diverge")

        # 3. Statistical rate compliance over 1,000 packets (p=0.10, 3-sigma tolerance: 7% - 13%)
        large_run = run_loss_simulation(seed=42, count=1000)
        observed_rate = len(large_run) / 1000.0
        self.assertGreaterEqual(observed_rate, 0.07, f"Observed loss rate {observed_rate} too low")
        self.assertLessEqual(observed_rate, 0.13, f"Observed loss rate {observed_rate} too high")

    def test_tr03_packet_reordering_independent(self):
        """TR03: Verify packets marked for reorder arrive out-of-order and metrics reflect it."""
        clock = MockClock(10.0)
        # 100% reorder rate on every second packet using a targeted rule or targeted delay
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)

        dummy_addr = ("127.0.0.1", 9004)

        # Packet 1: delay by 50 ms
        rule_delay = [TargetedRule(action="DELAY", target_packet_id=1, delay_ms=50.0)]
        pkt1 = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))
        scheduler.handle_packet(pkt1, dummy_addr, None, now=clock(), targeted_rules=rule_delay)

        # Advance clock by 5 ms
        clock.advance(0.005)

        # Packet 2: normal forward (0 ms delay)
        pkt2 = serialize_packet(DataPacket(connection_id="s1", packet_id=2, record_id=2, payload={"v": 2}))
        scheduler.handle_packet(pkt2, dummy_addr, None, now=clock())

        # At t=10.005: Packet 2 is eligible, Packet 1 is scheduled for 10.050
        popped_early = scheduler.pop_eligible(clock())
        self.assertEqual(len(popped_early), 1)
        self.assertEqual(popped_early[0].packet_info["packet_id"], 2, "Packet 2 should overtake Packet 1")

        # Advance to t=10.055
        clock.advance(0.050)
        popped_late = scheduler.pop_eligible(clock())
        self.assertEqual(len(popped_late), 1)
        self.assertEqual(popped_late[0].packet_info["packet_id"], 1, "Packet 1 should dispatch after Packet 2")

        self.assertEqual(scheduler.metrics["packets_delayed_for_reordering"], 1)
        self.assertEqual(scheduler.metrics["packets_dispatched_out_of_order"], 1)

    def test_tr04_stall_queue_and_release(self):
        """TR04: Verify 400 ms link stall holds packets and releases all upon expiration."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # Trigger 400 ms stall at t=10.0 (ends at 10.4)
        scheduler.trigger_stall(0.400, now=clock())
        self.assertEqual(scheduler.stall_end_time, 10.4)

        # Enqueue 5 packets during stall
        for i in range(1, 6):
            clock.advance(0.010)
            raw = serialize_packet(DataPacket(connection_id="s1", packet_id=i, record_id=i, payload={"v": i}))
            scheduler.handle_packet(raw, dummy_addr, None, now=clock())

        # While stall is active (t=10.2), no packets may leave
        self.assertEqual(len(scheduler.pop_eligible(clock())), 0, "No packet may be dispatched during an active stall")

        # Advance clock to t=10.401 (stall expired)
        clock.current_time = 10.401
        released = scheduler.pop_eligible(clock())
        self.assertEqual(len(released), 5, "All queued packets must be released after stall expiration")
        self.assertEqual([item.packet_info["packet_id"] for item in released], [1, 2, 3, 4, 5])

    def test_tr05_stall_preserves_configured_reordering(self):
        """TR05: Verify reordering assigned during or before stall is strictly preserved upon release."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # Trigger 400 ms stall at t=10.0 (ends at 10.4)
        scheduler.trigger_stall(0.400, now=clock())

        # Packet 1 arrives during stall; assigned 30 ms reorder jitter
        rules_p1 = [TargetedRule(action="DELAY", target_packet_id=1, delay_ms=30.0)]
        raw1 = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))
        scheduler.handle_packet(raw1, dummy_addr, None, now=clock(), targeted_rules=rules_p1)

        # Packet 2 arrives during stall; normal delay (0 ms)
        clock.advance(0.010)
        raw2 = serialize_packet(DataPacket(connection_id="s1", packet_id=2, record_id=2, payload={"v": 2}))
        scheduler.handle_packet(raw2, dummy_addr, None, now=clock())

        # Advance to exactly t=10.400 (stall boundary)
        clock.current_time = 10.400
        first_wave = scheduler.pop_eligible(clock())
        self.assertEqual(len(first_wave), 1)
        self.assertEqual(first_wave[0].packet_info["packet_id"], 2, "Packet 2 (0ms jitter) must overtake Packet 1")

        # Advance to t=10.430 (stall boundary + 30 ms)
        clock.current_time = 10.430
        second_wave = scheduler.pop_eligible(clock())
        self.assertEqual(len(second_wave), 1)
        self.assertEqual(second_wave[0].packet_info["packet_id"], 1, "Packet 1 (30ms jitter) must dispatch second")

    def test_tr06_overlapping_stall_exact_scenario(self):
        """
        TR06: Exact required scenario:
        - Existing stall ends at 10.0s.
        - A packet is scheduled for 10.03s.
        - At 9.9s, another 400ms stall is triggered.
        - New stall ends at 10.4s.
        - The packet must be scheduled for 10.43s, preserving its 30ms offset.
        """
        clock = MockClock(9.5)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # 1. Existing stall ends at 10.0s
        scheduler.stall_end_time = 10.0

        # 2. Packet scheduled for 10.03s (30 ms boundary offset)
        rule_delay = [TargetedRule(action="DELAY", target_packet_id=1, delay_ms=30.0)]
        raw = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))
        item = scheduler.handle_packet(raw, dummy_addr, None, now=clock(), targeted_rules=rule_delay)

        self.assertIsNotNone(item)
        self.assertAlmostEqual(item.dispatch_time, 10.03, places=5)
        self.assertAlmostEqual(item.boundary_offset, 0.03, places=5)

        # 3. At 9.9s, another 400 ms stall is triggered
        clock.current_time = 9.9
        new_stall_end = scheduler.trigger_stall(0.400, now=clock())

        # 4. New stall ends at 10.4s
        self.assertAlmostEqual(new_stall_end, 10.4, places=5)
        self.assertAlmostEqual(scheduler.stall_end_time, 10.4, places=5)

        # 5. Verify the packet is now scheduled for 10.43s
        self.assertEqual(len(scheduler.heap), 1)
        rescheduled_item = scheduler.heap[0]
        self.assertAlmostEqual(
            rescheduled_item.dispatch_time,
            10.43,
            places=5,
            msg="Overlapping stall must shift packet to exactly 10.43s, preserving its 30ms offset",
        )
        self.assertAlmostEqual(rescheduled_item.boundary_offset, 0.03, places=5)

    def test_tr07_previously_queued_packets_during_stall(self):
        """TR07: Verify packets scheduled before stall are shifted forward while preserving relative spacing."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # Schedule Packet 1 at 10.02 (delay 20ms) and Packet 2 at 10.05 (delay 50ms)
        r1 = [TargetedRule(action="DELAY", target_packet_id=1, delay_ms=20.0)]
        r2 = [TargetedRule(action="DELAY", target_packet_id=2, delay_ms=50.0)]
        raw1 = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))
        raw2 = serialize_packet(DataPacket(connection_id="s1", packet_id=2, record_id=2, payload={"v": 2}))

        scheduler.handle_packet(raw1, dummy_addr, None, now=clock(), targeted_rules=r1)
        scheduler.handle_packet(raw2, dummy_addr, None, now=clock(), targeted_rules=r2)

        # Trigger 400ms stall at t=10.01
        clock.current_time = 10.01
        scheduler.trigger_stall(0.400, now=clock())
        self.assertAlmostEqual(scheduler.stall_end_time, 10.41, places=4)

        # Both items shifted: Packet 1 to ~10.42, Packet 2 to ~10.45 (spacing preserved: 30 ms)
        clock.current_time = 10.419
        self.assertEqual(len(scheduler.pop_eligible(clock())), 0)

        clock.current_time = 10.421
        p1 = scheduler.pop_eligible(clock())
        self.assertEqual(len(p1), 1)
        self.assertEqual(p1[0].packet_info["packet_id"], 1)

        clock.current_time = 10.451
        p2 = scheduler.pop_eligible(clock())
        self.assertEqual(len(p2), 1)
        self.assertEqual(p2[0].packet_info["packet_id"], 2)

    def test_tr08_independent_forward_and_reverse_scheduling(self):
        """TR08: Verify stall on forward path does NOT block or delay eligible packets on reverse path."""
        clock = MockClock(10.0)
        cfg_fwd = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        cfg_rev = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        fwd_sch = DirectionScheduler("FORWARD", cfg_fwd, random.Random(42), time_func=clock)
        rev_sch = DirectionScheduler("REVERSE", cfg_rev, random.Random(42), time_func=clock)

        # Trigger 400ms stall on FORWARD path
        fwd_sch.trigger_stall(0.400, now=clock())
        self.assertAlmostEqual(fwd_sch.stall_end_time, 10.40)
        self.assertEqual(rev_sch.stall_end_time, 0.0, "Reverse path stall state must remain unaffected")

        # Enqueue datagrams on both paths at t=10.05
        clock.advance(0.05)
        raw_fwd = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))
        raw_rev = serialize_packet(StartAckPacket(connection_id="s1", status="READY", expected_records=100))

        fwd_sch.handle_packet(raw_fwd, ("127.0.0.1", 9004), None, now=clock())
        rev_sch.handle_packet(raw_rev, ("127.0.0.1", 9001), None, now=clock())

        # Advance clock to t=10.06: Forward must be blocked; Reverse must dispatch immediately!
        clock.advance(0.01)
        self.assertEqual(len(fwd_sch.pop_eligible(clock())), 0, "Forward path must remain stalled")
        rev_eligible = rev_sch.pop_eligible(clock())
        self.assertEqual(len(rev_eligible), 1, "Reverse path must dispatch immediately despite forward stall")
        self.assertEqual(rev_eligible[0].packet_info["type"], "START_ACK")

    def test_tr09_combined_impairments_stress(self):
        """TR09: Verify combined loss (5%), reordering (10%), and 400ms stall run safely without crashes."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(
            loss_rate=0.05,
            reorder_rate=0.10,
            reorder_min_delay_ms=20.0,
            reorder_max_delay_ms=50.0,
            stall_interval_sec=0.0,  # Explicitly trigger stall via rule
        )
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # Stall trigger at packet 50
        rules = [TargetedRule(action="STALL", target_packet_id=50, stall_ms=400.0)]

        packets_sent = 200
        for i in range(1, packets_sent + 1):
            clock.advance(0.001)
            raw = serialize_packet(DataPacket(connection_id="stress-sess", packet_id=i, record_id=i, payload={"v": i}))
            scheduler.handle_packet(raw, dummy_addr, None, now=clock(), targeted_rules=rules)

        # Advance past all possible delays (stall 400ms + max jitter 50ms)
        clock.advance(2.0)
        dispatched = scheduler.pop_eligible(clock())

        metrics = scheduler.metrics
        self.assertEqual(metrics["packets_received"], packets_sent)
        self.assertEqual(
            metrics["packets_forwarded"] + metrics["packets_dropped_loss"],
            packets_sent,
            "Total received must equal forwarded + dropped",
        )
        self.assertGreater(metrics["packets_dropped_loss"], 0, "5% loss should drop some packets")
        self.assertGreater(metrics["packets_delayed_for_reordering"], 0, "10% reordering should delay some packets")
        self.assertEqual(metrics["stalls_triggered"], 1, "Exactly one stall should have been triggered")
        self.assertEqual(len(scheduler.heap), 0, "All items must be drained after full time advance")

    def test_tr10_targeted_fault_drop_and_delay(self):
        """TR10: Verify targeted rules drop specific packet_id and delay specific packet_id."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        rules = [
            TargetedRule(action="DROP", target_packet_id=7),
            TargetedRule(action="DELAY", target_packet_id=3, delay_ms=80.0),
        ]

        for i in range(1, 11):
            raw = serialize_packet(DataPacket(connection_id="s1", packet_id=i, record_id=i, payload={"v": i}))
            scheduler.handle_packet(raw, dummy_addr, None, now=clock(), targeted_rules=rules)

        clock.advance(0.010)
        first_wave = scheduler.pop_eligible(clock())
        first_pids = [item.packet_info["packet_id"] for item in first_wave]

        # Pkt 7 was dropped; Pkt 3 is delayed by 80ms
        self.assertNotIn(7, first_pids, "Packet 7 must be dropped by targeted rule")
        self.assertNotIn(3, first_pids, "Packet 3 must be delayed by targeted rule")
        self.assertEqual(set(first_pids), {1, 2, 4, 5, 6, 8, 9, 10})

        # Advance past 80ms
        clock.advance(0.100)
        second_wave = scheduler.pop_eligible(clock())
        self.assertEqual(len(second_wave), 1)
        self.assertEqual(second_wave[0].packet_info["packet_id"], 3, "Packet 3 must arrive after its 80ms delay")
        self.assertEqual(scheduler.metrics["packets_dropped_targeted"], 1)

    def test_tr11_targeted_stall_single_direction(self):
        """TR11: Verify targeted stall on FORWARD direction does not stall REVERSE direction."""
        clock = MockClock(10.0)
        cfg_fwd = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        cfg_rev = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        fwd_sch = DirectionScheduler("FORWARD", cfg_fwd, random.Random(42), time_func=clock)
        rev_sch = DirectionScheduler("REVERSE", cfg_rev, random.Random(42), time_func=clock)

        rule = [TargetedRule(action="STALL", target_packet_id=1, stall_ms=300.0, direction="FORWARD")]
        raw1 = serialize_packet(DataPacket(connection_id="s1", packet_id=1, record_id=1, payload={"v": 1}))

        fwd_sch.handle_packet(raw1, ("127.0.0.1", 9004), None, now=clock(), targeted_rules=rule)
        self.assertAlmostEqual(fwd_sch.stall_end_time, 10.300)
        self.assertEqual(rev_sch.stall_end_time, 0.0)

    def test_tr12_malformed_and_oversized_datagram_safety(self):
        """TR12: Verify non-JSON, garbage bytes, and large datagrams do not crash the relay."""
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        scheduler = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # 1. Non-JSON string
        garbage = b"This is not JSON at all! Just raw arbitrary bytes."
        res1 = scheduler.handle_packet(garbage, dummy_addr, None, now=clock())
        self.assertIsNotNone(res1)
        self.assertFalse(res1.packet_info["valid"])

        # 2. Large binary datagram (4,000 bytes)
        big_binary = os.urandom(4000)
        res2 = scheduler.handle_packet(big_binary, dummy_addr, None, now=clock())
        self.assertIsNotNone(res2)
        self.assertEqual(res2.packet_info["byte_len"], 4000)

        clock.advance(0.01)
        popped = scheduler.pop_eligible(clock())
        self.assertEqual(len(popped), 2)
        self.assertEqual(popped[0].raw_bytes, garbage)
        self.assertEqual(popped[1].raw_bytes, big_binary)

    def test_tr13_e2e_three_process_clean_run(self):
        """TR13: Verify full end-to-end communication through Relay over UDP loopback sockets."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "e2e_relay_received.jsonl")

            # Ports for integration test
            receiver_port = 19014
            relay_port_a = 19012  # Sender talks to Port A
            relay_port_b = 19013  # Relay talks to Receiver from Port B
            sender_port = 19011

            # 1. Start Receiver
            receiver = UdpReceiver(
                host="127.0.0.1",
                port=receiver_port,
                output_path=out_file,
            )
            rec_thread = threading.Thread(
                target=receiver.run_until_complete,
                kwargs={"timeout": 10.0},
                daemon=True,
            )
            rec_thread.start()
            time.sleep(0.1)

            # 2. Start Impairment Relay (Zero impairment for clean test)
            relay_cfg = RelayConfig(
                port_a=relay_port_a,
                port_b=relay_port_b,
                sender_addr=("127.0.0.1", sender_port),
                receiver_addr=("127.0.0.1", receiver_port),
                seed=42,
                forward=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
                reverse=RelayDirectionConfig(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(
                target=relay.run,
                kwargs={"max_duration": 10.0, "idle_timeout": 2.5},
                daemon=True,
            )
            relay_thread.start()
            time.sleep(0.1)

            try:
                # 3. Start Sender targeting Relay Port A (19012)
                sender = UdpSender(
                    target_host="127.0.0.1",
                    target_port=relay_port_a,
                    bind_port=sender_port,
                )

                sender_metrics = sender.transmit_file(
                    input_file_path="data/sample_100.jsonl",
                    session_id="relay-e2e-100",
                    pace_interval=0.0001,
                )

                # Wait for Receiver and Relay
                rec_thread.join(timeout=4.0)
                relay_thread.join(timeout=4.0)

                # Assert Sender
                self.assertEqual(sender_metrics["status"], "COMPLETED")
                self.assertEqual(sender_metrics["sent_records"], 100)

                # Assert Receiver
                rec_metrics = receiver.get_metrics()
                self.assertEqual(rec_metrics["status"], "COMPLETED")
                self.assertEqual(rec_metrics["received_records"], 100)
                self.assertEqual(rec_metrics["missing_records_count"], 0)
                self.assertEqual(rec_metrics["duplicate_records_count"], 0)
                self.assertEqual(rec_metrics["out_of_order_records_count"], 0)

                # Assert Relay
                relay_metrics = relay.get_metrics()
                self.assertGreaterEqual(relay_metrics["forward"]["packets_forwarded"], 102)  # START + 100 DATA + FIN
                self.assertGreaterEqual(relay_metrics["reverse"]["packets_forwarded"], 2)    # START_ACK + FIN_ACK

                # Validate output JSONL
                val_res = validate_jsonl_file(out_file, expected_count=100)
                self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")
                self.assertEqual(val_res["total_records"], 100)

            finally:
                receiver.close()
                relay.close()

    def test_tr14_manual_stall_configuration_reproduction(self):
        """
        TR14: Reproduction of manual stall configuration:
        --loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42.

        Verifies:
        1. At least one stall is triggered when packets arrive after interval has elapsed.
        2. Packets arriving during the stall are queued and delayed.
        3. All queued packets are forwarded once the stall expires.
        4. In full 3-process loopback transmission, all 100 records are received and verified.
        """
        # Part A: Deterministic scheduler verification with MockClock
        clock = MockClock(10.0)
        cfg = RelayDirectionConfig(
            loss_rate=0.0,
            reorder_rate=0.0,
            stall_interval_sec=0.05,
            stall_duration_ms=400.0,
        )
        sch = DirectionScheduler("FORWARD", cfg, random.Random(42), time_func=clock)
        dummy_addr = ("127.0.0.1", 9004)

        # 60 ms elapses after scheduler creation before first packet arrives
        clock.advance(0.060)

        # Send 102 packets burst (1 START + 100 DATA + 1 FIN)
        raw_start = serialize_packet(StartPacket(connection_id="s1", expected_records=100))
        sch.handle_packet(raw_start, dummy_addr, None, now=clock())

        for i in range(1, 101):
            clock.advance(0.0001)
            raw = serialize_packet(DataPacket(connection_id="s1", packet_id=i, record_id=i, payload={"v": i}))
            sch.handle_packet(raw, dummy_addr, None, now=clock())

        clock.advance(0.0001)
        raw_fin = serialize_packet(FinPacket(connection_id="s1", packet_id=101, final_record_id=100, total_packets=102))
        sch.handle_packet(raw_fin, dummy_addr, None, now=clock())

        # Assert stall was triggered and packets delayed
        self.assertGreaterEqual(sch.metrics["stalls_triggered"], 1)
        self.assertGreater(sch.metrics["packets_delayed_by_stall"], 0)
        self.assertAlmostEqual(sch.stall_end_time, 10.460, places=3)

        # While stall is active at t=10.20, nothing is eligible
        clock.current_time = 10.200
        self.assertEqual(len(sch.pop_eligible(clock())), 0)

        # After stall expires at t=10.461, all 102 packets are released in order
        clock.current_time = 10.461
        released = sch.pop_eligible(clock())
        self.assertEqual(len(released), 102)
        self.assertEqual(released[0].packet_info["type"], "START")
        self.assertEqual(released[-1].packet_info["type"], "FIN")

        # Part B: Full 3-process end-to-end loopback integration with exact CLI parameters
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "stalled_relay_received.jsonl")

            receiver_port = 19024
            relay_port_a = 19022
            relay_port_b = 19023
            sender_port = 19021

            # 1. Receiver
            receiver = UdpReceiver(
                host="127.0.0.1",
                port=receiver_port,
                output_path=out_file,
            )
            rec_thread = threading.Thread(
                target=receiver.run_until_complete,
                kwargs={"timeout": 10.0},
                daemon=True,
            )
            rec_thread.start()
            time.sleep(0.05)

            # 2. Relay with --loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42
            relay_cfg = RelayConfig(
                port_a=relay_port_a,
                port_b=relay_port_b,
                sender_addr=("127.0.0.1", sender_port),
                receiver_addr=("127.0.0.1", receiver_port),
                seed=42,
                forward=RelayDirectionConfig(
                    loss_rate=0.0,
                    reorder_rate=0.0,
                    stall_interval_sec=0.05,
                    stall_duration_ms=400.0,
                ),
                reverse=RelayDirectionConfig(
                    loss_rate=0.0,
                    reorder_rate=0.0,
                    stall_interval_sec=0.0,
                    stall_duration_ms=400.0,
                ),
            )
            relay = ImpairmentRelay(config=relay_cfg)
            relay_thread = threading.Thread(
                target=relay.run,
                kwargs={"max_duration": 10.0, "idle_timeout": 2.5},
                daemon=True,
            )
            relay_thread.start()
            # Wait 60 ms so that the 50 ms stall interval has elapsed since initialization
            time.sleep(0.06)

            try:
                # 3. Sender
                sender = UdpSender(
                    target_host="127.0.0.1",
                    target_port=relay_port_a,
                    bind_port=sender_port,
                    retry_timeout=1.0,
                )
                sender_metrics = sender.transmit_file(
                    input_file_path="data/sample_100.jsonl",
                    session_id="relay-stall-100",
                    pace_interval=0.0001,
                )

                rec_thread.join(timeout=6.0)
                relay_thread.join(timeout=6.0)

                # Sender assertions
                self.assertEqual(sender_metrics["status"], "COMPLETED")
                self.assertEqual(sender_metrics["sent_records"], 100)

                # Receiver assertions
                rec_metrics = receiver.get_metrics()
                self.assertEqual(rec_metrics["status"], "COMPLETED")
                self.assertEqual(rec_metrics["received_records"], 100)
                self.assertEqual(rec_metrics["missing_records_count"], 0)
                self.assertGreaterEqual(rec_metrics["duplicate_records_count"], 0)

                # Relay assertions
                relay_metrics = relay.get_metrics()
                self.assertGreaterEqual(
                    relay_metrics["forward"]["stalls_triggered"],
                    1,
                    "At least one stall must be triggered in the forward direction",
                )
                self.assertGreater(
                    relay_metrics["forward"]["packets_delayed_by_stall"],
                    0,
                    "Packets must be delayed by stall in forward direction",
                )
                self.assertGreaterEqual(
                    relay_metrics["forward"]["packets_forwarded"],
                    102,
                    "All forward packets must be forwarded after stall drains",
                )

                # File validation
                val_res = validate_jsonl_file(out_file, expected_count=100)
                self.assertTrue(val_res["valid"], f"Validation errors: {val_res['errors']}")
                self.assertEqual(val_res["total_records"], 100)

            finally:
                receiver.close()
                relay.close()


if __name__ == "__main__":
    unittest.main()
