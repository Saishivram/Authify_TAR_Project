"""
tests/test_sack.py
==================
Unit tests for SackTracker and SACK range retirement mechanics.
"""

import unittest
from transport.sack import SackTracker
from transport.packet import MAX_SACK_RANGES, AckPacket


class TestSackTracker(unittest.TestCase):
    def test_initial_state(self):
        tracker = SackTracker()
        self.assertEqual(tracker.largest_packet_id, 0)
        self.assertEqual(tracker.ack_received_through_packet_id, 0)
        self.assertEqual(tracker.ack_range_floor_packet_id, 0)
        self.assertEqual(tracker.get_sack_ranges(), [])

    def test_contiguous_in_order(self):
        tracker = SackTracker()
        for pid in [1, 2, 3, 4, 5]:
            self.assertTrue(tracker.record_received_packet(pid))
            self.assertEqual(tracker.ack_received_through_packet_id, pid)
            self.assertEqual(tracker.ack_range_floor_packet_id, pid)
            self.assertEqual(tracker.largest_packet_id, pid)
            self.assertEqual(tracker.get_sack_ranges(), [])

    def test_duplicate_suppression(self):
        tracker = SackTracker()
        self.assertTrue(tracker.record_received_packet(1))
        self.assertTrue(tracker.record_received_packet(2))
        # Cumulative duplicate
        self.assertFalse(tracker.record_received_packet(1))
        self.assertFalse(tracker.record_received_packet(2))
        self.assertEqual(tracker.ack_received_through_packet_id, 2)

        # SACK range duplicate
        self.assertTrue(tracker.record_received_packet(5))
        self.assertFalse(tracker.record_received_packet(5))
        self.assertEqual(tracker.get_sack_ranges(), [[5, 5]])

    def test_discontiguous_and_merging(self):
        tracker = SackTracker()
        # Receive 1, 2
        tracker.record_received_packet(1)
        tracker.record_received_packet(2)

        # Receive 5 -> [[5, 5]]
        self.assertTrue(tracker.record_received_packet(5))
        self.assertEqual(tracker.get_sack_ranges(), [[5, 5]])
        self.assertEqual(tracker.largest_packet_id, 5)

        # Receive 6 -> extends upward -> [[6, 5]]
        self.assertTrue(tracker.record_received_packet(6))
        self.assertEqual(tracker.get_sack_ranges(), [[6, 5]])

        # Receive 9 -> new discontiguous range -> [[9, 9], [6, 5]]
        self.assertTrue(tracker.record_received_packet(9))
        self.assertEqual(tracker.get_sack_ranges(), [[9, 9], [6, 5]])

        # Receive 8 -> extends downward -> [[9, 8], [6, 5]]
        self.assertTrue(tracker.record_received_packet(8))
        self.assertEqual(tracker.get_sack_ranges(), [[9, 8], [6, 5]])

        # Bridge merge: receive 7 -> merges [[9, 8]] and [[6, 5]] into [[9, 5]]
        self.assertTrue(tracker.record_received_packet(7))
        self.assertEqual(tracker.get_sack_ranges(), [[9, 5]])

    def test_absorb_into_cumulative(self):
        tracker = SackTracker()
        tracker.record_received_packet(1)
        tracker.record_received_packet(2)
        # SACK range [5, 4]
        tracker.record_received_packet(5)
        tracker.record_received_packet(4)
        self.assertEqual(tracker.get_sack_ranges(), [[5, 4]])
        self.assertEqual(tracker.ack_received_through_packet_id, 2)

        # Now packet 3 arrives, connecting contiguous watermark to [5, 4]
        self.assertTrue(tracker.record_received_packet(3))
        self.assertEqual(tracker.ack_received_through_packet_id, 5)
        self.assertEqual(tracker.get_sack_ranges(), [])
        self.assertEqual(tracker.ack_range_floor_packet_id, 5)

    def test_max_ranges_and_floor_truncation(self):
        tracker = SackTracker(max_ranges=4)
        # Create 6 separated ranges: 2, 4, 6, 8, 10, 12
        # Cumulative watermark is 0
        for pid in [2, 4, 6, 8, 10, 12]:
            tracker.record_received_packet(pid)

        # Should only retain the 4 highest ranges: 12, 10, 8, 6
        ranges = tracker.get_sack_ranges()
        self.assertEqual(len(ranges), 4)
        self.assertEqual(ranges, [[12, 12], [10, 10], [8, 8], [6, 6]])

        # Lowest retained range is [6, 6]. Floor should advance to 6 - 1 = 5
        self.assertEqual(tracker.ack_range_floor_packet_id, 5)
        self.assertEqual(tracker.ack_received_through_packet_id, 0)
        self.assertEqual(tracker.largest_packet_id, 12)

        # Classification check with truncated history:
        # 0 is CUMULATIVE_ACK (<= 0)
        self.assertEqual(tracker.classify_packet(0), "CUMULATIVE_ACK")
        # 2 and 4 are below floor (<= 5) and > 0 -> UNKNOWN
        self.assertEqual(tracker.classify_packet(2), "UNKNOWN")
        self.assertEqual(tracker.classify_packet(3), "UNKNOWN")
        self.assertEqual(tracker.classify_packet(4), "UNKNOWN")
        self.assertEqual(tracker.classify_packet(5), "UNKNOWN")
        # 6 is in SACK range
        self.assertEqual(tracker.classify_packet(6), "SACK_ACK")
        # 7 is unacknowledged gap (5 < 7 <= 12, not in ranges)
        self.assertEqual(tracker.classify_packet(7), "UNACKNOWLEDGED")
        # 12 is in SACK range
        self.assertEqual(tracker.classify_packet(12), "SACK_ACK")
        # 13 is unseen
        self.assertEqual(tracker.classify_packet(13), "UNSEEN")

    def test_floor_monotonicity(self):
        tracker = SackTracker(max_ranges=2)
        # 2, 4, 6
        tracker.record_received_packet(2)
        tracker.record_received_packet(4)
        # Truncates 2 when 6 arrives -> retained: [[6, 6], [4, 4]]
        tracker.record_received_packet(6)
        # Lowest retained is 4 -> floor is 3
        self.assertEqual(tracker.ack_range_floor_packet_id, 3)

        # Watermark advances to 1
        tracker.record_received_packet(1)
        self.assertEqual(tracker.ack_received_through_packet_id, 1)
        # Floor must remain >= 3
        self.assertEqual(tracker.ack_range_floor_packet_id, 3)

    def test_create_ack_packet(self):
        tracker = SackTracker()
        tracker.record_received_packet(1)
        tracker.record_received_packet(2)
        tracker.record_received_packet(5)

        ack = tracker.create_ack_packet(
            connection_id="test-conn-123",
            receiver_delivered_through=2,
            ack_delay_ms=1.5,
        )
        self.assertIsInstance(ack, AckPacket)
        self.assertEqual(ack.connection_id, "test-conn-123")
        self.assertEqual(ack.largest_packet_id, 5)
        self.assertEqual(ack.ack_received_through_packet_id, 2)
        self.assertEqual(ack.ack_range_floor_packet_id, 2)
        self.assertEqual(ack.ack_ranges, [[5, 5]])
        self.assertEqual(ack.ack_delay_ms, 1.5)
        self.assertEqual(ack.receiver_delivered_through, 2)

    def test_invalid_packet_id(self):
        tracker = SackTracker()
        with self.assertRaises(ValueError):
            tracker.record_received_packet(True)
        with self.assertRaises(ValueError):
            tracker.record_received_packet(False)
        with self.assertRaises(ValueError):
            tracker.record_received_packet(0)
        with self.assertRaises(ValueError):
            tracker.record_received_packet(-5)
        with self.assertRaises(ValueError):
            tracker.record_received_packet("10")  # type: ignore


if __name__ == "__main__":
    unittest.main()
