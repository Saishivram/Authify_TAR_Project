"""
tests/test_reorder_buffer.py
============================
Unit tests for ReorderBuffer: in-order pass-through, gap flushing, capacity drops, duplicate suppression.
"""

import unittest
from transport.reorder_buffer import ReorderBuffer
from transport.packet import DataPacket


def make_data_packet(packet_id: int, record_id: int) -> DataPacket:
    return DataPacket(
        connection_id="test-conn",
        packet_id=packet_id,
        record_id=record_id,
        attempt=1,
        payload={"id": record_id, "data": f"record-{record_id}"},
    )


class TestReorderBuffer(unittest.TestCase):
    def test_in_order_sequential_delivery(self):
        rb = ReorderBuffer()
        self.assertEqual(rb.next_expected_record, 1)
        self.assertEqual(rb.receiver_delivered_through, 0)
        self.assertTrue(rb.is_empty)

        p1 = make_data_packet(1, 1)
        delivered, status = rb.process_data_packet(p1)
        self.assertEqual(status, "DELIVERED")
        self.assertEqual(delivered, [p1])
        self.assertEqual(rb.next_expected_record, 2)
        self.assertEqual(rb.receiver_delivered_through, 1)
        self.assertTrue(rb.is_empty)

        p2 = make_data_packet(2, 2)
        delivered, status = rb.process_data_packet(p2)
        self.assertEqual(status, "DELIVERED")
        self.assertEqual(delivered, [p2])
        self.assertEqual(rb.next_expected_record, 3)
        self.assertEqual(rb.receiver_delivered_through, 2)

    def test_gap_buffering_and_cascading_flush(self):
        rb = ReorderBuffer()
        p2 = make_data_packet(2, 2)
        p4 = make_data_packet(4, 4)
        p5 = make_data_packet(5, 5)

        # Arrive out of order: 2, 4, 5
        del2, stat2 = rb.process_data_packet(p2)
        self.assertEqual(stat2, "BUFFERED")
        self.assertEqual(del2, [])
        self.assertEqual(rb.buffered_count, 1)

        del4, stat4 = rb.process_data_packet(p4)
        self.assertEqual(stat4, "BUFFERED")
        self.assertEqual(del4, [])

        del5, stat5 = rb.process_data_packet(p5)
        self.assertEqual(stat5, "BUFFERED")
        self.assertEqual(del5, [])
        self.assertEqual(rb.buffered_count, 3)

        # In-order packet 1 arrives: should deliver 1 and flush 2
        p1 = make_data_packet(1, 1)
        del1, stat1 = rb.process_data_packet(p1)
        self.assertEqual(stat1, "DELIVERED")
        self.assertEqual(del1, [p1, p2])
        self.assertEqual(rb.next_expected_record, 3)
        self.assertEqual(rb.receiver_delivered_through, 2)
        self.assertEqual(rb.buffered_count, 2)  # 4 and 5 remain

        # Packet 3 arrives: should deliver 3 and flush 4, 5
        p3 = make_data_packet(3, 3)
        del3, stat3 = rb.process_data_packet(p3)
        self.assertEqual(stat3, "DELIVERED")
        self.assertEqual(del3, [p3, p4, p5])
        self.assertEqual(rb.next_expected_record, 6)
        self.assertEqual(rb.receiver_delivered_through, 5)
        self.assertTrue(rb.is_empty)

    def test_in_order_pass_through_at_capacity(self):
        # Buffer capacity = 3
        rb = ReorderBuffer(max_capacity=3)

        # Buffer records 2, 3, 4 -> reaches capacity 3
        p2 = make_data_packet(2, 2)
        p3 = make_data_packet(3, 3)
        p4 = make_data_packet(4, 4)
        rb.process_data_packet(p2)
        rb.process_data_packet(p3)
        rb.process_data_packet(p4)
        self.assertEqual(rb.buffered_count, 3)

        # An out-of-order record 6 arriving when full must be DROPPED
        p6 = make_data_packet(6, 6)
        del6, stat6 = rb.process_data_packet(p6)
        self.assertEqual(stat6, "FULL_DROPPED")
        self.assertEqual(del6, [])
        self.assertEqual(rb.buffered_count, 3)

        # Crucial Invariant: In-order record 1 arriving when full must be ACCEPTED
        p1 = make_data_packet(1, 1)
        del1, stat1 = rb.process_data_packet(p1)
        self.assertEqual(stat1, "DELIVERED")
        # Should flush 1, 2, 3, 4!
        self.assertEqual(del1, [p1, p2, p3, p4])
        self.assertEqual(rb.next_expected_record, 5)
        self.assertEqual(rb.receiver_delivered_through, 4)
        self.assertEqual(rb.buffered_count, 0)

    def test_duplicate_suppression(self):
        rb = ReorderBuffer()
        p1 = make_data_packet(1, 1)
        rb.process_data_packet(p1)

        # Duplicate of delivered record
        p1_dup = make_data_packet(10, 1)
        del_dup, stat_dup = rb.process_data_packet(p1_dup)
        self.assertEqual(stat_dup, "DUPLICATE_DELIVERED")
        self.assertEqual(del_dup, [])

        # Buffer record 3
        p3 = make_data_packet(3, 3)
        rb.process_data_packet(p3)

        # Duplicate of buffered record
        p3_dup = make_data_packet(11, 3)
        del_bdup, stat_bdup = rb.process_data_packet(p3_dup)
        self.assertEqual(stat_bdup, "DUPLICATE_BUFFERED")
        self.assertEqual(del_bdup, [])

    def test_invalid_parameters(self):
        with self.assertRaises(ValueError):
            ReorderBuffer(max_capacity=0)
        with self.assertRaises(ValueError):
            ReorderBuffer(start_record_id=0)


if __name__ == "__main__":
    unittest.main()
