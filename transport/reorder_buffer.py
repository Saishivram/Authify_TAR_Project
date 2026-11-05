"""
transport/reorder_buffer.py
===========================
Receiver-side reorder buffer for in-order record delivery and gap buffering.

Features:
- Maintains strict monotonically increasing record delivery (1, 2, 3...).
- Bounded capacity (default 5,000 records) preventing memory exhaustion.
- In-Order Pass-Through Invariant: In-order records (record_id == next_expected_record)
  are ALWAYS accepted even when buffer is full, immediately delivering and flushing
  consecutive buffered records.
- Drops out-of-order records when buffer is full without updating delivery state.
- Suppresses duplicates cleanly (both already-delivered and currently-buffered).
"""

from typing import Dict, List, Optional, Tuple
from transport.packet import DataPacket


class ReorderBuffer:
    """
    Manages receiver-side record ordering and delivery.

    Invariants:
    1. receiver_delivered_through == next_expected_record - 1
    2. buffer contains only record_ids > next_expected_record
    3. len(buffer) <= max_capacity (except during instantaneous in-order flush)
    4. In-order records bypass capacity limits to drain the buffer
    """

    def __init__(self, max_capacity: int = 5000, start_record_id: int = 1):
        if max_capacity < 1:
            raise ValueError("max_capacity must be at least 1")
        if start_record_id < 1:
            raise ValueError("start_record_id must be a positive integer")

        self.max_capacity = max_capacity
        self.next_expected_record = start_record_id
        self.receiver_delivered_through = start_record_id - 1
        self.buffer: Dict[int, DataPacket] = {}

    @property
    def buffered_count(self) -> int:
        """Returns the number of out-of-order records currently buffered."""
        return len(self.buffer)

    @property
    def is_empty(self) -> bool:
        """Returns True if no records are waiting in the reorder buffer."""
        return len(self.buffer) == 0

    def process_data_packet(self, packet: DataPacket) -> Tuple[List[DataPacket], str]:
        """
        Processes an incoming DataPacket.

        Returns:
            (delivered_packets, status) where status is one of:
            - 'DELIVERED': In-order packet arrived (and possibly flushed buffered records).
            - 'BUFFERED': Out-of-order packet buffered waiting for preceding gaps.
            - 'DUPLICATE_DELIVERED': Already delivered (record_id < next_expected_record).
            - 'DUPLICATE_BUFFERED': Already buffered in the gap queue.
            - 'FULL_DROPPED': Out-of-order packet dropped because buffer is at max capacity.
        """
        record_id = packet.record_id

        # 1. Duplicate check: Already delivered
        if record_id < self.next_expected_record:
            return ([], "DUPLICATE_DELIVERED")

        # 2. Duplicate check: Already buffered
        if record_id in self.buffer:
            return ([], "DUPLICATE_BUFFERED")

        # 3. In-order arrival: Always accepted even if buffer is at or above capacity!
        if record_id == self.next_expected_record:
            delivered: List[DataPacket] = [packet]
            self.next_expected_record += 1
            self.receiver_delivered_through = record_id

            # Flush any contiguous records waiting in buffer
            while self.next_expected_record in self.buffer:
                next_pkt = self.buffer.pop(self.next_expected_record)
                delivered.append(next_pkt)
                self.receiver_delivered_through = next_pkt.record_id
                self.next_expected_record += 1

            return (delivered, "DELIVERED")

        # 4. Out-of-order arrival: check capacity
        if len(self.buffer) >= self.max_capacity:
            return ([], "FULL_DROPPED")

        self.buffer[record_id] = packet
        return ([], "BUFFERED")
