"""
transport/sack.py
=================
SACK (Selective Acknowledgement) range tracking and retirement engine for TAR.

Maintains:
- `largest_packet_id`: Highest transmission packet ID observed.
- `ack_received_through_packet_id`: Highest strictly contiguous received packet ID from 1.
- `ack_ranges`: Non-overlapping, descending [high, low] ranges for discontiguous arrivals.
- `ack_range_floor_packet_id`: Lower boundary of retained discontiguous SACK history.
- Maximum 32 ranges: Truncates oldest ranges to bound memory to O(1) state.
- UNKNOWN packet classification: Packets below the floor that are not contiguous are
  designated UNKNOWN (never assumed received, never claimed lost by truncation alone).
"""

from typing import List, Optional, Tuple

from transport.packet import (
    MAX_SACK_RANGES,
    AckPacket,
    InvalidPacketError,
)


class SackTracker:
    """
    Tracks received transmission packet IDs and generates standardized wire SACK ranges.

    Invariants maintained:
    1. ack_received_through_packet_id <= ack_range_floor_packet_id <= largest_packet_id.
    2. ack_ranges is strictly descending, non-overlapping, non-adjacent:
       for all i: low_i > high_{i+1} + 1.
    3. len(ack_ranges) <= max_ranges (default 32).
    4. If ack_ranges is non-empty: ack_ranges[-1][1] > ack_range_floor_packet_id.
    5. O(1) memory bound: Only interval endpoints [high, low] are stored.
    """

    def __init__(self, max_ranges: int = MAX_SACK_RANGES):
        if max_ranges < 1:
            raise ValueError("max_ranges must be at least 1")
        self.max_ranges = max_ranges
        self.largest_packet_id: int = 0
        self.ack_received_through_packet_id: int = 0
        self.ack_range_floor_packet_id: int = 0
        # List of [high, low] intervals sorted in strictly descending order
        self.ranges: List[List[int]] = []

    def record_received_packet(self, packet_id: int) -> bool:
        """
        Records receipt of a packet ID.

        Returns:
            True if the packet was newly recorded.
            False if it was already received / duplicate.
        """
        if isinstance(packet_id, bool) or not isinstance(packet_id, int) or packet_id <= 0:
            raise ValueError(f"packet_id must be a positive integer, got {packet_id!r}")

        # 1. Check if already cumulatively acknowledged
        if packet_id <= self.ack_received_through_packet_id:
            return False

        # 2. Check if contiguous extension of cumulative watermark
        if packet_id == self.ack_received_through_packet_id + 1:
            self.ack_received_through_packet_id += 1
            if packet_id > self.largest_packet_id:
                self.largest_packet_id = packet_id

            # Absorb any consecutive discontiguous intervals that now connect
            while self.ranges:
                lowest_high, lowest_low = self.ranges[-1]
                if lowest_low <= self.ack_received_through_packet_id + 1:
                    self.ack_received_through_packet_id = max(
                        self.ack_received_through_packet_id, lowest_high
                    )
                    self.ranges.pop()
                else:
                    break

            if self.ack_received_through_packet_id > self.largest_packet_id:
                self.largest_packet_id = self.ack_received_through_packet_id

            self._update_floor()
            return True

        # 3. Discontiguous arrival (packet_id > ack_received_through_packet_id + 1)
        if packet_id > self.largest_packet_id:
            self.largest_packet_id = packet_id

        # Insert or merge into descending interval list
        inserted = self._insert_discontiguous_packet(packet_id)
        if inserted:
            self._enforce_max_ranges()
            self._update_floor()
            return True
        return False

    def _insert_discontiguous_packet(self, packet_id: int) -> bool:
        """Inserts packet_id into self.ranges, merging with adjacent/overlapping ranges."""
        # Fast path: empty ranges
        if not self.ranges:
            self.ranges.append([packet_id, packet_id])
            return True

        # Search position in descending ranges: ranges are [high, low] with high >= low
        for idx, (high, low) in enumerate(self.ranges):
            # Already contained
            if low <= packet_id <= high:
                return False

            # Extends high upward (packet_id == high + 1)
            if packet_id == high + 1:
                self.ranges[idx][0] = packet_id
                # Check if it merges with previous range (idx - 1)
                if idx > 0 and self.ranges[idx - 1][1] == packet_id + 1:
                    # Merge current into previous
                    self.ranges[idx - 1][1] = self.ranges[idx][1]
                    self.ranges.pop(idx)
                return True

            # Extends low downward (packet_id == low - 1)
            if packet_id == low - 1:
                self.ranges[idx][1] = packet_id
                # Check if it merges with next range (idx + 1)
                if idx < len(self.ranges) - 1 and packet_id - 1 == self.ranges[idx + 1][0]:
                    # Merge next into current
                    self.ranges[idx][1] = self.ranges[idx + 1][1]
                    self.ranges.pop(idx + 1)
                return True

            # Belongs before this range in descending order
            if packet_id > high:
                self.ranges.insert(idx, [packet_id, packet_id])
                return True

        # Belongs after the last range in descending order
        self.ranges.append([packet_id, packet_id])
        return True

    def _enforce_max_ranges(self) -> None:
        """Capped at max_ranges: drops the oldest (lowest) intervals and advances floor."""
        if len(self.ranges) > self.max_ranges:
            # Retain only the newest (highest) max_ranges
            self.ranges = self.ranges[:self.max_ranges]
            # Advance floor to lowest retained range low - 1
            lowest_retained_low = self.ranges[-1][1]
            self.ack_range_floor_packet_id = max(
                self.ack_range_floor_packet_id, lowest_retained_low - 1
            )

    def _update_floor(self) -> None:
        """Ensures floor is at least contiguous watermark, or equals watermark if ranges empty."""
        if not self.ranges:
            self.ack_range_floor_packet_id = self.ack_received_through_packet_id
        else:
            self.ack_range_floor_packet_id = max(
                self.ack_range_floor_packet_id, self.ack_received_through_packet_id
            )

    def get_sack_ranges(self) -> List[List[int]]:
        """Returns a copy of the current descending [high, low] SACK ranges."""
        return [[h, l] for h, l in self.ranges]

    def create_ack_packet(
        self,
        connection_id: str,
        receiver_delivered_through: int,
        ack_delay_ms: float = 0.0,
    ) -> AckPacket:
        """Constructs a validated AckPacket reflecting the current tracker state."""
        return AckPacket(
            connection_id=connection_id,
            largest_packet_id=self.largest_packet_id,
            ack_received_through_packet_id=self.ack_received_through_packet_id,
            ack_range_floor_packet_id=self.ack_range_floor_packet_id,
            ack_ranges=self.get_sack_ranges(),
            ack_delay_ms=ack_delay_ms,
            receiver_delivered_through=receiver_delivered_through,
        )

    def classify_packet(self, packet_id: int) -> str:
        """
        Classifies packet_id from receiver tracker state:
        - 'CUMULATIVE_ACK': <= ack_received_through_packet_id
        - 'SACK_ACK': in active ack_ranges
        - 'UNKNOWN': ack_received_through_packet_id < packet_id <= ack_range_floor_packet_id
        - 'UNACKNOWLEDGED': ack_range_floor_packet_id < packet_id <= largest_packet_id
        - 'UNSEEN': > largest_packet_id
        """
        if packet_id <= self.ack_received_through_packet_id:
            return "CUMULATIVE_ACK"
        if packet_id > self.largest_packet_id:
            return "UNSEEN"
        if packet_id <= self.ack_range_floor_packet_id:
            return "UNKNOWN"

        for high, low in self.ranges:
            if low <= packet_id <= high:
                return "SACK_ACK"

        return "UNACKNOWLEDGED"
