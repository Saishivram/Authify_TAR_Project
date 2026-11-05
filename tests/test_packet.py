"""
test_packet.py
==============
Automated unit tests for TAR packet framing, serialization, deserialization,
and 1,200-byte wire limit enforcement.
"""

import json
import unittest

from transport.packet import (
    MAX_DATAGRAM_BYTES,
    MAX_SACK_RANGES,
    PROTOCOL_VERSION,
    AckPacket,
    DataPacket,
    ErrorPacket,
    FinAckPacket,
    FinPacket,
    InvalidPacketError,
    OversizedDatagramError,
    ProbePacket,
    ResetPacket,
    StartAckPacket,
    StartPacket,
    deserialize_packet,
    serialize_packet,
)


class TestPacket(unittest.TestCase):
    """Test suite for packet framing, serialization, and MTU enforcement."""

    def test_data_packet_serialization_roundtrip(self):
        """Verify DATA packet serializes and deserializes accurately."""
        payload = {"value": "Test payload string", "category": "normal"}
        pkt = DataPacket(
            connection_id="test-session-1",
            packet_id=101,
            record_id=1,
            attempt=1,
            payload=payload,
        )

        raw_bytes = serialize_packet(pkt)
        self.assertIsInstance(raw_bytes, bytes)
        self.assertLessEqual(len(raw_bytes), MAX_DATAGRAM_BYTES)

        restored = deserialize_packet(raw_bytes)
        self.assertIsInstance(restored, DataPacket)
        self.assertEqual(restored.connection_id, "test-session-1")
        self.assertEqual(restored.packet_id, 101)
        self.assertEqual(restored.record_id, 1)
        self.assertEqual(restored.attempt, 1)
        self.assertEqual(restored.payload, payload)
        self.assertEqual(restored.version, PROTOCOL_VERSION)
        self.assertEqual(restored.type, "DATA")

    def test_start_and_start_ack_roundtrip(self):
        """Verify START and START_ACK packets serialize and deserialize correctly."""
        start_pkt = StartPacket(
            connection_id="session-42",
            expected_records=2000,
            initial_rate=2000,
        )
        raw_start = serialize_packet(start_pkt)
        res_start = deserialize_packet(raw_start)
        self.assertIsInstance(res_start, StartPacket)
        self.assertEqual(res_start.connection_id, "session-42")
        self.assertEqual(res_start.expected_records, 2000)

        ack_pkt = StartAckPacket(
            connection_id="session-42",
            expected_records=2000,
            status="READY",
        )
        raw_ack = serialize_packet(ack_pkt)
        res_ack = deserialize_packet(raw_ack)
        self.assertIsInstance(res_ack, StartAckPacket)
        self.assertEqual(res_ack.connection_id, "session-42")
        self.assertEqual(res_ack.status, "READY")

    def test_fin_and_fin_ack_roundtrip(self):
        """Verify FIN and FIN_ACK packets serialize and deserialize correctly."""
        fin_pkt = FinPacket(
            connection_id="session-42",
            packet_id=2001,
            final_record_id=2000,
            total_packets=2000,
        )
        raw_fin = serialize_packet(fin_pkt)
        res_fin = deserialize_packet(raw_fin)
        self.assertIsInstance(res_fin, FinPacket)
        self.assertEqual(res_fin.packet_id, 2001)
        self.assertEqual(res_fin.final_record_id, 2000)

        fin_ack = FinAckPacket(
            connection_id="session-42",
            final_record_id=2000,
            receiver_delivered_through=2000,
            total_delivered_records=2000,
        )
        raw_fin_ack = serialize_packet(fin_ack)
        res_fin_ack = deserialize_packet(raw_fin_ack)
        self.assertIsInstance(res_fin_ack, FinAckPacket)
        self.assertEqual(res_fin_ack.final_record_id, 2000)
        self.assertEqual(res_fin_ack.receiver_delivered_through, 2000)

    def test_error_packet_roundtrip(self):
        """Verify ERROR packet serialization."""
        err_pkt = ErrorPacket(
            connection_id="session-42",
            code="INCOMPLETE_DELIVERY",
            message="Missing 5 records",
        )
        raw_err = serialize_packet(err_pkt)
        res_err = deserialize_packet(raw_err)
        self.assertIsInstance(res_err, ErrorPacket)
        self.assertEqual(res_err.code, "INCOMPLETE_DELIVERY")
        self.assertIn("Missing 5 records", res_err.message)

    def test_oversized_datagram_serialization_rejected(self):
        """Verify serializer rejects datagrams exceeding 1,200 bytes with OversizedDatagramError."""
        huge_payload = {"value": "X" * 1250, "category": "oversized"}
        pkt = DataPacket(
            connection_id="session-big",
            packet_id=1,
            record_id=1,
            payload=huge_payload,
        )

        with self.assertRaises(OversizedDatagramError) as ctx:
            serialize_packet(pkt, max_bytes=1200)

        self.assertGreater(ctx.exception.actual_bytes, 1200)
        self.assertEqual(ctx.exception.limit, 1200)

    def test_oversized_datagram_deserialization_rejected(self):
        """Verify deserializer rejects raw byte buffers exceeding 1,200 bytes."""
        oversized_bytes = b'{"type":"DATA","version":1,"connection_id":"s"}' + (b"A" * 1200)
        self.assertGreater(len(oversized_bytes), 1200)

        with self.assertRaises(OversizedDatagramError) as ctx:
            deserialize_packet(oversized_bytes, max_bytes=1200)

        self.assertGreater(ctx.exception.actual_bytes, 1200)

    def test_corrupted_json_rejected(self):
        """Verify invalid or truncated JSON raises InvalidPacketError."""
        corrupted = b'{"type": "DATA", "version": 1, "connection_id": incomplete'
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(corrupted)

    def test_invalid_utf8_rejected(self):
        """Verify invalid UTF-8 raw bytes raise InvalidPacketError."""
        bad_utf8 = b'\xff\xfe\x00\x01\x80'
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(bad_utf8)

    def test_unsupported_version_rejected(self):
        """Verify packets with unsupported version raise InvalidPacketError."""
        bad_version_data = json.dumps({
            "type": "START",
            "version": 99,
            "connection_id": "sess-1",
            "expected_records": 100,
        }).encode("utf-8")

        with self.assertRaises(InvalidPacketError) as ctx:
            deserialize_packet(bad_version_data)
        self.assertIn("Unsupported protocol version", str(ctx.exception))

        # Boolean version (e.g. True) must also be rejected
        bool_version_data = json.dumps({
            "type": "START",
            "version": True,
            "connection_id": "sess-1",
            "expected_records": 100,
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError) as ctx:
            deserialize_packet(bool_version_data)
        self.assertIn("Unsupported protocol version", str(ctx.exception))

    def test_missing_required_fields_rejected(self):
        """Verify missing fields in DATA or START raise InvalidPacketError."""
        # Missing record_id in DATA
        missing_record = json.dumps({
            "type": "DATA",
            "version": 1,
            "connection_id": "sess-1",
            "packet_id": 1,
            "payload": "content",
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(missing_record)

        # Missing expected_records in START
        missing_expected = json.dumps({
            "type": "START",
            "version": 1,
            "connection_id": "sess-1",
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(missing_expected)

    def test_unicode_and_empty_payload_datagram_sizes(self):
        """Verify empty and Unicode payloads serialize within wire limits."""
        # Empty payload
        empty_pkt = DataPacket(
            connection_id="sess-empty",
            packet_id=1,
            record_id=1,
            payload={"value": "", "category": "empty"},
        )
        empty_bytes = serialize_packet(empty_pkt)
        self.assertLess(len(empty_bytes), 200)
        self.assertGreater(len(empty_bytes), 100)

        # Multi-lingual Unicode payload
        unicode_pkt = DataPacket(
            connection_id="sess-uni",
            packet_id=2,
            record_id=2,
            payload={"value": "TAR transport: 快速 信頼性 🚀 📦 Здравствуйте! مرحبا", "category": "unicode"},
        )
        unicode_bytes = serialize_packet(unicode_pkt)
        self.assertLess(len(unicode_bytes), 400)
        restored = deserialize_packet(unicode_bytes)
        self.assertIn("快速", restored.payload["value"])
        self.assertIn("🚀", restored.payload["value"])

    def test_ack_packet_serialization_roundtrip(self):
        """Verify ACK packet serializes and deserializes accurately with SACK ranges."""
        ack_pkt = AckPacket(
            connection_id="sess-ack-100",
            largest_packet_id=105,
            ack_received_through_packet_id=49,
            ack_range_floor_packet_id=90,
            ack_ranges=[[105, 105], [103, 102], [100, 100], [95, 91]],
            ack_delay_ms=1.25,
            receiver_delivered_through=5,
        )
        raw_bytes = serialize_packet(ack_pkt)
        self.assertIsInstance(raw_bytes, bytes)
        self.assertLessEqual(len(raw_bytes), MAX_DATAGRAM_BYTES)

        restored = deserialize_packet(raw_bytes)
        self.assertIsInstance(restored, AckPacket)
        self.assertEqual(restored.connection_id, "sess-ack-100")
        self.assertEqual(restored.largest_packet_id, 105)
        self.assertEqual(restored.ack_received_through_packet_id, 49)
        self.assertEqual(restored.ack_range_floor_packet_id, 90)
        self.assertEqual(
            restored.ack_ranges,
            [[105, 105], [103, 102], [100, 100], [95, 91]],
        )
        self.assertAlmostEqual(restored.ack_delay_ms, 1.25, places=2)
        self.assertEqual(restored.receiver_delivered_through, 5)
        self.assertEqual(restored.version, PROTOCOL_VERSION)
        self.assertEqual(restored.type, "ACK")

    def test_ack_empty_ranges(self):
        """Verify ACK packet with empty ack_ranges (pure contiguous reception)."""
        ack_pkt = AckPacket(
            connection_id="sess-ack-empty",
            largest_packet_id=50,
            ack_received_through_packet_id=50,
            ack_range_floor_packet_id=50,
            ack_ranges=[],
            ack_delay_ms=0.0,
            receiver_delivered_through=50,
        )
        raw_bytes = serialize_packet(ack_pkt)
        restored = deserialize_packet(raw_bytes)
        self.assertIsInstance(restored, AckPacket)
        self.assertEqual(restored.ack_ranges, [])
        self.assertEqual(restored.largest_packet_id, 50)
        self.assertEqual(restored.ack_received_through_packet_id, 50)
        self.assertEqual(restored.ack_range_floor_packet_id, 50)

    def test_ack_range_ordering_and_validation(self):
        """Verify ACK range ordering, bounds, non-overlap, and non-adjacency rules."""
        # 1. Inverted range [high, low] where high < low (e.g. [103, 104])
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[103, 104]],
                receiver_delivered_through=5,
            )

        # 2. Ascending order of ranges
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[95, 91], [105, 100]],
                receiver_delivered_through=5,
            )

        # 3. Overlapping ranges
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[105, 100], [102, 95]],
                receiver_delivered_through=5,
            )

        # 4. Adjacent ranges (must be merged into single range)
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[105, 103], [102, 100]],  # 103 == 102 + 1: adjacent!
                receiver_delivered_through=5,
            )

        # 5. Non-integer values in ranges
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[105, "100"]],
                receiver_delivered_through=5,
            )

        # 6. Boolean in ranges (Python bool is subclass of int)
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[True, False]],
                receiver_delivered_through=5,
            )

        # 7. Range not length 2
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[105]],
                receiver_delivered_through=5,
            )

        # 8. Range high exceeding largest_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=100,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=90,
                ack_ranges=[[105, 102]],  # 105 > largest_packet_id (100)
                receiver_delivered_through=5,
            )

    def test_ack_max_32_ranges_and_rejection_over_32(self):
        """Verify maximum 32 SACK ranges accepted and >32 ranges rejected."""
        # Exactly 32 valid non-overlapping descending non-adjacent ranges
        valid_32_ranges = [[200 - i * 3, 200 - i * 3] for i in range(32)]
        # Lowest range will be [200 - 31*3, 200 - 31*3] = [107, 107]
        ack_pkt_32 = AckPacket(
            connection_id="sess-32",
            largest_packet_id=200,
            ack_received_through_packet_id=50,
            ack_range_floor_packet_id=100,  # 100 < 107
            ack_ranges=valid_32_ranges,
            receiver_delivered_through=10,
        )
        self.assertEqual(len(ack_pkt_32.ack_ranges), 32)
        raw_bytes = serialize_packet(ack_pkt_32)
        self.assertLessEqual(len(raw_bytes), MAX_DATAGRAM_BYTES)
        restored = deserialize_packet(raw_bytes)
        self.assertEqual(len(restored.ack_ranges), 32)

        # 33 ranges -> rejected
        invalid_33_ranges = [[250 - i * 3, 250 - i * 3] for i in range(33)]
        with self.assertRaises(InvalidPacketError) as ctx:
            AckPacket(
                connection_id="sess-33",
                largest_packet_id=250,
                ack_received_through_packet_id=50,
                ack_range_floor_packet_id=100,
                ack_ranges=invalid_33_ranges,
                receiver_delivered_through=10,
            )
        self.assertIn("exceeds maximum allowed limit of 32 ranges", str(ctx.exception))

    def test_ack_invalid_watermarks_and_types(self):
        """Verify rejection of invalid watermark values and mismatched constraints."""
        # 1. Negative largest_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", -1, 0, 0, [], 0)

        # 2. Negative ack_received_through_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, -1, 0, [], 0)

        # 3. Negative ack_range_floor_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, -1, [], 0)

        # 4. Negative receiver_delivered_through
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, 0, [], -1)

        # 5. Negative, NaN, or Inf ack_delay_ms
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, 0, [], 0, ack_delay_ms=-0.5)
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, 0, [], 0, ack_delay_ms=float("nan"))
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, 0, [], 0, ack_delay_ms=float("inf"))
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 0, 0, [], 0, ack_delay_ms=-float("inf"))

        # 6. largest_packet_id < ack_received_through_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 10, 20, 20, [], 0)

        # 7. ack_range_floor_packet_id < ack_received_through_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket("s1", 20, 10, 5, [], 0)

        # 8. Lowest SACK range <= ack_range_floor_packet_id
        with self.assertRaises(InvalidPacketError):
            AckPacket(
                connection_id="s1",
                largest_packet_id=105,
                ack_received_through_packet_id=49,
                ack_range_floor_packet_id=95,  # floor is 95, but lowest range is [95, 91] -> 91 <= 95
                ack_ranges=[[105, 105], [95, 91]],
                receiver_delivered_through=5,
            )

        # 9. Deserialization missing required fields
        for missing in ("ack_ranges", "receiver_delivered_through", "largest_packet_id", "ack_range_floor_packet_id"):
            data = {
                "type": "ACK",
                "version": 1,
                "connection_id": "s1",
                "largest_packet_id": 10,
                "ack_received_through_packet_id": 5,
                "ack_range_floor_packet_id": 5,
                "ack_ranges": [],
                "receiver_delivered_through": 5,
            }
            del data[missing]
            with self.assertRaises(InvalidPacketError):
                deserialize_packet(json.dumps(data).encode("utf-8"))

        # 10. Boolean in place of integer watermark
        bad_bool_data = json.dumps({
            "type": "ACK",
            "version": 1,
            "connection_id": "s1",
            "largest_packet_id": True,
            "ack_received_through_packet_id": 5,
            "ack_range_floor_packet_id": 5,
            "ack_ranges": [],
            "receiver_delivered_through": 5,
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(bad_bool_data)

    def test_probe_packet_serialization_roundtrip(self):
        """Verify PROBE packet serializes and deserializes accurately."""
        probe_pkt = ProbePacket(
            connection_id="sess-probe-1",
            packet_id=106,
            record_id=3,
            attempt=2,
            probe_number=1,
            payload={"value": "record-3", "category": "normal"},
        )
        raw_bytes = serialize_packet(probe_pkt)
        self.assertIsInstance(raw_bytes, bytes)
        self.assertLessEqual(len(raw_bytes), MAX_DATAGRAM_BYTES)

        restored = deserialize_packet(raw_bytes)
        self.assertIsInstance(restored, ProbePacket)
        self.assertEqual(restored.connection_id, "sess-probe-1")
        self.assertEqual(restored.packet_id, 106)
        self.assertEqual(restored.record_id, 3)
        self.assertEqual(restored.attempt, 2)
        self.assertEqual(restored.probe_number, 1)
        self.assertEqual(restored.payload, {"value": "record-3", "category": "normal"})
        self.assertEqual(restored.version, PROTOCOL_VERSION)
        self.assertEqual(restored.type, "PROBE")

    def test_probe_packet_validation(self):
        """Verify validation of PROBE fields (positive packet_id/record_id, attempt)."""
        # Non-positive packet_id
        with self.assertRaises(InvalidPacketError):
            ProbePacket("s1", packet_id=0, record_id=1, payload="test")
        with self.assertRaises(InvalidPacketError):
            ProbePacket("s1", packet_id=-5, record_id=1, payload="test")
        with self.assertRaises(InvalidPacketError):
            ProbePacket("s1", packet_id=True, record_id=1, payload="test")

        # Non-positive record_id
        with self.assertRaises(InvalidPacketError):
            ProbePacket("s1", packet_id=1, record_id=0, payload="test")

        # Invalid attempt
        with self.assertRaises(InvalidPacketError):
            ProbePacket("s1", packet_id=1, record_id=1, payload="test", attempt=0)

        # Missing required field in JSON
        missing_payload = json.dumps({
            "type": "PROBE",
            "version": 1,
            "connection_id": "s1",
            "packet_id": 10,
            "record_id": 5,
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(missing_payload)

    def test_reset_packet_serialization_roundtrip(self):
        """Verify RESET packet serializes and deserializes accurately."""
        rst_pkt = ResetPacket(
            connection_id="sess-rst-1",
            reason="operator_requested",
        )
        raw_bytes = serialize_packet(rst_pkt)
        self.assertIsInstance(raw_bytes, bytes)
        self.assertLessEqual(len(raw_bytes), MAX_DATAGRAM_BYTES)

        restored = deserialize_packet(raw_bytes)
        self.assertIsInstance(restored, ResetPacket)
        self.assertEqual(restored.connection_id, "sess-rst-1")
        self.assertEqual(restored.reason, "operator_requested")
        self.assertEqual(restored.version, PROTOCOL_VERSION)
        self.assertEqual(restored.type, "RESET")

    def test_reset_packet_validation(self):
        """Verify RESET packet rejects empty or malformed reason."""
        with self.assertRaises(InvalidPacketError):
            ResetPacket(connection_id="s1", reason="")
        with self.assertRaises(InvalidPacketError):
            ResetPacket(connection_id="s1", reason="   ")
        with self.assertRaises(InvalidPacketError):
            ResetPacket(connection_id="s1", reason=123)

        missing_reason = json.dumps({
            "type": "RESET",
            "version": 1,
            "connection_id": "s1",
        }).encode("utf-8")
        with self.assertRaises(InvalidPacketError):
            deserialize_packet(missing_reason)

    def test_mtu_enforcement_new_packet_types(self):
        """Verify 1,200-byte wire MTU enforcement on new packet types."""
        # Oversized ProbePacket
        huge_probe = ProbePacket(
            connection_id="sess-big-probe",
            packet_id=1,
            record_id=1,
            payload={"data": "P" * 1250},
        )
        with self.assertRaises(OversizedDatagramError):
            serialize_packet(huge_probe, max_bytes=1200)

        # Oversized raw ACK
        huge_ack_bytes = json.dumps({
            "type": "ACK",
            "version": 1,
            "connection_id": "s",
            "largest_packet_id": 10,
            "ack_received_through_packet_id": 5,
            "ack_range_floor_packet_id": 5,
            "ack_ranges": [],
            "receiver_delivered_through": 5,
            "padding": "A" * 1200,
        }).encode("utf-8")
        with self.assertRaises(OversizedDatagramError):
            deserialize_packet(huge_ack_bytes, max_bytes=1200)


if __name__ == "__main__":
    unittest.main()
