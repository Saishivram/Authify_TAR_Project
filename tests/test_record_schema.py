"""
test_record_schema.py
=====================
Automated unit tests for TAR logical record schema validation, byte limits,
encoding integrity, stream ordering, and error reporting.
"""

import os
import tempfile
import unittest

from schemas.record_schema import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    MAX_DATA_DATAGRAM_BYTES,
    RecordValidationError,
    estimate_data_datagram_bytes,
    get_payload_bytes,
    get_value_bytes,
    validate_jsonl_file,
    validate_record,
    validate_record_stream,
)


class TestRecordSchema(unittest.TestCase):
    """Test suite for individual record validation and stream rules."""

    def test_normal_record_valid(self):
        """Verify normal standard records pass validation."""
        rec = {
            "record_id": 1,
            "payload": {"value": "Hello world from TAR protocol", "category": "normal"},
        }
        is_valid, err = validate_record(rec)
        self.assertTrue(is_valid, f"Expected valid record, got error: {err}")
        self.assertIsNone(err)

    def test_string_payload_valid(self):
        """Verify record with string payload passes validation."""
        rec = {"record_id": 2, "payload": "Simple string payload"}
        is_valid, err = validate_record(rec)
        self.assertTrue(is_valid)

    def test_empty_payload(self):
        """Verify empty payloads are accepted as valid according to specifications."""
        rec_empty_str = {"record_id": 3, "payload": ""}
        rec_empty_dict = {"record_id": 4, "payload": {"value": "", "category": "empty"}}

        is_valid_str, _ = validate_record(rec_empty_str)
        is_valid_dict, _ = validate_record(rec_empty_dict)

        self.assertTrue(is_valid_str)
        self.assertTrue(is_valid_dict)

        # value_bytes must be 0 for empty value
        self.assertEqual(len(get_value_bytes(rec_empty_str["payload"])), 0)
        self.assertEqual(len(get_value_bytes(rec_empty_dict["payload"])), 0)

        # serialized_payload_bytes occupies 31 bytes for the JSON envelope {"value":"","category":"empty"}
        ser_empty_dict = get_payload_bytes(rec_empty_dict["payload"])
        self.assertEqual(len(ser_empty_dict), 31)

    def test_unicode_payload(self):
        """Verify multi-byte UTF-8 Unicode characters (accents, Asian scripts, Emojis) validate properly."""
        unicode_text = "TAR Reliable Transport: 快速 🚀 堅牢性 📦 مرحبا بالعالم! Überprüfung ∑(x) ≥ 0"
        rec = {
            "record_id": 5,
            "payload": {"value": unicode_text, "category": "unicode"},
        }
        is_valid, err = validate_record(rec)
        self.assertTrue(is_valid, f"Failed Unicode validation: {err}")

        # Check UTF-8 byte serialization preserves characters
        raw_bytes = get_payload_bytes(rec["payload"])
        self.assertIn("快速".encode("utf-8"), raw_bytes)
        self.assertIn("🚀".encode("utf-8"), raw_bytes)

    def test_large_payload_within_limit(self):
        """Verify large payload within configured limit (e.g. 800 bytes) is valid."""
        large_text = "A" * 700
        rec = {"record_id": 6, "payload": {"value": large_text, "category": "large"}}
        is_valid, err = validate_record(rec, max_payload_bytes=1000)
        self.assertTrue(is_valid, f"Large valid payload was rejected: {err}")

    def test_boundary_size_payload(self):
        """Verify boundary payloads close to the 1,000-byte threshold pass if <= limit."""
        target_size = 980
        padding = "X" * target_size
        rec = {"record_id": 7, "payload": {"value": padding}}
        is_valid, err = validate_record(rec, max_payload_bytes=1000)
        self.assertTrue(is_valid, f"Boundary payload should pass: {err}")

    def test_oversized_payload_rejected(self):
        """Verify oversized payloads exceeding max_payload_bytes are rejected."""
        oversized_text = "Z" * 1050
        rec = {"record_id": 8, "payload": {"value": oversized_text}}
        is_valid, err = validate_record(rec, max_payload_bytes=1000)
        self.assertFalse(is_valid)
        self.assertIn("exceeds configured maximum limit", err)

    def test_missing_required_fields(self):
        """Verify missing record_id or payload is rejected with clear error."""
        # Missing record_id
        is_valid, err = validate_record({"payload": "data"})
        self.assertFalse(is_valid)
        self.assertIn("Missing required field: 'record_id'", err)

        # Missing payload
        is_valid, err = validate_record({"record_id": 9})
        self.assertFalse(is_valid)
        self.assertIn("Missing required field: 'payload'", err)

    def test_invalid_data_types(self):
        """Verify invalid data types (negative ID, zero, boolean, string ID) are rejected."""
        # Non-dict record
        is_valid, err = validate_record("not a dict")
        self.assertFalse(is_valid)
        self.assertIn("Record must be a dictionary", err)

        # Zero record_id
        is_valid, err = validate_record({"record_id": 0, "payload": "test"})
        self.assertFalse(is_valid)
        self.assertIn("positive integer >= 1", err)

        # Negative record_id
        is_valid, err = validate_record({"record_id": -5, "payload": "test"})
        self.assertFalse(is_valid)
        self.assertIn("positive integer >= 1", err)

        # Float record_id
        is_valid, err = validate_record({"record_id": 1.5, "payload": "test"})
        self.assertFalse(is_valid)
        self.assertIn("must be an integer", err)

        # Boolean record_id (in Python bool is int subclass, must be rejected)
        is_valid, err = validate_record({"record_id": True, "payload": "test"})
        self.assertFalse(is_valid)
        self.assertIn("must be an integer", err)

        # Invalid payload type (e.g. integer or list not wrapped in object)
        is_valid, err = validate_record({"record_id": 10, "payload": 12345})
        self.assertFalse(is_valid)
        self.assertIn("'payload' must be an object (dict) or string", err)

    def test_sequential_id_stream_validation(self):
        """Verify stream validator confirms strictly sequential IDs."""
        valid_stream = [
            {"record_id": 1, "payload": "one"},
            {"record_id": 2, "payload": "two"},
            {"record_id": 3, "payload": "three"},
            {"record_id": 4, "payload": "four"},
        ]
        res = validate_record_stream(valid_stream, expected_start_id=1, expected_count=4)
        self.assertTrue(res["valid"])
        self.assertEqual(res["total_records"], 4)
        self.assertEqual(len(res["errors"]), 0)

    def test_gap_in_stream_detected(self):
        """Verify skipped record IDs are detected and reported."""
        stream_with_gap = [
            {"record_id": 1, "payload": "one"},
            {"record_id": 2, "payload": "two"},
            {"record_id": 5, "payload": "five"},  # Gaps: 3, 4
        ]
        res = validate_record_stream(stream_with_gap, expected_start_id=1)
        self.assertFalse(res["valid"])
        self.assertEqual(res["missing_ids"], [3, 4])
        self.assertTrue(any("Sequence gap" in e for e in res["errors"]))

    def test_duplicate_record_ids_detected(self):
        """Verify duplicate record IDs in stream are flagged."""
        stream_with_dup = [
            {"record_id": 1, "payload": "one"},
            {"record_id": 2, "payload": "two"},
            {"record_id": 2, "payload": "duplicate two"},
            {"record_id": 3, "payload": "three"},
        ]
        res = validate_record_stream(stream_with_dup, expected_start_id=1)
        self.assertFalse(res["valid"])
        self.assertIn(2, res["duplicate_ids"])
        self.assertTrue(any("Duplicate record_id" in e for e in res["errors"]))

    def test_payload_exactly_at_configured_maximum(self):
        """Verify payload whose serialized byte length is exactly at the limit passes."""
        # Test with custom limit of 500 bytes
        exact_500_str = "K" * 500
        self.assertEqual(len(get_payload_bytes(exact_500_str)), 500)
        is_valid_500, err_500 = validate_record(
            {"record_id": 1, "payload": exact_500_str},
            max_payload_bytes=500,
        )
        self.assertTrue(is_valid_500, f"Expected 500-byte payload to pass: {err_500}")
        self.assertIsNone(err_500)

        # Test with DEFAULT_MAX_PAYLOAD_BYTES (1,000 bytes)
        exact_1000_str = "M" * DEFAULT_MAX_PAYLOAD_BYTES
        self.assertEqual(len(get_payload_bytes(exact_1000_str)), 1000)
        is_valid_1000, err_1000 = validate_record(
            {"record_id": 2, "payload": exact_1000_str},
            max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES,
        )
        self.assertTrue(is_valid_1000, f"Expected 1,000-byte payload to pass: {err_1000}")
        self.assertIsNone(err_1000)

    def test_payload_one_byte_above_configured_maximum(self):
        """Verify payload whose serialized byte length is exactly 1 byte above limit is rejected."""
        # 501 bytes for 500 byte limit
        over_501_str = "K" * 501
        self.assertEqual(len(get_payload_bytes(over_501_str)), 501)
        is_valid_501, err_501 = validate_record(
            {"record_id": 1, "payload": over_501_str},
            max_payload_bytes=500,
        )
        self.assertFalse(is_valid_501)
        self.assertIn("exceeds configured maximum limit of 500 bytes", err_501)
        self.assertIn("501 bytes", err_501)

        # 1,001 bytes for DEFAULT_MAX_PAYLOAD_BYTES (1,000 bytes) limit
        over_1001_str = "M" * (DEFAULT_MAX_PAYLOAD_BYTES + 1)
        self.assertEqual(len(get_payload_bytes(over_1001_str)), 1001)
        is_valid_1001, err_1001 = validate_record(
            {"record_id": 2, "payload": over_1001_str},
            max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES,
        )
        self.assertFalse(is_valid_1001)
        self.assertIn("exceeds configured maximum limit of 1000 bytes", err_1001)
        self.assertIn("1001 bytes", err_1001)

    def test_invalid_utf8_input(self):
        """Verify invalid UTF-8 inputs (such as unpaired surrogates or broken byte files) are rejected safely."""
        # Unpaired surrogate code point cannot be encoded to UTF-8
        surrogate_char = "\ud800"
        with self.assertRaises(RecordValidationError):
            get_payload_bytes(surrogate_char)

        with self.assertRaises(RecordValidationError):
            get_value_bytes({"value": surrogate_char})

        is_valid, err = validate_record({"record_id": 1, "payload": surrogate_char})
        self.assertFalse(is_valid)
        self.assertIn("UTF-8 serialization failed", err)

        # Test non-UTF-8 binary data in JSONL file
        with tempfile.TemporaryDirectory() as tmp_dir:
            bad_file = os.path.join(tmp_dir, "bad_utf8.jsonl")
            with open(bad_file, "wb") as f:
                f.write(b'{"record_id": 1, "payload": "valid"}\n')
                f.write(b'\xff\xfe\xfd\x80\n')  # Invalid UTF-8 bytes
            res = validate_jsonl_file(bad_file)
            self.assertFalse(res["valid"])
            self.assertTrue(any("Invalid UTF-8 bytes" in e for e in res["errors"]))

    def test_malformed_jsonl_lines(self):
        """Verify malformed JSON lines in a JSONL file are reported with line numbers and syntax details."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            bad_file = os.path.join(tmp_dir, "malformed.jsonl")
            with open(bad_file, "w", encoding="utf-8") as f:
                f.write('{"record_id": 1, "payload": {"value": "first"}}\n')
                f.write('{"record_id": 2, "payload": INCOMPLETE_JSON\n')
                f.write('{"record_id": 3, "payload": {"value": "third"}}\n')

            res = validate_jsonl_file(bad_file)
            self.assertFalse(res["valid"])
            self.assertTrue(
                any("Line 2" in e and "Malformed JSON syntax" in e for e in res["errors"]),
                f"Expected line 2 malformed syntax error, got: {res['errors']}",
            )

    def test_empty_jsonl_file(self):
        """Verify empty JSONL files are handled cleanly without exceptions."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            empty_file = os.path.join(tmp_dir, "empty.jsonl")
            open(empty_file, "w", encoding="utf-8").close()

            # Empty file without expected count passes as 0 records
            res = validate_jsonl_file(empty_file)
            self.assertTrue(res["valid"])
            self.assertEqual(res["total_records"], 0)
            self.assertEqual(len(res["errors"]), 0)

            # Empty file with expected count fails count validation
            res_counted = validate_jsonl_file(empty_file, expected_count=100)
            self.assertFalse(res_counted["valid"])
            self.assertEqual(res_counted["total_records"], 0)
            self.assertTrue(any("count mismatch" in e.lower() for e in res_counted["errors"]))

    def test_incorrect_record_ordering(self):
        """Verify scrambled, reversed, or non-sequential record sequences are flagged as out-of-order."""
        # 1. Scrambled sequence [1, 3, 2]
        scrambled = [
            {"record_id": 1, "payload": "one"},
            {"record_id": 3, "payload": "three"},
            {"record_id": 2, "payload": "two"},
        ]
        res1 = validate_record_stream(scrambled)
        self.assertFalse(res1["valid"])
        self.assertTrue(any("Sequence gap" in e or "Out-of-order" in e for e in res1["errors"]))

        # 2. Descending sequence [3, 2, 1]
        descending = [
            {"record_id": 3, "payload": "three"},
            {"record_id": 2, "payload": "two"},
            {"record_id": 1, "payload": "one"},
        ]
        res2 = validate_record_stream(descending)
        self.assertFalse(res2["valid"])
        self.assertTrue(any("Sequence gap" in e or "Out-of-order" in e for e in res2["errors"]))

        # 3. Inverted pair [2, 1] starting with ID 2 instead of 1
        inverted = [
            {"record_id": 2, "payload": "two"},
            {"record_id": 1, "payload": "one"},
        ]
        res3 = validate_record_stream(inverted, expected_start_id=1)
        self.assertFalse(res3["valid"])
        self.assertTrue(any("Sequence gap" in e or "Out-of-order" in e for e in res3["errors"]))

    def test_datagram_size_estimation_and_limits(self):
        """
        Verify estimated DATA datagram sizing and document MTU bounds.
        Note: Verification of the live 1,200-byte datagram limit will be added in Phase 2
        once the actual packet serializer is implemented.
        """
        # Normal payload
        payload_normal = {"value": "Record 1: test packet", "category": "normal"}
        est_normal = estimate_data_datagram_bytes(payload_normal, sample_packet_id=1, sample_record_id=1)
        self.assertLess(est_normal, 200)

        # Empty payload
        payload_empty = {"value": "", "category": "empty"}
        est_empty = estimate_data_datagram_bytes(payload_empty, sample_packet_id=1, sample_record_id=1)
        self.assertLess(est_empty, 150)
        self.assertGreater(est_empty, 100)

        # Boundary payload (approaching 1,000 bytes)
        payload_boundary = {"value": "X" * 960, "category": "boundary"}
        est_boundary = estimate_data_datagram_bytes(payload_boundary, sample_packet_id=999999, sample_record_id=999999)
        self.assertLess(est_boundary, MAX_DATA_DATAGRAM_BYTES)
        self.assertGreater(est_boundary, 1050)


if __name__ == "__main__":
    unittest.main()
