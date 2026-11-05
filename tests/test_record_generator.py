"""
test_record_generator.py
========================
Automated unit tests for TAR Record Generator and Inspector, verifying
dataset determinism, edge-case generation, 400-800B large payload constraints,
three-metric size accounting, and 100-record individual inspection reports.
"""

import os
import tempfile
import unittest

from generator.inspector import inspect_dataset, is_unicode_payload
from generator.record_generator import RecordGenerator, generate_dataset
from schemas.record_schema import (
    LARGE_PAYLOAD_MAX_BYTES,
    LARGE_PAYLOAD_MIN_BYTES,
    get_payload_bytes,
    get_value_bytes,
    validate_record_stream,
)


class TestRecordGenerator(unittest.TestCase):
    """Test suite for dataset generation, determinism, and inspection."""

    def test_default_count_is_2000(self):
        """Verify default generation produces exactly 2,000 records."""
        generator = RecordGenerator(seed=100)
        records = generator.generate_dataset(record_count=2000)
        self.assertEqual(len(records), 2000)
        self.assertEqual(records[0]["record_id"], 1)
        self.assertEqual(records[-1]["record_id"], 2000)

    def test_deterministic_generation_same_seed(self):
        """Verify identical seed produces strictly identical records and payloads."""
        gen1 = RecordGenerator(seed=12345)
        recs1 = gen1.generate_dataset(record_count=100)

        gen2 = RecordGenerator(seed=12345)
        recs2 = gen2.generate_dataset(record_count=100)

        self.assertEqual(recs1, recs2, "Generators with identical seed must produce identical output")

    def test_different_seeds_produce_different_data(self):
        """Verify different seeds produce differing payloads."""
        gen1 = RecordGenerator(seed=111)
        recs1 = gen1.generate_dataset(record_count=50)

        gen2 = RecordGenerator(seed=999)
        recs2 = gen2.generate_dataset(record_count=50)

        # Record IDs match (1..50), but payloads should differ
        self.assertNotEqual(
            [r["payload"]["value"] for r in recs1],
            [r["payload"]["value"] for r in recs2],
        )

    def test_identical_jsonl_output_same_seed_and_parameters(self):
        """Verify identical seed and count produces identical byte-for-byte JSONL files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_a = os.path.join(tmp_dir, "dataset_a.jsonl")
            file_b = os.path.join(tmp_dir, "dataset_b.jsonl")
            file_diff = os.path.join(tmp_dir, "dataset_diff.jsonl")

            # Generate two runs with seed=42
            generate_dataset(record_count=100, seed=42, output_path=file_a)
            generate_dataset(record_count=100, seed=42, output_path=file_b)

            # Generate one run with different seed=999
            generate_dataset(record_count=100, seed=999, output_path=file_diff)

            with open(file_a, "rb") as fa, open(file_b, "rb") as fb, open(file_diff, "rb") as fd:
                bytes_a = fa.read()
                bytes_b = fb.read()
                bytes_diff = fd.read()

            self.assertEqual(bytes_a, bytes_b, "Files produced with same seed must be binary identical")
            self.assertNotEqual(bytes_a, bytes_diff, "Files produced with different seeds must differ")

    def test_sequential_unique_ids_across_all_records(self):
        """Verify all generated records have sequential, non-duplicated IDs."""
        generator = RecordGenerator(seed=42)
        records = generator.generate_dataset(record_count=500)

        stream_res = validate_record_stream(records, expected_start_id=1, expected_count=500)
        self.assertTrue(stream_res["valid"], f"Stream validation failed: {stream_res['errors']}")
        self.assertEqual(len(stream_res["duplicate_ids"]), 0)
        self.assertEqual(len(stream_res["missing_ids"]), 0)

    def test_all_edge_case_categories_present(self):
        """Verify generator produces normal, empty, unicode, large, and boundary payloads."""
        generator = RecordGenerator(seed=42)
        records = generator.generate_dataset(record_count=200)

        categories = set(r["payload"]["category"] for r in records)
        expected_categories = {"normal", "empty", "unicode", "large", "boundary"}

        self.assertTrue(
            expected_categories.issubset(categories),
            f"Expected all categories {expected_categories}, got {categories}",
        )

        # Verify unicode detection finds actual multi-byte characters
        unicode_records = [r for r in records if r["payload"]["category"] == "unicode"]
        self.assertGreater(len(unicode_records), 0)
        self.assertTrue(any(is_unicode_payload(r["payload"]) for r in unicode_records))

        # Verify empty payloads have empty string values and 0 value bytes
        empty_records = [r for r in records if r["payload"]["category"] == "empty"]
        self.assertGreater(len(empty_records), 0)
        self.assertTrue(all(r["payload"]["value"] == "" for r in empty_records))
        self.assertTrue(all(len(get_value_bytes(r["payload"])) == 0 for r in empty_records))
        self.assertTrue(all(len(get_payload_bytes(r["payload"])) == 31 for r in empty_records))

        # Verify boundary payloads approach 900-1000 bytes
        boundary_records = [r for r in records if r["payload"]["category"] == "boundary"]
        self.assertGreater(len(boundary_records), 0)
        for br in boundary_records:
            raw_bytes = get_payload_bytes(br["payload"])
            self.assertGreaterEqual(len(raw_bytes), 900)
            self.assertLessEqual(len(raw_bytes), 1000)

    def test_large_payload_range_strictly_400_to_800_bytes(self):
        """Verify all large-category records fall strictly within the 400 - 800 bytes range."""
        generator = RecordGenerator(seed=42)
        records = generator.generate_dataset(record_count=500)

        large_records = [r for r in records if r["payload"]["category"] == "large"]
        self.assertGreater(len(large_records), 20)

        for rec in large_records:
            ser_len = len(get_payload_bytes(rec["payload"]))
            val_len = len(get_value_bytes(rec["payload"]))
            self.assertGreaterEqual(
                ser_len,
                LARGE_PAYLOAD_MIN_BYTES,
                f"Large record serialized bytes below {LARGE_PAYLOAD_MIN_BYTES}: {ser_len}",
            )
            self.assertLessEqual(
                ser_len,
                LARGE_PAYLOAD_MAX_BYTES,
                f"Large record serialized bytes above {LARGE_PAYLOAD_MAX_BYTES}: {ser_len}",
            )
            self.assertGreaterEqual(val_len, LARGE_PAYLOAD_MIN_BYTES - 40)
            self.assertLessEqual(val_len, LARGE_PAYLOAD_MAX_BYTES)

    def test_inspection_report_three_metrics_and_explanation(self):
        """Verify inspector accurately calculates all three byte metrics and empty explanation."""
        generator = RecordGenerator(seed=42)
        records = generator.generate_dataset(record_count=150)

        report = inspect_dataset(records)
        summary = report["dataset_summary"]
        metrics = report["payload_size_metrics"]
        first100 = report["first_100_records_inspection"]

        self.assertEqual(summary["total_records"], 150)
        self.assertTrue(summary["sequential_unique_ids_confirmed"])
        self.assertEqual(summary["validation_errors_count"], 0)

        # Metric 1: value_bytes
        self.assertIn("value_bytes", metrics)
        self.assertEqual(metrics["value_bytes"]["minimum_bytes"], 0)  # Empty records have 0 value bytes

        # Metric 2: serialized_logical_payload_bytes
        self.assertIn("serialized_logical_payload_bytes", metrics)
        self.assertGreaterEqual(metrics["serialized_logical_payload_bytes"]["minimum_bytes"], 31)

        # Metric 3: estimated_complete_data_datagram_bytes
        self.assertIn("estimated_complete_data_datagram_bytes", metrics)
        self.assertGreater(metrics["estimated_complete_data_datagram_bytes"]["minimum_bytes"], 100)
        self.assertLess(metrics["estimated_complete_data_datagram_bytes"]["maximum_bytes"], 1200)

        # Empty payload explanation presence
        self.assertIn("empty_payload_explanation", report)
        self.assertIn("31 bytes", report["empty_payload_explanation"])

        # First 100 individual records verification
        self.assertEqual(first100["records_inspected_count"], 100)
        self.assertTrue(first100["all_first_100_valid"])
        self.assertEqual(len(first100["samples"]), 100)

    def test_first_100_inspection_evidence_file_generation(self):
        """Verify inspector writes the dedicated 100-record individual inspection file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            jsonl_file = os.path.join(tmp_dir, "sample.jsonl")
            report_file = os.path.join(tmp_dir, "report.json")
            evidence_file = os.path.join(tmp_dir, "first_100_inspection.md")

            generate_dataset(record_count=100, seed=42, output_path=jsonl_file)

            from generator.inspector import generate_inspection_report
            generate_inspection_report(jsonl_file, report_file, evidence_file)

            self.assertTrue(os.path.exists(report_file))
            self.assertTrue(os.path.exists(evidence_file))

            # Verify evidence file contains all 100 IDs
            with open(evidence_file, "r", encoding="utf-8") as f:
                content = f.read()

            self.assertIn("TAR First 100 Records Individual Inspection Evidence", content)
            self.assertIn("Exactly 100 records", content)
            # Verify rows 1 and 100 are present in table
            self.assertIn("| 1 |", content)
            self.assertIn("| 100 |", content)


if __name__ == "__main__":
    unittest.main()
