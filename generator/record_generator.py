"""
record_generator.py
===================
Synthetic record generator for the TAR (Time-Aware Reliable Transport) system.

Generates sequential, unique logical records with configurable counts and random seeds.
Each record adheres to the TAR logical record format and exercises critical edge cases:
- Normal variable-length alphanumeric records.
- Empty payloads (0 bytes value).
- Rich Unicode payloads (multi-byte UTF-8, CJK, Arabic, Cyrillic, Emojis, math symbols).
- Large payloads (400 - 800 bytes).
- Boundary-size payloads (approaching the 1,000-byte payload ceiling for 1,200-byte UDP MTU).

All generation is strictly deterministic when initialized with the same random seed.
"""

import argparse
import json
import os
import random
import sys
from typing import Any, Dict, List, Optional

from schemas.record_schema import (
    BOUNDARY_PAYLOAD_MIN_BYTES,
    DEFAULT_MAX_PAYLOAD_BYTES,
    LARGE_PAYLOAD_MAX_BYTES,
    LARGE_PAYLOAD_MIN_BYTES,
    get_payload_bytes,
    validate_record,
)

# Diverse Unicode corpus exercising 1-byte, 2-byte, 3-byte, and 4-byte UTF-8 sequences
UNICODE_SNIPPETS = [
    "TAR transport protocol: 快速, 信頼性, 堅牢性 🚀📦",
    "Multi-lingual test: Здравствуйте мир! مرحبا بالعالم! 你好世界！",
    "Accented characters: Résumé, façade, Überprüfung, mañana, São Paulo",
    "Mathematical symbols: ∀x ∈ ℝ, ∃y : f(x) ≤ ∫[0,∞] e^(-t) dt ⊕ ∑(a_k)",
    "Emoji sequence: 🛰️ 📡 🌐 ⚡ 🛡️ ⏱️ 🎯 🔄 🚨 ✅ ❌",
    "Special delimiters & escaped JSON quotes: {\"key\": \"value with \\\"quotes\\\" and \\n newlines\"}",
]

NORMAL_WORDS = [
    "network", "packet", "datagram", "transmission", "acknowledgment",
    "reordering", "timeout", "latency", "throughput", "buffer",
    "delivery", "session", "transport", "protocol", "recovery",
    "sequence", "monotonic", "sliding", "window", "stream",
]


class RecordGenerator:
    """
    Deterministic logical record generator.
    Produces sequential records with controlled payload category distributions.
    """

    def __init__(
        self,
        seed: int = 42,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    ):
        self.seed = seed
        self.max_payload_bytes = max_payload_bytes
        self.rng = random.Random(seed)

    def _generate_normal_payload(self, record_id: int) -> str:
        """Generates a standard variable-length text sentence (30 - 150 bytes)."""
        num_words = self.rng.randint(4, 15)
        words = self.rng.choices(NORMAL_WORDS, k=num_words)
        return f"Record {record_id}: " + " ".join(words) + "."

    def _generate_empty_payload(self, record_id: int) -> str:
        """Generates an empty string payload (0 bytes)."""
        return ""

    def _generate_unicode_payload(self, record_id: int) -> str:
        """Generates a multi-byte UTF-8 text string containing diverse scripts and symbols."""
        snippet = self.rng.choice(UNICODE_SNIPPETS)
        suffix = f" [ID:{record_id}]"
        return f"{snippet}{suffix}"

    def _generate_large_payload(self, record_id: int) -> str:
        """
        Generates a large payload strictly within the 400 - 800 bytes range
        for serialized logical payload bytes.
        """
        # Sized so both value_bytes and serialized JSON payload stay within 400-800 bytes
        target_bytes = self.rng.randint(LARGE_PAYLOAD_MIN_BYTES, LARGE_PAYLOAD_MAX_BYTES - 40)
        prefix = f"Record {record_id} LARGE_PAYLOAD: "
        padding = "ABCDEF0123456789-" * (target_bytes // 17 + 1)
        full_text = (prefix + padding)[:target_bytes]
        return full_text

    def _generate_boundary_payload(self, record_id: int) -> str:
        """
        Generates a boundary payload calibrated to approach max_payload_bytes
        (e.g., 950 - 1000 bytes) to test maximum MTU threshold boundaries.
        """
        # Ensure payload object JSON size is very close to max_payload_bytes
        target_len = self.max_payload_bytes - 38
        prefix = f"Record {record_id} BOUNDARY_MAX_SIZE: "
        fill = "X" * max(0, target_len - len(prefix))
        return prefix + fill

    def generate_record(self, record_id: int, category: Optional[str] = None) -> Dict[str, Any]:
        """
        Generates a single record with sequential record_id and selected category.
        If category is None, picks category deterministically based on seed distribution.
        """
        if category is None:
            # Deterministic category distribution:
            # First 20 records explicitly cycle through all edge cases so early inspection is rich
            if record_id <= 20:
                cycle = [
                    "normal", "empty", "unicode", "large", "boundary",
                    "normal", "unicode", "normal", "large", "boundary",
                    "empty", "normal", "unicode", "boundary", "large",
                    "normal", "unicode", "normal", "empty", "boundary"
                ]
                category = cycle[(record_id - 1) % len(cycle)]
            else:
                # Weighted distribution for remaining dataset:
                # Normal: 70%, Unicode: 12%, Large: 8%, Boundary: 6%, Empty: 4%
                rand_val = self.rng.random()
                if rand_val < 0.70:
                    category = "normal"
                elif rand_val < 0.82:
                    category = "unicode"
                elif rand_val < 0.90:
                    category = "large"
                elif rand_val < 0.96:
                    category = "boundary"
                else:
                    category = "empty"

        if category == "empty":
            val = self._generate_empty_payload(record_id)
        elif category == "unicode":
            val = self._generate_unicode_payload(record_id)
        elif category == "large":
            val = self._generate_large_payload(record_id)
        elif category == "boundary":
            val = self._generate_boundary_payload(record_id)
        else:
            category = "normal"
            val = self._generate_normal_payload(record_id)

        # The payload format is an inspectable object matching PACKET_FORMAT.md
        payload_obj = {
            "value": val,
            "category": category,
        }

        record = {
            "record_id": record_id,
            "payload": payload_obj,
        }

        # Verify generated record conforms to schema
        is_valid, err = validate_record(record, max_payload_bytes=self.max_payload_bytes)
        if not is_valid:
            raise RuntimeError(f"Generator produced invalid record: {err}")

        return record

    def generate_dataset(
        self,
        record_count: int = 2000,
        output_jsonl_path: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generates exactly record_count sequential records from ID 1 to record_count.
        Optionally saves to a UTF-8 JSONL file.
        """
        records: List[Dict[str, Any]] = []

        if output_jsonl_path:
            os.makedirs(os.path.dirname(os.path.abspath(output_jsonl_path)), exist_ok=True)
            out_file = open(output_jsonl_path, "w", encoding="utf-8")
        else:
            out_file = None

        try:
            for rid in range(1, record_count + 1):
                rec = self.generate_record(rid)
                records.append(rec)
                if out_file:
                    out_file.write(json.dumps(rec, ensure_ascii=False) + "\n")
        finally:
            if out_file:
                out_file.close()

        return records


def generate_dataset(
    record_count: int = 2000,
    seed: int = 42,
    output_path: Optional[str] = None,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> List[Dict[str, Any]]:
    """Helper function to instantiate RecordGenerator and generate dataset in one call."""
    generator = RecordGenerator(seed=seed, max_payload_bytes=max_payload_bytes)
    return generator.generate_dataset(record_count=record_count, output_jsonl_path=output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TAR logical records dataset.")
    parser.add_argument("--count", type=int, default=2000, help="Number of records to generate (default: 2000)")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed (default: 42)")
    parser.add_argument("--output", type=str, default="data/records_2000.jsonl", help="Output JSONL file path")
    parser.add_argument("--report", type=str, default=None, help="Optional output inspection report path")
    args = parser.parse_args()

    print(f"Generating {args.count} records (seed={args.seed}) -> {args.output}")
    records = generate_dataset(record_count=args.count, seed=args.seed, output_path=args.output)
    print(f"Successfully generated {len(records)} records in {args.output}")

    if args.report:
        from generator.inspector import generate_inspection_report
        generate_inspection_report(args.output, args.report)
        print(f"Inspection report saved to {args.report}")
