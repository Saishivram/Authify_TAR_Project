"""
record_schema.py
================
Defines schema validation rules and constraints for logical records in the
TAR (Time-Aware Reliable Transport) system.

A logical record represents the application-level data unit identified by
`record_id`. When transported over UDP in later phases, each record will be
carried in DATA packets. To guarantee that DATA datagrams do not exceed the
conservative 1,200-byte UDP MTU ceiling (PACKET_FORMAT.md Section 1),
the encoded payload size is constrained by default to 1,000 bytes.
"""

import json
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# Protocol datagram and payload limits (see PACKET_FORMAT.md Sections 1 & 2)
# The complete serialized DATA datagram must never exceed 1,200 UTF-8 bytes on the wire.
# In Phase 1, we enforce a configurable 1,000-byte ceiling on logical payloads
# to comfortably accommodate the ~115-150 byte DATA packet header envelope.
# Note: Verification of the complete 1,200-byte UDP datagram limit will be formally
# tested once the actual DATA packet serializer is built in Phase 2/3.
DEFAULT_MAX_PAYLOAD_BYTES = 1000
MAX_DATA_DATAGRAM_BYTES = 1200
ESTIMATED_DATA_ENVELOPE_OVERHEAD = 115

# Payload classification boundaries
LARGE_PAYLOAD_MIN_BYTES = 400
LARGE_PAYLOAD_MAX_BYTES = 800
BOUNDARY_PAYLOAD_MIN_BYTES = 900


class RecordValidationError(Exception):
    """Raised when a logical record violates schema or protocol constraints."""

    def __init__(self, message: str, record_id: Optional[int] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.record_id = record_id
        self.details = details or {}

    def __str__(self) -> str:
        if self.record_id is not None:
            return f"[Record ID {self.record_id}] {self.message}"
        return self.message


def get_payload_bytes(payload: Union[Dict[str, Any], str, Any]) -> bytes:
    """
    Serializes a logical payload into UTF-8 bytes to determine its exact byte length.
    Raises RecordValidationError if the payload is not JSON-serializable or contains invalid UTF-8.
    """
    try:
        if isinstance(payload, str):
            return payload.encode("utf-8")
        elif isinstance(payload, dict):
            # Compact JSON serialization matching protocol transport
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        else:
            # Fallback serialization for inspectable objects
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (UnicodeEncodeError, TypeError) as exc:
        raise RecordValidationError(f"Payload cannot be encoded to UTF-8 JSON: {exc}") from exc


def get_value_bytes(payload: Union[Dict[str, Any], str, Any]) -> bytes:
    """
    Extracts and returns the raw UTF-8 bytes of the logical record value.
    - If payload is a dict with key 'value', encodes payload['value'].
    - If payload is a string, encodes the string directly.
    - For empty payloads (e.g. value=""), returns b"" (0 bytes).
    """
    try:
        if isinstance(payload, dict):
            val = payload.get("value", "")
            if val is None:
                return b""
            if isinstance(val, str):
                return val.encode("utf-8")
            return str(val).encode("utf-8")
        elif isinstance(payload, str):
            return payload.encode("utf-8")
        return b""
    except UnicodeEncodeError as exc:
        raise RecordValidationError(f"Payload value cannot be encoded to UTF-8: {exc}") from exc


def estimate_data_datagram_bytes(
    payload: Union[Dict[str, Any], str, Any],
    sample_packet_id: int = 999999,
    sample_record_id: int = 999999,
    connection_id: str = "session-001",
) -> int:
    """
    Calculates the projected byte length of a complete serialized DATA packet
    wrapping this logical payload.

    Uses the canonical TAR DATA datagram envelope (PACKET_FORMAT.md Section 2):
    {
        "type": "DATA",
        "version": 1,
        "connection_id": connection_id,
        "packet_id": sample_packet_id,
        "record_id": sample_record_id,
        "attempt": 1,
        "payload": payload
    }

    Note: In Phase 1, this represents an estimated datagram projection.
    Formal testing against the 1,200-byte UDP datagram ceiling will be executed
    once the actual DATA packet serializer is built in Phase 2/3.
    """
    envelope = {
        "type": "DATA",
        "version": 1,
        "connection_id": connection_id,
        "packet_id": sample_packet_id,
        "record_id": sample_record_id,
        "attempt": 1,
        "payload": payload,
    }
    return len(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def validate_record(
    record: Any,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> Tuple[bool, Optional[str]]:
    """
    Validates a single logical record against schema rules.

    Requirements:
    1. Must be a dictionary.
    2. Must contain required field 'record_id' (positive non-zero integer).
    3. Must contain required field 'payload' (dict or str).
    4. Payload must be valid UTF-8 and JSON-serializable.
    5. UTF-8 byte length of payload must not exceed max_payload_bytes.

    Returns:
        (True, None) if valid.
        (False, error_message) if invalid.
    """
    if not isinstance(record, dict):
        return False, f"Record must be a dictionary, got {type(record).__name__}"

    # Required field: record_id
    if "record_id" not in record:
        return False, "Missing required field: 'record_id'"

    record_id = record["record_id"]
    # Check type strictly (bool is a subclass of int in Python, so exclude bool)
    if isinstance(record_id, bool) or not isinstance(record_id, int):
        return False, f"'record_id' must be an integer, got {type(record_id).__name__} ({record_id!r})"

    if record_id <= 0:
        return False, f"'record_id' must be a positive integer >= 1, got {record_id}"

    # Required field: payload
    if "payload" not in record:
        return False, f"Missing required field: 'payload' (record_id={record_id})"

    payload = record["payload"]
    if not isinstance(payload, (dict, str)):
        return False, (
            f"'payload' must be an object (dict) or string, got {type(payload).__name__} "
            f"(record_id={record_id})"
        )

    # Validate UTF-8 encoding and byte size
    try:
        payload_bytes = get_payload_bytes(payload)
    except RecordValidationError as err:
        return False, f"UTF-8 serialization failed: {err.message} (record_id={record_id})"

    if len(payload_bytes) > max_payload_bytes:
        return False, (
            f"Payload size ({len(payload_bytes)} bytes) exceeds configured maximum limit of "
            f"{max_payload_bytes} bytes (record_id={record_id})"
        )

    return True, None


def validate_record_stream(
    records: Iterable[Any],
    expected_start_id: int = 1,
    expected_count: Optional[int] = None,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> Dict[str, Any]:
    """
    Validates a full stream or sequence of records.

    Verifies:
    - Every individual record complies with validate_record.
    - Record IDs are strictly positive, unique, and strictly sequential starting from expected_start_id.
    - Detects duplicate record IDs.
    - Detects missing / skipped record IDs.
    - If expected_count is provided, verifies total delivered count.

    Returns:
        Summary dict containing validation status and diagnostic details.
    """
    total_records = 0
    errors: List[str] = []
    seen_ids: set = set()
    duplicate_ids: List[int] = []
    missing_ids: List[int] = []
    expected_id = expected_start_id

    for idx, record in enumerate(records, start=1):
        if isinstance(record, dict) and "_malformed_error" in record:
            errors.append(record["_malformed_error"])
            total_records += 1
            continue

        total_records += 1
        is_valid, err_msg = validate_record(record, max_payload_bytes=max_payload_bytes)
        if not is_valid:
            errors.append(f"Record index {idx}: {err_msg}")
            continue

        rid = record["record_id"]

        # Check duplicates
        if rid in seen_ids:
            duplicate_ids.append(rid)
            errors.append(f"Duplicate record_id encountered at index {idx}: {rid}")
        else:
            seen_ids.add(rid)

        # Check sequential order
        if rid != expected_id:
            if rid > expected_id:
                # Discovered gaps
                gaps = list(range(expected_id, rid))
                missing_ids.extend(gaps)
                errors.append(f"Sequence gap at index {idx}: expected record_id {expected_id}, got {rid} (missing: {gaps})")
            elif rid < expected_id and rid not in duplicate_ids:
                errors.append(f"Out-of-order record_id at index {idx}: expected {expected_id}, got {rid}")
            expected_id = rid + 1
        else:
            expected_id += 1

    # Check count expectation if configured
    if expected_count is not None and total_records != expected_count:
        errors.append(f"Total record count mismatch: expected {expected_count}, got {total_records}")

    is_success = len(errors) == 0

    return {
        "valid": is_success,
        "total_records": total_records,
        "errors": errors,
        "duplicate_ids": sorted(list(set(duplicate_ids))),
        "missing_ids": sorted(list(set(missing_ids))),
        "expected_start_id": expected_start_id,
        "last_delivered_id": expected_id - 1 if total_records > 0 else None,
    }


def validate_jsonl_file(
    file_path: str,
    expected_count: Optional[int] = None,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> Dict[str, Any]:
    """
    Reads a JSONL file line-by-line and runs validate_record_stream.
    Handles malformed JSON lines, decoding errors, and empty files gracefully.
    """
    def record_generator():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        yield json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        yield {"_malformed_error": f"Line {line_no}: Malformed JSON syntax - {exc}"}
        except UnicodeDecodeError as exc:
            yield {"_malformed_error": f"File encoding error: Invalid UTF-8 bytes encountered - {exc}"}
        except FileNotFoundError as exc:
            yield {"_malformed_error": f"File not found: {exc}"}

    return validate_record_stream(
        record_generator(),
        expected_count=expected_count,
        max_payload_bytes=max_payload_bytes,
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m schemas.record_schema <path_to_records.jsonl> [expected_count]")
        sys.exit(1)

    target_file = sys.argv[1]
    count = int(sys.argv[2]) if len(sys.argv) > 2 else None

    print(f"Validating records from: {target_file}")
    result = validate_jsonl_file(target_file, expected_count=count)

    if result["valid"]:
        print(f"SUCCESS: All {result['total_records']} records are valid, sequential, and unique!")
        sys.exit(0)
    else:
        print(f"FAILURE: Found {len(result['errors'])} validation errors in {result['total_records']} records.")
        for err in result["errors"][:10]:
            print(f"  - {err}")
        if len(result["errors"]) > 10:
            print(f"  ... and {len(result['errors']) - 10} more errors.")
        sys.exit(1)
