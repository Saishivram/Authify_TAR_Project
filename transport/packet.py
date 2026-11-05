"""
transport/packet.py
===================
Packet framing, serialization, deserialization, and datagram size enforcement
for the TAR (Time-Aware Reliable Transport) protocol.

Conforms to PACKET_FORMAT.md:
- Every datagram is an independent UTF-8 JSON object.
- Required envelope headers: `type`, `version`, `connection_id`.
- Hard MTU limit: Complete serialized datagrams must NEVER exceed 1,200 UTF-8 bytes.
- Actual wire length is measured on every transmission and receipt.
"""

from dataclasses import asdict, dataclass
import json
import math
from typing import Any, Dict, Optional, Union


# Protocol wire constraints
PROTOCOL_VERSION = 1
MAX_DATAGRAM_BYTES = 1200
DEFAULT_RECEIVE_BUFFER_SIZE = 65535  # Max datagram bytes application can receive in one recvfrom() call



class PacketError(Exception):
    """Base exception for packet errors."""
    pass


class OversizedDatagramError(PacketError):
    """Raised when a serialized or received datagram exceeds the 1,200-byte wire limit."""

    def __init__(self, actual_bytes: int, limit: int = MAX_DATAGRAM_BYTES):
        super().__init__(
            f"Datagram size ({actual_bytes} bytes) exceeds maximum MTU ceiling of {limit} bytes"
        )
        self.actual_bytes = actual_bytes
        self.limit = limit


class InvalidPacketError(PacketError):
    """Raised when a datagram cannot be parsed or violates schema constraints."""
    pass


@dataclass
class DataPacket:
    connection_id: str
    packet_id: int
    record_id: int
    payload: Any
    attempt: int = 1
    version: int = PROTOCOL_VERSION
    type: str = "DATA"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "packet_id": self.packet_id,
            "record_id": self.record_id,
            "attempt": self.attempt,
            "payload": self.payload,
        }


@dataclass
class StartPacket:
    connection_id: str
    expected_records: int
    initial_rate: int = 2000
    version: int = PROTOCOL_VERSION
    type: str = "START"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "expected_records": self.expected_records,
            "initial_rate": self.initial_rate,
        }


@dataclass
class StartAckPacket:
    connection_id: str
    expected_records: int
    status: str = "READY"
    version: int = PROTOCOL_VERSION
    type: str = "START_ACK"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "status": self.status,
            "expected_records": self.expected_records,
        }


@dataclass
class FinPacket:
    connection_id: str
    packet_id: int
    final_record_id: int
    total_packets: int
    version: int = PROTOCOL_VERSION
    type: str = "FIN"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "packet_id": self.packet_id,
            "final_record_id": self.final_record_id,
            "total_packets": self.total_packets,
        }


@dataclass
class FinAckPacket:
    connection_id: str
    final_record_id: int
    receiver_delivered_through: int
    total_delivered_records: int
    version: int = PROTOCOL_VERSION
    type: str = "FIN_ACK"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "final_record_id": self.final_record_id,
            "receiver_delivered_through": self.receiver_delivered_through,
            "total_delivered_records": self.total_delivered_records,
        }


@dataclass
class ErrorPacket:
    connection_id: str
    code: str
    message: str
    version: int = PROTOCOL_VERSION
    type: str = "ERROR"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "code": self.code,
            "message": self.message,
        }


MAX_SACK_RANGES = 32


def validate_ack_fields(
    connection_id: str,
    largest_packet_id: Any,
    ack_received_through_packet_id: Any,
    ack_range_floor_packet_id: Any,
    ack_ranges: Any,
    ack_delay_ms: Any,
    receiver_delivered_through: Any,
) -> None:
    """Validates all fields and invariants of an AckPacket."""
    if not isinstance(connection_id, str) or not connection_id.strip():
        raise InvalidPacketError("ACK packet missing or invalid 'connection_id'")

    for name, val in [
        ("largest_packet_id", largest_packet_id),
        ("ack_received_through_packet_id", ack_received_through_packet_id),
        ("ack_range_floor_packet_id", ack_range_floor_packet_id),
        ("receiver_delivered_through", receiver_delivered_through),
    ]:
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise InvalidPacketError(f"ACK field '{name}' must be a non-negative integer, got {val!r}")

    if (
        isinstance(ack_delay_ms, bool)
        or not isinstance(ack_delay_ms, (int, float))
        or not math.isfinite(ack_delay_ms)
        or ack_delay_ms < 0
    ):
        raise InvalidPacketError(f"ACK field 'ack_delay_ms' must be a finite non-negative number, got {ack_delay_ms!r}")

    if largest_packet_id < ack_received_through_packet_id:
        raise InvalidPacketError(
            f"largest_packet_id ({largest_packet_id}) cannot be less than "
            f"ack_received_through_packet_id ({ack_received_through_packet_id})"
        )

    if ack_range_floor_packet_id < ack_received_through_packet_id:
        raise InvalidPacketError(
            f"ack_range_floor_packet_id ({ack_range_floor_packet_id}) cannot be less than "
            f"ack_received_through_packet_id ({ack_received_through_packet_id})"
        )

    if not isinstance(ack_ranges, (list, tuple)):
        raise InvalidPacketError(f"ACK field 'ack_ranges' must be a list, got {type(ack_ranges).__name__}")

    if len(ack_ranges) > MAX_SACK_RANGES:
        raise InvalidPacketError(
            f"ack_ranges count ({len(ack_ranges)}) exceeds maximum allowed limit of {MAX_SACK_RANGES} ranges"
        )

    prev_low = None
    for idx, r in enumerate(ack_ranges):
        if not isinstance(r, (list, tuple)) or len(r) != 2:
            raise InvalidPacketError(
                f"Each SACK range must be [high_packet_id, low_packet_id] of length 2, got {r!r} at index {idx}"
            )
        high, low = r
        if isinstance(high, bool) or not isinstance(high, int) or high < 0:
            raise InvalidPacketError(f"SACK range high ({high!r}) at index {idx} must be a non-negative integer")
        if isinstance(low, bool) or not isinstance(low, int) or low < 0:
            raise InvalidPacketError(f"SACK range low ({low!r}) at index {idx} must be a non-negative integer")
        if high < low:
            raise InvalidPacketError(
                f"SACK range invalid: high ({high}) must be >= low ({low}) at index {idx}"
            )
        if high > largest_packet_id:
            raise InvalidPacketError(
                f"SACK range high ({high}) at index {idx} exceeds largest_packet_id ({largest_packet_id})"
            )
        if prev_low is not None:
            if prev_low <= high:
                raise InvalidPacketError(
                    f"SACK ranges must be strictly descending and non-overlapping: "
                    f"previous low ({prev_low}) <= current high ({high}) at index {idx}"
                )
            if prev_low == high + 1:
                raise InvalidPacketError(
                    f"SACK ranges must be non-adjacent (adjacent ranges must be merged): "
                    f"previous low ({prev_low}) is adjacent to current high ({high}) at index {idx}"
                )
        prev_low = low

    if ack_ranges:
        lowest_sack_packet = ack_ranges[-1][1]
        if lowest_sack_packet <= ack_range_floor_packet_id:
            raise InvalidPacketError(
                f"Lowest SACK packet ID ({lowest_sack_packet}) in ack_ranges cannot be less than "
                f"or equal to ack_range_floor_packet_id ({ack_range_floor_packet_id})"
            )


def validate_probe_fields(
    connection_id: str,
    packet_id: Any,
    record_id: Any,
    attempt: Any,
    probe_number: Any,
) -> None:
    """Validates all fields and invariants of a ProbePacket."""
    if not isinstance(connection_id, str) or not connection_id.strip():
        raise InvalidPacketError("PROBE packet missing or invalid 'connection_id'")

    for name, val in [("packet_id", packet_id), ("record_id", record_id)]:
        if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
            raise InvalidPacketError(f"PROBE field '{name}' must be a positive integer, got {val!r}")

    for name, val in [("attempt", attempt), ("probe_number", probe_number)]:
        if isinstance(val, bool) or not isinstance(val, int) or val < 1:
            raise InvalidPacketError(f"PROBE field '{name}' must be an integer >= 1, got {val!r}")


def validate_reset_fields(connection_id: str, reason: Any) -> None:
    """Validates all fields of a ResetPacket."""
    if not isinstance(connection_id, str) or not connection_id.strip():
        raise InvalidPacketError("RESET packet missing or invalid 'connection_id'")
    if not isinstance(reason, str) or not reason.strip():
        raise InvalidPacketError("RESET packet 'reason' must be a non-empty string")


@dataclass
class AckPacket:
    connection_id: str
    largest_packet_id: int
    ack_received_through_packet_id: int
    ack_range_floor_packet_id: int
    ack_ranges: list
    receiver_delivered_through: int
    ack_delay_ms: float = 0.0
    version: int = PROTOCOL_VERSION
    type: str = "ACK"

    def __post_init__(self) -> None:
        validate_ack_fields(
            connection_id=self.connection_id,
            largest_packet_id=self.largest_packet_id,
            ack_received_through_packet_id=self.ack_received_through_packet_id,
            ack_range_floor_packet_id=self.ack_range_floor_packet_id,
            ack_ranges=self.ack_ranges,
            ack_delay_ms=self.ack_delay_ms,
            receiver_delivered_through=self.receiver_delivered_through,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "largest_packet_id": self.largest_packet_id,
            "ack_received_through_packet_id": self.ack_received_through_packet_id,
            "ack_range_floor_packet_id": self.ack_range_floor_packet_id,
            "ack_ranges": [[int(h), int(l)] for h, l in self.ack_ranges],
            "ack_delay_ms": round(float(self.ack_delay_ms), 3),
            "receiver_delivered_through": self.receiver_delivered_through,
        }


@dataclass
class ProbePacket:
    connection_id: str
    packet_id: int
    record_id: int
    payload: Any
    attempt: int = 1
    probe_number: int = 1
    version: int = PROTOCOL_VERSION
    type: str = "PROBE"

    def __post_init__(self) -> None:
        validate_probe_fields(
            connection_id=self.connection_id,
            packet_id=self.packet_id,
            record_id=self.record_id,
            attempt=self.attempt,
            probe_number=self.probe_number,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "packet_id": self.packet_id,
            "record_id": self.record_id,
            "attempt": self.attempt,
            "probe_number": self.probe_number,
            "payload": self.payload,
        }


@dataclass
class ResetPacket:
    connection_id: str
    reason: str
    version: int = PROTOCOL_VERSION
    type: str = "RESET"

    def __post_init__(self) -> None:
        validate_reset_fields(
            connection_id=self.connection_id,
            reason=self.reason,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "version": self.version,
            "connection_id": self.connection_id,
            "reason": self.reason,
        }


Packet = Union[
    DataPacket,
    StartPacket,
    StartAckPacket,
    FinPacket,
    FinAckPacket,
    ErrorPacket,
    AckPacket,
    ProbePacket,
    ResetPacket,
]


def serialize_packet(packet: Packet, max_bytes: int = MAX_DATAGRAM_BYTES) -> bytes:
    """
    Serializes a packet object into UTF-8 JSON bytes and strictly enforces
    that the actual datagram size does not exceed max_bytes (1,200 bytes).

    Raises:
        OversizedDatagramError: If the serialized bytes exceed max_bytes.
        PacketError: If serialization fails.
    """
    try:
        if hasattr(packet, "to_dict"):
            data = packet.to_dict()
        else:
            data = asdict(packet)
        raw_bytes = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, UnicodeEncodeError) as exc:
        raise PacketError(f"Failed to serialize packet: {exc}") from exc

    actual_bytes = len(raw_bytes)
    if actual_bytes > max_bytes:
        raise OversizedDatagramError(actual_bytes=actual_bytes, limit=max_bytes)

    return raw_bytes


def deserialize_packet(raw_bytes: bytes, max_bytes: int = MAX_DATAGRAM_BYTES) -> Packet:
    """
    Parses a raw datagram into a typed Packet object.
    Measures the actual received wire length and rejects datagrams exceeding max_bytes.

    Raises:
        OversizedDatagramError: If raw_bytes length exceeds max_bytes.
        InvalidPacketError: If JSON decoding fails, version is wrong, or fields are missing.
    """
    actual_bytes = len(raw_bytes)
    if actual_bytes > max_bytes:
        raise OversizedDatagramError(actual_bytes=actual_bytes, limit=max_bytes)

    try:
        decoded = raw_bytes.decode("utf-8")
        data = json.loads(decoded)
    except UnicodeDecodeError as exc:
        raise InvalidPacketError(f"Datagram contains invalid UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise InvalidPacketError(f"Datagram contains malformed JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise InvalidPacketError(f"Datagram root must be a JSON object, got {type(data).__name__}")

    # Check required common fields
    pkt_type = data.get("type")
    version = data.get("version")
    connection_id = data.get("connection_id")

    if not pkt_type or not isinstance(pkt_type, str):
        raise InvalidPacketError("Datagram missing or invalid 'type' field")
    if isinstance(version, bool) or version != PROTOCOL_VERSION:
        raise InvalidPacketError(
            f"Unsupported protocol version: {version!r} (expected {PROTOCOL_VERSION})"
        )
    if not connection_id or not isinstance(connection_id, str):
        raise InvalidPacketError("Datagram missing or invalid 'connection_id' field")

    # Map to typed packet class
    try:
        if pkt_type == "DATA":
            for req in ("packet_id", "record_id", "payload"):
                if req not in data:
                    raise InvalidPacketError(f"DATA packet missing required field: '{req}'")
            return DataPacket(
                connection_id=connection_id,
                packet_id=int(data["packet_id"]),
                record_id=int(data["record_id"]),
                payload=data["payload"],
                attempt=int(data.get("attempt", 1)),
                version=version,
            )

        elif pkt_type == "START":
            if "expected_records" not in data:
                raise InvalidPacketError("START packet missing required field: 'expected_records'")
            return StartPacket(
                connection_id=connection_id,
                expected_records=int(data["expected_records"]),
                initial_rate=int(data.get("initial_rate", 2000)),
                version=version,
            )

        elif pkt_type == "START_ACK":
            return StartAckPacket(
                connection_id=connection_id,
                expected_records=int(data.get("expected_records", 0)),
                status=str(data.get("status", "READY")),
                version=version,
            )

        elif pkt_type == "FIN":
            for req in ("packet_id", "final_record_id"):
                if req not in data:
                    raise InvalidPacketError(f"FIN packet missing required field: '{req}'")
            return FinPacket(
                connection_id=connection_id,
                packet_id=int(data["packet_id"]),
                final_record_id=int(data["final_record_id"]),
                total_packets=int(data.get("total_packets", data["final_record_id"])),
                version=version,
            )

        elif pkt_type == "FIN_ACK":
            for req in ("final_record_id", "receiver_delivered_through"):
                if req not in data:
                    raise InvalidPacketError(f"FIN_ACK packet missing required field: '{req}'")
            return FinAckPacket(
                connection_id=connection_id,
                final_record_id=int(data["final_record_id"]),
                receiver_delivered_through=int(data["receiver_delivered_through"]),
                total_delivered_records=int(data.get("total_delivered_records", data["final_record_id"])),
                version=version,
            )

        elif pkt_type == "ERROR":
            return ErrorPacket(
                connection_id=connection_id,
                code=str(data.get("code", "UNKNOWN_ERROR")),
                message=str(data.get("message", "")),
                version=version,
            )

        elif pkt_type == "ACK":
            for req in (
                "largest_packet_id",
                "ack_received_through_packet_id",
                "ack_range_floor_packet_id",
                "ack_ranges",
                "receiver_delivered_through",
            ):
                if req not in data:
                    raise InvalidPacketError(f"ACK packet missing required field: '{req}'")
            return AckPacket(
                connection_id=connection_id,
                largest_packet_id=data["largest_packet_id"],
                ack_received_through_packet_id=data["ack_received_through_packet_id"],
                ack_range_floor_packet_id=data["ack_range_floor_packet_id"],
                ack_ranges=data["ack_ranges"],
                ack_delay_ms=data.get("ack_delay_ms", 0.0),
                receiver_delivered_through=data["receiver_delivered_through"],
                version=version,
            )

        elif pkt_type == "PROBE":
            for req in ("packet_id", "record_id", "payload"):
                if req not in data:
                    raise InvalidPacketError(f"PROBE packet missing required field: '{req}'")
            return ProbePacket(
                connection_id=connection_id,
                packet_id=data["packet_id"],
                record_id=data["record_id"],
                payload=data["payload"],
                attempt=data.get("attempt", 1),
                probe_number=data.get("probe_number", 1),
                version=version,
            )

        elif pkt_type == "RESET":
            if "reason" not in data:
                raise InvalidPacketError("RESET packet missing required field: 'reason'")
            return ResetPacket(
                connection_id=connection_id,
                reason=data["reason"],
                version=version,
            )

        else:
            raise InvalidPacketError(f"Unknown packet type: '{pkt_type}'")

    except (ValueError, TypeError) as exc:
        raise InvalidPacketError(f"Field type error in packet {pkt_type}: {exc}") from exc
