# TAR Packet Format Specification

**Status:** Initial wire-format proposal\
**Transport:** UDP\
**Encoding:** UTF-8 JSON datagrams\
**Protocol:** TAR experimental protocol

## 1. General rules

Every datagram is one UTF-8 JSON object. Each message includes:

-   `type`
-   `version`
-   `connection_id`

The receiver validates the message type, version, connection ID,
required fields, numeric ranges, and maximum datagram size before
processing it.

Use a conservative maximum UDP payload size (initially 1,200 bytes) to
avoid IP fragmentation in typical environments. Reject oversized
messages rather than silently truncating them.

JSON is selected for inspectability and implementation simplicity.
Measure its CPU and bandwidth overhead; binary encoding is not part of
the initial scope.

## 2. DATA

``` json
{
  "type": "DATA",
  "version": 1,
  "connection_id": "session-001",
  "packet_id": 103,
  "record_id": 3,
  "attempt": 1,
  "payload": {
    "value": "record-3"
  }
}
```

  Field             Type            Meaning
  ----------------- --------------- --------------------------------
  `type`            string          `DATA`
  `version`         integer         Protocol version
  `connection_id`   string          Current simulation session
  `packet_id`       integer         Unique transmission ID
  `record_id`       integer         Logical record ID
  `attempt`         integer         Attempt number for this record
  `payload`         object/string   Application record

The sender records its local monotonic send time in its transmission
table. It need not trust a timestamp supplied in a datagram for loss
detection.

## 3. ACK

``` json
{
  "type": "ACK",
  "version": 1,
  "connection_id": "session-001",
  "largest_packet_id": 105,
  "ack_received_through_packet_id": 90,
  "ack_range_floor_packet_id": 91,
  "ack_ranges": [
    [105, 105],
    [104, 103],
    [101, 101],
    [95, 91]
  ],
  "ack_delay_ms": 1.2,
  "receiver_delivered_through": 5
}
```

  ----------------------------------------------------------------------------------
  Field                              Type                    Meaning
  ---------------------------------- ----------------------- -----------------------
  `largest_packet_id`                integer                 Largest packet ID
                                                             observed/received so far

  `ack_received_through_packet_id`   integer                 Highest packet ID of a
                                                             strictly contiguous prefix
                                                             received from packet 1

  `ack_range_floor_packet_id`        integer                 Lower boundary of retained
                                                             discontiguous SACK ranges

  `ack_ranges`                       array                   Descending inclusive
                                                             `[high, low]` ranges of
                                                             received packet IDs (<= 32)

  `ack_delay_ms`                     number                  Monotonic time from packet
                                                             receipt to ACK generation

  `receiver_delivered_through`       integer                 Highest contiguous
                                                             application record delivered
  ----------------------------------------------------------------------------------

Ranges are strictly descending and non-overlapping: each range is `[high_packet_id, low_packet_id]`
where `high_packet_id >= low_packet_id`. For example, `[[105, 105], [104, 103], [101, 101]]`
acknowledges packet IDs 101, 103, 104, and 105; it does not acknowledge 102.

Packet ID interpretation by sender:
- `X <= ack_received_through_packet_id`: Cumulatively acknowledged.
  This watermark represents the highest packet ID of a strictly contiguous prefix received
  from packet 1. It advances only across unbroken sequences and never crosses a gap.
- `ack_range_floor_packet_id <= X <= largest_packet_id`:
  Acknowledged if covered by any `[high, low]` range in `ack_ranges`. If omitted within
  this span, the packet was not received (potential loss subject to RACK loss deadline).
- `ack_received_through_packet_id < X < ack_range_floor_packet_id`:
  Unknown state due to SACK history truncation. The sender MUST NEVER assume omitted
  packets in this gap are acknowledged or definitively lost.
- `X > largest_packet_id`: In flight, not yet observed by receiver.

The receiver bounds memory by retaining at most 32 discontiguous SACK ranges (`MAX_SACK_RANGES = 32`).
When older ranges are evicted, `ack_range_floor_packet_id` advances monotonically.
`receiver_delivered_through` confirms sequential application-level record delivery and is
the sole condition for transitioning a logical record to `DELIVERED`. An ACK matching a `packet_id`
confirms transmission receipt only (`BUFFERED_AT_RECEIVER`).

## 4. PROBE

A probe carries useful data where possible. It is not an empty message
if an outstanding record can be included.

``` json
{
  "type": "PROBE",
  "version": 1,
  "connection_id": "session-001",
  "packet_id": 106,
  "record_id": 3,
  "attempt": 2,
  "probe_number": 1,
  "payload": {
    "value": "record-3"
  }
}
```

A PROBE has a unique packet ID. Its acknowledgement is included in
normal ACK ranges. PTO expiration triggers a probe but does not itself
declare the original packet lost.

## 5. START

``` json
{
  "type": "START",
  "version": 1,
  "connection_id": "session-001",
  "config": {
    "record_rate": 2000,
    "record_count": 10000
  }
}
```

START establishes the run/session context. The control server may also
start processes out of band, but the data-plane session must validate
its connection ID.

## 6. FIN

``` json
{
  "type": "FIN",
  "version": 1,
  "connection_id": "session-001",
  "packet_id": 10050,
  "final_record_id": 10000
}
```

FIN states that no additional logical records will be generated and
identifies the final record expected. Every FIN transmission or
retransmission is assigned a unique `packet_id` to allow RTT tracking
and retransmission management.

## 7. `FIN_ACK`

``` json
{
  "type": "FIN_ACK",
  "version": 1,
  "connection_id": "session-001",
  "final_record_id": 10000,
  "receiver_delivered_through": 10000
}
```

The receiver sends `FIN_ACK` only after all records through
`final_record_id` have been delivered in order. `FIN` / `FIN_ACK` are subject
to loss and must be retried.

## 8. RESET

``` json
{
  "type": "RESET",
  "version": 1,
  "connection_id": "session-001",
  "reason": "operator_requested"
}
```

RESET terminates the current simulation session. It must not be confused
with successful FIN completion.

## 9. ERROR

``` json
{
  "type": "ERROR",
  "version": 1,
  "connection_id": "session-001",
  "code": "INVALID_PACKET",
  "message": "Missing record_id"
}
```

Do not expose stack traces or sensitive local details through protocol
errors.

## 10. Validation rules

Reject or safely ignore:

-   Invalid JSON or invalid UTF-8.
-   Unknown protocol versions.
-   Wrong connection IDs.
-   Unknown message types.
-   Missing required fields.
-   Negative or non-integer packet/record IDs.
-   Invalid ACK ranges, including overlapping or ascending ranges.
-   Oversized datagrams.
-   Invalid or unexpected state transitions.

Malformed packets must not crash the sender, receiver, relay, or control
server.

## 11. Clock rules

-   `sent_at` is maintained locally by the sender using a monotonic
    clock.
-   Receiver timestamps are diagnostic only unless a clock
    synchronization mechanism is explicitly added.
-   Never subtract a receiver monotonic timestamp from a sender
    monotonic timestamp.
-   ACK delay is a duration reported by the receiver, not an absolute
    timestamp.

## 12. Future compatibility

The `version` field allows future format changes. A binary format,
authentication, checksums beyond UDP's transport checksum, and
multi-stream identifiers are out of scope for the initial
implementation.
