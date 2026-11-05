# TAR --- Time-Aware Reliable Transport

## Algorithm Specification

**Status:** Design proposal for the take-home assignment\
**Implementation language:** Python\
**Transport:** UDP datagrams\
**Protocol type:** Custom, single-stream, reliable, ordered transport\
**Design inspiration:** RACK-TLP (RFC 8985) and QUIC loss detection (RFC
9002)

> TAR is an experimental protocol designed for this assignment. It
> borrows selected mechanisms from RACK-TLP and QUIC; it is not a
> complete implementation of either standard.

## 1. Goals and requirements

The system must:

-   Send approximately 2,000 logical records per second.
-   Operate over a network simulator configured for 5% packet loss, 2%
    reordering, and occasional 400 ms stalls.
-   Deliver every successfully accepted logical record exactly once to
    the application in-session and in increasing record-ID order (state
    is maintained in-memory for the session; crash recovery across process
    restarts is outside the initial scope).
-   Recover missing records through retransmission.
-   Never silently declare completion while records remain missing.
-   Expose connection, transmission, recovery, and delivery state to a
    browser dashboard.
-   Start as an integrated system using one documented command.

The one-second delivery-stall requirement is an empirical acceptance
criterion measured under specified test conditions (including 400 ms
stalls, reverse-path ACK delays, retransmission loss, multiple gaps,
and combined impairments). It is not a theoretical guarantee for arbitrary
network outages. Tests must measure and report the actual maximum
application delivery gaps.

## 2. Core design separation

TAR separates three concepts:

1.  **Logical record:** The application data that must be delivered,
    identified by `record_id`.
2.  **Transmission:** One attempt to carry a logical record, identified
    by a globally increasing `packet_id` within a connection.
3.  **Delivery progress:** The receiver's `next_expected_record`, which
    determines which logical record may be delivered next.

A retransmission keeps the same `record_id` but receives a new
`packet_id`. For example, packet 103 may carry record 3, and a later
retransmission may be packet 110 carrying record 3.

This distinction allows the receiver to acknowledge each transmission
independently while delivering the logical record only once.

## 3. Process architecture

The initial implementation uses four processes:

-   **Sender:** Generates/transmits DATA, tracks transmissions,
    processes ACKs, detects loss, and schedules recovery.
-   **Network relay:** Receives UDP datagrams from either endpoint and
    independently applies loss, delay, reordering, and stall impairment
    in each direction before forwarding. Forwards raw datagram bytes
    without premature JSON deserialization unless inspecting for
    targeted tests.
    -   *Stall and Reorder Interaction:* When a stall is active in a
        direction, arriving packets are placed into that direction's
        `stall_queue`. To ensure the combined impairment mode actually
        produces packet reordering after a stall, per-packet reordering
        decisions and delay offsets ($\Delta t_{\text{reo}}$) are determined
        upon arrival. When the stall clears at $T_{\text{end}}$, non-reordered
        packets are dispatched immediately, while reordered packets are
        dispatched at $T_{\text{end}} + \Delta t_{\text{reo}}$. This guarantees
        that reordering occurs even among packets held during a stall.
-   **Receiver:** Tracks received packet IDs, buffers logical records,
    delivers consecutive records, and sends ACKs.
-   **Control/API server:** Starts and stops the simulation processes,
    exposes configuration and metrics, and streams events to the
    dashboard.

The sender and receiver communicate through actual UDP sockets. The
relay is in the data path; the API server is not a substitute for
transport communication.

Use explicit, static UDP port mappings across four sockets on loopback:
- Sender socket: `127.0.0.1:9001`
- Relay Port A (Sender-facing): `127.0.0.1:9002`
- Relay Port B (Receiver-facing): `127.0.0.1:9003`
- Receiver socket: `127.0.0.1:9004`

The relay forwards datagrams received on Port A to the receiver (`9004`),
and forwards datagrams received on Port B to the sender (`9001`). ACK
traffic passes through the same impairment mechanism in the reverse
direction. Do not rely on dynamic address discovery from initial packets.

## 4. Packet identity and record identity

-   `record_id`: Positive, monotonically increasing logical record
    number.
-   `packet_id`: Positive, monotonically increasing transmission number,
    unique for every DATA or PROBE transmission that carries a record.
-   `attempt`: Per-record attempt counter, starting at 1.
-   A retransmission always receives a fresh `packet_id`.
-   Packet IDs are never reused during a connection.
-   A connection/session identifier prevents stale datagrams from a
    previous run being accepted in a new run.

Do not use packet arrival order as a substitute for record order.

## 5. Sender state

Maintain the following state:

  -----------------------------------------------------------------------
  State                               Meaning
  ----------------------------------- -----------------------------------
  `next_record_id`                    Next logical record ID to generate

  `next_packet_id`                    Next unique transmission ID

  `outstanding_packets`               Transmission records not yet
                                      acknowledged or retired

  `record_status`                     Logical record state: queued,
                                      outstanding, delivered/confirmed,
                                      failed

  `record_attempts`                   Number of transmission attempts per
                                      logical record

  `latest_delivered_tx_time`          Send timestamp of the most recently
                                      transmitted packet newly
                                      acknowledged

  `latest_delivered_ack_time`         Local monotonic time when that ACK
                                      was processed

  `min_rtt`                           Minimum valid RTT sample

  `latest_rtt`                        Most recent valid RTT sample

  `smoothed_rtt`                      Smoothed RTT estimate

  `rtt_variance`                      RTT variation estimate

  `reordering_window`                 Current RACK-inspired reordering
                                      tolerance

  `reo_wnd_multiplier`                Adaptive multiplier for the base
                                      reordering window

  `reo_wnd_persist`                   Recovery-round persistence counter

  `last_ack_progress_time`            Local time of most recent newly
                                      acknowledged transmission

  `probe_count`                       Consecutive probe count

  `retransmission_count`              Total retransmission attempts

  `spurious_retransmission_count`     Retransmissions later shown to have
                                      been unnecessary

  `send_window`                       Maximum number of in-flight logical
                                      records/transmissions permitted by
                                      the implementation
  -----------------------------------------------------------------------

Each transmission record should contain at least:

``` python
{
    "packet_id": 103,
    "record_id": 3,
    "sent_at": 1250.45,       # local monotonic clock
    "attempt": 1,
    "acknowledged": False,
    "lost": False,
    "is_probe": False,
    "retransmission_of": None
}
```

Keep transmission history long enough to recognize a late ACK for an
original transmission after its retransmission has arrived. Retire old
history only after a documented retention policy.

## 6. Receiver state

Maintain:

-   `next_expected_record`: Next logical record eligible for application
    delivery.
-   `reorder_buffer`: Mapping from record ID to payload.
-   `received_packet_ids`: Recent packet IDs received in this
    connection.
-   `delivered_record_ids` or an equivalent bounded delivery
    watermark/history.
-   `ack_history`: Packet IDs/ranges to include in ACKs.
-   `total_received_packets`, `total_delivered_records`, and buffer
    occupancy.

For each DATA packet:

1.  Validate connection ID, type, required fields, and payload size.
2.  Record the packet ID as received, even if its logical record is
    already present.
3.  If the record ID is at or above `next_expected_record` and is not
    already buffered, store its payload.
4.  While `next_expected_record` exists in the reorder buffer, remove
    it, deliver it to the application, and increment
    `next_expected_record`.
5.  Schedule an ACK that includes the received packet ID.

A duplicate transmission must still be acknowledged. A duplicate logical
record must not be delivered twice.

## 7. ACK policy and ACK ranges

ACKs identify **packet IDs**, not record IDs.

The receiver sends:

-   An immediate ACK when an incoming packet reveals a new gap in the
    received packet-ID sequence.
-   An immediate ACK for a PROBE.
-   Otherwise, a delayed ACK after 10 newly received packets or 5 ms,
    whichever occurs first.

Under persistent packet gaps, to prevent flood while preserving feedback,
subsequent packets arriving while existing gaps remain unresolved follow
the standard threshold (10 packets or 5 ms). The implementation measures
and reports ACK frequency (`acks_sent_per_second` and ACK-to-DATA ratio)
under all impairment conditions.

The ACK includes a descending list of inclusive packet-ID ranges, for
example:

``` json
{
  "type": "ACK",
  "version": 1,
  "connection_id": "session-001",
  "largest_packet_id": 105,
  "ack_received_through_packet_id": 90,
  "ack_range_floor_packet_id": 91,
  "ack_ranges": [[105, 105], [104, 103], [101, 101], [95, 91]],
  "ack_delay_ms": 1.2,
  "receiver_delivered_through": 5
}
```

Interpretation of packet IDs by the sender:
1.  **$X \le \text{ack\_received\_through\_packet\_id}$:** Considered
    cumulatively acknowledged. Represents the highest packet ID of a strictly
    contiguous sequence of received packet IDs from packet 1 up to that value.
2.  **$\text{ack\_range\_floor\_packet\_id} \le X \le \text{largest\_packet\_id}$:**
    -   If $X$ falls within one of the ranges in `ack_ranges`: **Acknowledged**.
    -   If $X$ is omitted between `ack_range_floor_packet_id` and
        `largest_packet_id`: **Unacknowledged** (potential gap, evaluated
        by RACK loss detection once later packets are delivered).
3.  **$\text{ack\_received\_through\_packet\_id} < X < \text{ack\_range\_floor\_packet\_id}$:**
    Unknown state due to SACK history truncation. The sender MUST NEVER assume omitted
    packets in this gap are acknowledged or definitively lost.
4.  **$X > \text{largest\_packet\_id}$:** Still in flight; not yet observed
    by the receiver.

Safe ACK history truncation and retirement policy:
- `packet_id` and `record_id` occupy distinct numbering spaces. Do not
  prune ACK ranges of packet IDs based on `receiver_delivered_through`
  (which is a record ID watermark).
- **Watermark Advancement Rule:** The watermark `ack_received_through_packet_id`
  represents the highest packet ID of a **strictly contiguous prefix of
  received packet IDs from packet 1 up to that value**. It advances *only*
  when every single packet ID from 1 through $K$ has been received. **It must
  never advance across a missing packet ID.** If a gap exists (e.g. packet 50
  is missing while 51--60 have arrived), `ack_received_through_packet_id`
  remains strictly at 49.
- **SACK Range Floor:** When discontinuous ranges exceed 32 blocks (`MAX_SACK_RANGES = 32`),
  older ranges are truncated and `ack_range_floor_packet_id` advances monotonically to the
  lower bound of the lowest retained range.
- **Duplicate and Late Packet Handling After Retirement:**
  - Because `ack_received_through_packet_id` never crosses a missing packet,
    any packet arriving with $\text{packet\_id} \le \text{ack\_received\_through\_packet\_id}$
    is guaranteed to be a duplicate transmission rather than a late unreceived
    packet. The receiver acknowledges it implicitly and discards duplicate payload.
  - A late packet arriving above `ack_received_through_packet_id` is accepted,
    buffered/delivered, and if it connects the contiguous prefix, allows
    `ack_received_through_packet_id` to advance forward.
- **Sender Transmission History Retention:** The sender maintains outstanding
  and declared-lost transmission records in a retention table for an explicit
  safety horizon:
  $$\text{retention\_time} = \max(2 \times SRTT + \text{reordering\_window}, 1.0\text{ s})$$
  following a loss declaration, or until session completion. If an ACK
  subsequently arrives acknowledging an original `packet_id` before its
  retention expiry, the sender detects that the packet was delayed rather
  than lost, registers a spurious retransmission, and adapts its reordering
  multiplier. Only after `retention_time` expires is the transmission record
  purged from sender memory. If an ACK refers to an already purged packet ID,
  it is safely and idempotently ignored.

## 8. RTT estimation

Use a monotonic clock for elapsed-time measurements. Never compare
monotonic timestamps from different processes or machines.

The sender can calculate an RTT sample for a newly acknowledged
transmission when the matching packet ID identifies the exact
transmission attempt. This avoids retransmission ambiguity because each
attempt has a different packet ID.

For a valid RTT sample `R`, use the RFC 6298-style estimator:

-   First sample: `SRTT = R`, `RTTVAR = R / 2`.
-   Later samples:
    -   `RTTVAR = 0.75 * RTTVAR + 0.25 * abs(old_SRTT - R)`
    -   `SRTT = 0.875 * old_SRTT + 0.125 * R`

Maintain `min_rtt` as the minimum valid sample. Document how receiver
ACK delay is accounted for. The initial implementation may subtract a
bounded, reported ACK delay from a sample, but must not allow a negative
or implausible sample.

## 9. RACK-inspired time-based loss detection

This is an intentionally simplified RACK-inspired detector, not a
verbatim implementation of RFC 8985.

### Mandatory Precondition: Later-ACK Evidence

A packet must **never** be declared lost merely because its own timer
or timeout has expired. Time-based loss detection in TAR strictly
requires causal evidence that a later transmission traversed the network
and reached the receiver. Without confirmed delivery of a later-transmitted
packet, RACK loss detection cannot fire. (Loss of tail packets where no
subsequent transmission is delivered is handled exclusively by Tail-Loss
Probes and Fallback RTO, never by RACK).

### Simplified TAR Loss Deadline

When an incoming ACK confirms reception of new transmissions:

1.  **Selection of Later Acknowledged Transmission:** Inspect all newly
    acknowledged packet IDs in the incoming ACK. Select the transmission
    with the latest send timestamp among them and record its local send time
    as `latest_delivered_tx_time`. Record `latest_delivered_ack_time` as
    the local monotonic arrival time of the ACK.
2.  **Mandatory Causal Evidence:** An outstanding transmission `P` is
    evaluated for loss if and only if it was sent prior to that delivered
    transmission:
    $$P.\text{sent\_at} < \text{latest\_delivered\_tx\_time}$$
3.  **Simplified Loss Deadline:** The loss deadline for `P` incorporates
    the outstanding packet's own send time, the path round-trip time, and
    the adaptive reordering tolerance:
    $$\text{loss\_deadline}(P) = P.\text{sent\_at} + \text{RTT} + \text{reordering\_window}$$
    where $\text{RTT} = \max(\text{latest\_rtt}, \text{min\_rtt})$ (or
    $SRTT$ if `latest_rtt` is not yet available).
4.  **Loss Declaration Rule:** Transmission `P` is declared lost when:
    $$P.\text{sent\_at} < \text{latest\_delivered\_tx\_time} \quad \text{AND} \quad \text{now} \ge \text{loss\_deadline}(P)$$
5.  If eligible, mark that transmission attempt lost (`lost = True`) and
    submit its logical record to the centralized recovery scheduler, unless
    the logical record is already confirmed delivered.

### Retransmission Ambiguity and Non-Conformance

-   **Retransmission Ambiguity Resolution:** Because every transmission
    (initial data, retransmission, or probe) receives a globally unique,
    strictly monotonic `packet_id`, each acknowledged `packet_id` precisely
    identifies the exact send attempt and its `sent_at` timestamp. This yields
    an unambiguous RTT sample and an exact per-attempt loss deadline without
    timestamp guessing.
-   **Distinction from full RFC 8985:** Full RFC 8985 implements complex
    D-SACK feedback handling, dynamic window inflation from sequence
    offsets, per-segment TCP control block state flags, and step-by-step
    timer re-arming heuristics. TAR uses this simplified time-domain
    deadline formulation; it does not claim full RFC 8985 compliance.

Use an initial base reordering window of `min_rtt / 4` once a valid minimum
RTT exists. Before an RTT sample exists, use a conservative configured
initial value and label it as a bootstrap assumption.

The implementation arms an event timer for the earliest pending loss
deadline rather than busy-polling.

## 10. Adaptive reordering tolerance

Use a RACK-inspired adaptive window:

-   Base window (time): `base_window = min_rtt / 4`.
-   Initial multiplier (dimensionless): `reo_wnd_multiplier = 1`.
-   Configured time cap: `reorder_cap = SRTT` once available; otherwise
    a documented bootstrap time cap.
-   Current reordering window (time):
    `reordering_window = min(reo_wnd_multiplier * base_window, reorder_cap)`.

The multiplier `reo_wnd_multiplier` is strictly dimensionless (integer:
1, 2, 3, ...), while the base window, cap, and resulting reordering
window have dimensions of time.

A spurious retransmission is established when the original transmission
`packet_id` is acknowledged after the same logical record was
retransmitted because the original was declared lost. Preserve the
original packet-ID history across attempts to establish this; seeing a
duplicate record alone is not sufficient evidence.

On qualifying spurious-retransmission evidence:

1.  Increment the dimensionless adaptive multiplier `reo_wnd_multiplier`
    by 1.
2.  Recalculate the time-based `reordering_window`, capped at `reorder_cap`.
3.  Record the event and its packet/record IDs.
4.  Reset the persistence counter.

### Recovery Flights and Round Completion Rule

-   **Recovery Flight Definition:** When loss recovery begins, the sender
    defines the `recovery_flight` as the exact set of outstanding,
    unacknowledged transmission attempts sent up to that moment:
    $$\text{recovery\_flight} = \{ P \mid P.\text{packet\_id} \le \text{recovery\_end\_packet} \text{ and not acknowledged at start} \}$$
    where $\text{recovery\_end\_packet}$ is the highest `packet_id` sent
    when loss was declared.
-   **Defined Terminal States:** An attempt in the recovery flight reaches
    a terminal state when it is:
    1.  Acknowledged (`acknowledged == True`), OR
    2.  Declared lost by RACK or RTO (`lost == True`), OR
    3.  Declared permanently failed by retry exhaustion (`failed == True`).
-   **Interaction with Retransmissions:** When an attempt $P \in \text{recovery\_flight}$
    is declared lost, its record is scheduled for retransmission as a new
    transmission $Q$ with a fresh $\text{packet\_id} > \text{recovery\_end\_packet}$.
    The original attempt $P$ has reached its terminal state (`lost = True`),
    fulfilling the recovery flight accounting for that transmission. The new
    attempt $Q$ is not part of the current recovery flight; it belongs to
    subsequent transmission history.
-   **Unambiguous Completion Rule:** A recovery round does **not** complete
    merely because a newer packet or retransmission is acknowledged. The
    recovery round completes **if and only if every transmission attempt
    belonging to the original `recovery_flight` has reached a defined
    terminal state** (each is either acknowledged, declared lost, or failed).
-   **Multiplier Decay:** When all attempts in the recovery flight have
    reached terminal status: if zero spurious retransmissions occurred
    during the flight's resolution, increment the 16-round decay counter.
    After 16 consecutive rounds without spurious retransmissions, decrement
    or reset `reo_wnd_multiplier` toward baseline (1). If any attempt in
    the flight was later recognized as a spurious retransmission, reset
    the counter to 0.

## 11. Tail-loss probing (TLP/PTO-inspired)

RACK-style detection needs later acknowledged transmission evidence. The
final outstanding transmission may have no later packet to provide that
evidence.

Use a QUIC PTO-inspired probe timer:

``` text
PTO = SRTT + max(4 * RTTVAR, timer_granularity) + max_ack_delay
```

Initial values:

-   Timer granularity: 1 ms.
-   Maximum ACK delay: 5 ms, matching the initial ACK policy.
-   If no RTT sample exists, use a conservative configured initial PTO.

When PTO expires:

1.  Send a probe carrying the oldest outstanding logical record where
    possible.
2.  Assign a new packet ID.
3.  Do not automatically mark the original transmission lost merely
    because PTO expired.
4.  Wait for ACK evidence.
5.  Back off the probe timeout exponentially after consecutive
    expirations, with a configured cap.

A probe is a request for fresh feedback, not proof of packet loss.

## 12. Fallback retransmission timeout

If ACK progress stops entirely, use a separate bounded fallback RTO so
the sender cannot wait forever when no later ACK arrives.

-   **Configurable Experimental Defaults:** Minimum floor of 500 ms,
    maximum ceiling of 4.0 s, and a retry limit of 10 attempts. These are
    provisional defaults to be validated by benchmark and recovery tests,
    not a theoretical guarantee of meeting the 1-second stall requirement.
-   **Role vs. Primary Recovery:** Sub-second recovery is expected to be
    achieved via the primary **RACK** and **PTO** mechanisms, which operate
    within 1--2 RTTs ($\approx 10\text{--}50\text{ ms}$). Fallback RTO
    serves strictly as a safety net against complete channel halts or
    tail deadlocks. The 500 ms floor prevents spurious RTO triggers during
    simulated 400 ms link stalls.
-   On expiry:
    -   Select the oldest outstanding logical record.
    -   Retransmit it with a fresh `packet_id`.
    -   Record the reason as `RTO_FALLBACK`.
    -   Increment the attempt counter and back off the timeout exponentially.
    -   Stop after the retry limit and report a terminal failure.
-   Never silently drop a record or report successful completion after retry
    exhaustion.
-   **Centralized Recovery Coordinator (Deduplication across RACK, PTO, RTO):**
    All recovery candidates—whether identified by RACK time-based detection,
    PTO tail probing, or Fallback RTO expiry—must be submitted to a
    centralized recovery coordinator that deduplicates work by `record_id`.
    The coordinator enforces:
    1.  **Delivery Check:** If `record_id <= receiver_delivered_through`,
        the recovery request is immediately dropped.
    2.  **In-Flight Deduplication:** If a transmission attempt for that
        `record_id` is already in flight and within an active round-trip
        window, redundant simultaneous retransmissions are suppressed.
    3.  **Conflict Resolution:** RACK loss recovery takes priority; PTO
        only probes when no loss repair is currently active; Fallback RTO
        acts only when all ACK progress has ceased. This guarantees that
        RACK, PTO, and Fallback RTO never schedule redundant simultaneous
        retransmissions for the same logical record.

## 13. Retransmission and delivery rules

-   Retransmissions retain the logical `record_id`.
-   Every transmission attempt gets a new `packet_id`.
-   Do not retransmit a record already confirmed delivered by the
    receiver's delivery progress, when that information is available.
-   A late ACK for an original attempt must update transmission history
    even if a retransmission has already succeeded.
-   The receiver delivers each logical record at most once and only in
    increasing record-ID order.
-   The receiver must not skip a missing record to improve throughput.

## 14. Flow control, application queue, and pacing

-   **Application Queue (`app_record_queue`):**
    -   A bounded FIFO queue between the record generator and transport pacer
        with a configurable default capacity of 5,000 records.
    -   **Overflow & Backpressure:** Generated records must **never be silently
        discarded**. If `app_record_queue` reaches capacity, the generator
        coroutine pauses asynchronously (`await app_record_queue.put(record)`)
        until the pacer drains records.
-   **Separation of Rate and Queue Metrics:**
    The implementation explicitly exposes and tracks these five metrics
    independently:
    1.  `intended_generation_rate`: The nominal configured target rate
        (e.g., 2,000 records/sec).
    2.  `actual_generation_rate`: The measured rate of logical records
        generated and pushed into `app_record_queue` per second.
    3.  `app_queue_size`: The current number of generated records waiting
        in `app_record_queue` to be transmitted.
    4.  `in_flight_count`: The number of unique packet transmissions
        currently sent over UDP that have neither been acknowledged nor retired.
    5.  `receiver_delivery_rate`: The measured rate of consecutive logical
        records delivered to the application at the receiver per second.
-   **Send Window:** Initial configurable in-flight limit (default 200 packets),
    representing transport capacity.
-   **Pacing & Measurement:**
    -   Batch-based pacing scheduled every 5--10 ms against monotonic
        `loop.time()`.
    -   The system measures and exposes `measured_batch_interval_ms` and
        `event_loop_lag_ms` (difference between scheduled and actual wake time).
    -   Do not claim 2,000 records/sec until validated by empirical measurement.
-   **Network Stall and Reordering Release Policy:**
    -   Stall queueing and reordering logic in the relay are independently
        configurable and testable.
    -   During a stall (default 400 ms on forward path), arriving datagrams
        are appended to a FIFO `stall_queue` without dropping.
    -   **Queue Release Policy:** When the stall expires, `stall_queue` is
        drained in arrival order. Any pre-assigned reordering delay jitter
        is preserved relative to queue release.
    -   When a stall halts forwarding, sender in-flight reaches `send_window`,
        transport halts, and newly generated records accumulate safely in
        `app_record_queue`. Once the stall clears, returning ACKs reduce
        in-flight count, and the sender resumes draining `app_record_queue`.

The initial implementation does not include a complete Internet-grade
congestion-control algorithm. It uses conservative batch pacing and a
bounded in-flight window, reports this limitation, and measures behaviour
at 2,000, 5,000, 10,000, and 20,000 records/sec.

## 15. Completion protocol

A sender must not stop merely because it has generated the final record.

The reliable FIN/FIN_ACK lifecycle is defined as follows:

1.  **FIN Initiation:** Once all records $1 \dots N$ have been generated
    and initially transmitted, the sender emits a `FIN` datagram containing
    `final_record_id` and assigned a new unique `packet_id`. The sender arms
    a dedicated FIN retransmission timer (e.g., $2 \times SRTT$).
2.  **FIN Retransmission Rule:** Every `FIN` retransmission receives a
    **fresh, unique `packet_id`** while retaining the exact same
    `final_record_id`.
3.  **Receiver In-Order Verification:**
    -   If the receiver receives `FIN` but records are still missing
        (`next_expected_record <= final_record_id`), the receiver records
        the final record ID, withholds `FIN_ACK`, and continues normal
        ACK feedback for incoming repair packets until all gaps are filled.
    -   Once `next_expected_record == final_record_id + 1`, the receiver
        delivers the final record and transmits `FIN_ACK`.
4.  **Lost FIN & Duplicate FIN Handling:**
    -   If `FIN` is dropped by the relay, the sender's FIN timer expires
        and the sender retransmits `FIN` with a new `packet_id`.
    -   If the receiver receives a duplicate `FIN` after having already
        completed delivery, it immediately retransmits `FIN_ACK`.
5.  **Lost FIN_ACK & Receiver State Retention (Linger):**
    -   If `FIN_ACK` is dropped on the reverse path, the sender will retransmit
        `FIN`.
    -   The receiver must retain connection state in a `LINGER` / `TIME_WAIT`
        state for a configurable default interval of 2.0 seconds after
        sending `FIN_ACK`, responding to every duplicate or retransmitted
        `FIN` with a repeated `FIN_ACK`.
6.  **Sender Completion:** The run is complete only when the sender receives
    a valid `FIN_ACK` confirming delivery through `final_record_id`. Retry
    exhaustion or a receiver timeout is an explicit failure, never a silent
    success.

## 16. Required metrics

Expose at least:

-   `intended_generation_rate`: Nominal configured rate (rec/sec).
-   `actual_generation_rate`: Measured record generation rate (rec/sec).
-   `app_queue_size`: Current and maximum application queue occupancy.
-   `in_flight_count`: Current unique unacknowledged UDP datagrams in flight.
-   `receiver_delivery_rate`: Measured application delivery rate (rec/sec).
-   Generated logical records.
-   Unique packet transmissions.
-   Received data transmissions.
-   Delivered logical records.
-   Network-dropped datagrams (by direction).
-   Reordered datagrams (by direction).
-   Delayed datagrams.
-   Retransmissions by reason: RACK, probe, fallback RTO.
-   Spurious retransmissions.
-   Current and maximum reorder-buffer size.
-   Current and smoothed RTT.
-   Minimum RTT.
-   Current reordering window and multiplier.
-   Batch timing interval and event-loop lag (mean and max ms).
-   ACK frequency (ACKs/sec) and ACK-to-DATA ratio.
-   Maximum gap between consecutive application deliveries ($\Delta t_{\text{delivery}}$).
-   Final missing-record count, duplicate delivery count, and ordering validation.

## 17. Non-goals

Do not implement:

-   Multiple independent application streams.
-   Multipath routing, UET, or MRC.
-   Kernel-level NIC reordering.
-   Full QUIC or TCP compatibility.
-   Audio/video jitter-buffer processing or NetEQ.
-   Machine-learning-based loss prediction.
-   Authentication, multi-tenancy, or deployment infrastructure.
-   A polished dashboard.

These are outside the assignment's essential scope.

## 18. Acceptance criteria

The implementation is acceptable only when:

1.  Sender and receiver communicate through actual UDP sockets via the
    impairment relay.
2.  The basic no-impairment case delivers all records in order.
3.  The required impairment conditions can be reproduced with a fixed
    seed.
4.  Loss, reordering, duplicate arrival, delayed arrival, ACK loss,
    retransmission loss, and tail loss are tested.
5.  No logical record is delivered twice or out of order.
6.  The sender does not silently declare success with missing records.
7.  The system reports measured throughput and delivery gaps.
8.  All components start through one documented command.
9.  The browser dashboard drives the system and shows live
    connection/link state.
10. The design's assumptions and limitations are documented.
