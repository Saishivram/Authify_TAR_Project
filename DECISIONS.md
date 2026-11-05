# DECISIONS.md

## Project: TAR --- Time-Aware Reliable Transport

This document records the current design assumptions, choices,
alternatives, trade-offs, and known limitations. Update it when
implementation evidence changes a decision. Do not describe an
unimplemented feature as completed.

## 1. What we chose and why

We chose Python and UDP sockets for a custom reliable, ordered transport
simulation. UDP exposes loss, duplication, delay, and reordering to the
application, allowing us to implement and observe the reliability
mechanisms required by the assignment. The sender and receiver will run
as independent processes and communicate through a dedicated UDP
impairment relay. A separate FastAPI control server will start and
observe the system and provide WebSocket updates to a plain browser
dashboard.

The protocol separates logical record IDs from transmission packet IDs.
A retransmission keeps the original record ID but receives a new packet
ID. This is inspired by QUIC's unambiguous packet-number tracking and
makes it possible to distinguish a late original transmission from its
retransmission. The receiver uses a bounded reorder buffer and delivers
only consecutive records, preserving strict ordering and preventing
duplicate application delivery.

For loss recovery, we chose a RACK-inspired time-based detector, an
adaptive reordering window, and a QUIC PTO/TLP-inspired probe for tail
loss. Selective ACK ranges report received transmission IDs. A bounded
fallback retransmission timeout handles cases where ACK progress stops
entirely. This is a custom hybrid, not a complete implementation of RFC
8985, QUIC, TCP, or any interoperable standard.

## 2. Assumptions and what would fail if they are wrong

-   There is one sender, one receiver, and one ordered logical stream.
    Multiple streams or senders would require separate sequencing and
    delivery state.
-   UDP is the data transport. Using TCP would hide much of the
    loss/reordering behaviour the assignment expects us to handle.
-   Logical record IDs are unique and sequential within a session.
    Reused or missing IDs would undermine ordering validation.
-   Every transmission attempt has a unique packet ID. Reusing IDs would
    make ACK and retransmission history ambiguous.
-   The impairment relay affects both data and ACK directions. If ACK
    traffic were reliable by assumption, recovery results would be too
    optimistic.
-   The initial ACK policy is immediate on a detected gap or probe,
    otherwise every 10 newly received packets or 5 ms, whichever occurs
    first. Different ACK behaviour changes feedback delay and overhead.
-   The initial reordering window is based on one quarter of minimum
    observed RTT after an RTT sample exists. Before that, a configured
    bootstrap value is used. If actual path reordering is larger,
    spurious retransmissions may occur.
-   The adaptive reordering window is capped and eventually returns
    toward baseline after a configured period without new reordering
    evidence. This is a simplified policy and will be evaluated against
    test results.
-   Sender and receiver state is initially in memory. Process crashes do
    not have durable recovery across restart.
-   The system initially handles one ordered stream. It does not provide
    independent progress for multiple streams.
-   The one-second delivery-stall target is evaluated under the
    specified test conditions. No finite recovery bound can be
    guaranteed during arbitrary or prolonged outages.
-   The initial implementation does not include full Internet-grade
    congestion control. It uses bounded in-flight work and conservative
    pacing; high-load behaviour must be measured.

## 3. Trade-offs

### UDP instead of TCP

**Benefit:** Direct control over packet identity, ACKs, loss detection,
retransmission, and ordering.

**Cost:** We must implement these mechanisms ourselves and must avoid
claiming TCP-like congestion-control behaviour without implementing it.

### RACK-inspired time-based detection

**Benefit:** Uses later acknowledgement evidence and elapsed time,
making it less dependent on duplicate-ACK counts when packets reorder.

**Cost:** Requires transmission timestamps, ACK history, RTT estimates,
timers, and a carefully chosen reordering window. A window that is too
small causes spurious retransmissions; one that is too large delays
recovery.

### QUIC-inspired unique transmission IDs and ACK ranges

**Benefit:** Each transmission attempt is unambiguous, including
retransmissions of the same logical record. ACK ranges communicate
non-contiguous reception.

**Cost:** More state, packet metadata, range processing, and
history-retention logic.

### Adaptive reordering window

**Benefit:** Can learn from observed spurious retransmissions and become
more tolerant of reordering.

**Cost:** More state and complexity. Adaptation can become too
conservative and delay genuine loss recovery. The implementation is
inspired by RFC 8985 but is not claimed to be identical to it.

### Strict ordered delivery and receiver buffering

**Benefit:** Meets the assignment's ordering requirement and prevents
the application from observing gaps or duplicates.

**Cost:** Head-of-line blocking: later records wait behind a missing
earlier record. This is an unavoidable consequence of strict ordering
for one stream.

### Tail-loss probes

**Benefit:** Can obtain fresh ACK evidence when the final outstanding
transmission is lost and no later packet exists to trigger RACK-style
inference.

**Cost:** Adds traffic and timer state. A probe timeout is not proof of
loss, so the implementation must not declare loss solely because PTO
expired.

### JSON over UDP

**Benefit:** Easy to inspect, debug, log, and demonstrate.

**Cost:** More serialization overhead and larger packets than a compact
binary format. We will measure whether this matters at the target rate.

### In-memory state

**Benefit:** Simple and quick to implement for a single-run simulation.

**Cost:** No durable recovery after a process crash. Persistence is
outside the initial scope.

## 4. Alternatives considered and rejected

-   **TCP sockets:** Rejected for the data path because TCP already
    performs reliability and ordered delivery, hiding the custom
    mechanisms being evaluated. TCP may still be used for the local
    dashboard/API if needed.
-   **Cumulative ACK only:** Rejected because it provides less
    information about non-contiguous packet reception than selective ACK
    ranges.
-   **Duplicate-ACK counting as the primary loss detector:** Rejected
    because reordering can trigger misleading duplicate
    acknowledgements.
-   **Fixed RTO only:** Retained as a fallback, but not selected as the
    primary detector because it may recover slowly or retransmit
    unnecessarily.
-   **Full QUIC implementation:** Rejected because the assignment needs
    a focused custom transport, not a standards-compliant QUIC stack.
-   **Multipath UET/MRC:** Rejected because the assignment can be
    satisfied with one simulated path; multipath and hardware NIC
    behaviour would expand scope substantially.
-   **NetEQ or machine-learning jitter adaptation:** Rejected because
    these address real-time media playout rather than reliable ordered
    record delivery.
-   **Database, Kafka, Redis, authentication, and deployment
    infrastructure:** Rejected as unnecessary for the required local
    simulation and contrary to scope discipline.

## 5. What we will measure

We will compare a fixed-RTO baseline, a RACK-inspired detector with a
fixed reordering window, and the full TAR hybrid using identical
deterministic impairment seeds.

Metrics include:

-   Correctness: missing, duplicate, and out-of-order delivered records.
-   Retransmissions and spurious retransmissions.
-   Recovery time and maximum gap between application deliveries.
-   Throughput and CPU usage.
-   Current/maximum receiver buffer occupancy.
-   RTT estimates and adaptive reordering-window changes.
-   Probe and fallback-RTO counts.

We will not claim the hybrid is better unless measured results support
that conclusion.

## 6. What we would do next with more time

-   Implement durable sender/receiver state and crash recovery.
-   Add a carefully specified congestion-control and pacing algorithm.
-   Evaluate binary packet encoding and serialization cost.
-   Explore multiple independent streams and per-stream ordering.
-   Test across separate physical machines and real network interfaces.
-   Compare the adaptive reordering policy with the full RFC 8985
    behaviour.
-   Add more extensive fuzzing and protocol-state-machine tests.

## 7. Decision log

  -----------------------------------------------------------------------
  Date                    Decision                Reason / evidence
  ----------------------- ----------------------- -----------------------
  2026-11-04              Python + UDP,           Initial project
                          independent             direction
                          sender/receiver,        
                          impairment relay        

  2026-11-04              Separate record IDs and Avoid retransmission
                          transmission packet IDs ambiguity

  2026-11-04              RACK loss deadline      loss_deadline(P) =
                          + mandatory later ACK   P.sent_at + RTT + reo_wnd.
                                                  Mandatory precondition:
                                                  P.sent_at < latest_tx; timer
                                                  expiry alone cannot infer loss

  2026-11-04              Contiguous ACK          ack_retired_through_packet_id
                          retirement watermark    advances ONLY across contiguous
                                                  prefix; never crosses gaps;
                                                  duplicates safely ignored

  2026-11-04              Recovery flight rule    Flight defined at loss entry;
                                                  round completes only when all
                                                  flight attempts reach terminal
                                                  state (ACKed, lost, or failed)

  2026-11-04              Centralized Recovery    Deduplicates RACK, PTO, and
                          Coordinator             RTO candidates by record_id;
                                                  prevents simultaneous redundant
                                                  retransmissions

  2026-11-04              App record queue        5,000 capacity; backpressure
                          backpressure & metrics  pauses generator; zero silent drops;
                                                  exposes intended, actual rates,
                                                  queue size, in-flight, delivery rate

  2026-11-04              Measurable pacing       5-10ms batch pacing; measure rate,
                                                  batch intervals, and event-loop lag;
                                                  2,000 rec/s is a target to measure

  2026-11-04              Reliable FIN lifecycle  Fresh packet_id per retransmission;
                          + 2s receiver linger    duplicate FIN triggers repeated FIN_ACK

  2026-11-04              1-second delivery       Measured acceptance criterion
                          acceptance criterion    across stalls, delay, and loss;
                                                  not a theoretical outage SLA

  2026-11-04              Independent relay       Stall queue dispatches held
                          stall & reorder release packets with relative reorder
                                                  delays preserved after stall clears

  2026-11-04              Configurable port       Sender 9001, Relay 9002/9003,
                          defaults                Receiver 9004, Control API 8000

  2026-11-04              Direct UDP Socket       Sender 127.0.0.1:9001, Receiver
                          Transport (Phase 2)     127.0.0.1:9004; verified direct loopback
                                                  delivery before relay insertion

  2026-11-04              Strict Datagram Wire    Measured actual byte length per datagram;
                          MTU (1,200 Bytes)       hard OversizedDatagramError rejection;
                                                  recvfrom(65535) reads full datagrams up to 64KB
                                                  so oversized packets are detected & rejected;
                                                  SO_RCVBUF expands OS receive queue capacity

  2026-11-04              START / START_ACK       Sender requires START_ACK before DATA;
                          Session Handshake       bounded retries (5x, 500ms); receiver
                                                  START handler is strictly idempotent

  2026-11-04              Pre-FIN_ACK Delivery    Receiver validates total count, strict order,
                          Verification & 2s Linger and zero missing/duplicate records before
                                                  issuing FIN_ACK; never ACKs incomplete data;
                                                  lingers for 2.0s to absorb duplicate FINs
                                                  with cached FIN_ACK before clean exit

  2026-11-04              Throughput & Metric     Separated sender DATA-loop throughput from full
                          Accounting              session throughput; track sent, received, missing,
                                                  duplicate, out-of-order, and oversized counts

  2026-11-04              Directional Decoupling  Forward and Reverse schedulers completely
                          in Relay (Phase 3)      independent; forward stall never blocks reverse ACKs

  2026-11-04              Min-Heap Relative-      Preserves genuine packet reordering across 400ms stalls;
                          Offset Scheduler        tracks boundary_offset; shifts heap safely without
                                                  in-place mutation; handles overlapping stalls cleanly

  2026-11-04              Rate-Based Stalls       Replaced per-packet probability with time-based interval
                          (Phase 3)               (default 5.0s, 400ms duration) to avoid stall storms

  2026-11-04              Session-Scoped Out-     Sequence inversions measured strictly per (connection_id,
                          of-Order Metrics        direction) across monotonic packet_ids (DATA, FIN)

  2026-11-04              Stall Interval Timer    Initialized last_stall_trigger_time to scheduler creation
                          Initialization          (self.time_func()); removed first-packet suppression so
                                                  short bursts arriving after interval trigger stalls reliably

  2026-11-05              Dual SACK Watermarks    Separated ack_received_through_packet_id (highest contiguous
                          (Phase 4)               received packet ID) from ack_range_floor_packet_id (lower
                                                  boundary of retained SACK history). Truncating old SACK ranges
                                                  below the floor treats omitted packet IDs as UNKNOWN, never
                                                  assumed received or lost. Prevents false completion or premature loss.

  2026-11-05              BUFFERED_AT_RECEIVER    Added explicit intermediate state separating packet receipt from
                          State & Coordinator     application delivery. An ACK matching a packet_id transitions
                          (Phase 4)               record to BUFFERED_AT_RECEIVER; only receiver_delivered_through
                                                  transitions record to DELIVERED. Feedback cancels pending repairs.

  2026-11-05              Separate PTO Tail       PTO probe expiration sends lightweight ProbePacket with fresh
                          Loss Probing (Phase 4)  packet_id to elicit immediate SACK feedback; PTO does NOT mark
                                                  transmissions lost or move records to REPAIR_PENDING. Exponential
                                                  backoff with max 2 probes before deferring to Fallback RTO.

  2026-11-05              ReorderBuffer In-Order  In-order records (record_id == next_expected_record) always bypass
                          Pass-Through Invariant  buffer capacity check, delivering immediately to disk and cascading
                          (Phase 4)               consecutive buffered records. Out-of-order packets drop on full capacity.

  2026-11-05              Gap-Aware FIN Lifecycle When enable_fin_repair=True, receipt of FIN with delivery gaps withholds
                          Teardown (Phase 4)      FIN_ACK and returns immediate SACK; sender repairs missing records
                                                  and retries FIN until full verification succeeds.

  2026-11-05              RFC 6298 RTT Estimator  Monotonic clocks only (time.monotonic()); alpha=0.125, beta=0.25;
                          & Karn's Sampling       Karn's algorithm samples only first-attempt un-repaired transmissions;
                          (Phase 4)               bounds ACK delay subtraction (<=5ms); adaptive reordering multiplier
                                                  (1.0x - 4.0x) with 16-flight clean recovery decay engine.

  2026-11-05              FastAPI + WebSockets    Single-process FastAPI control server with async WebSockets stream
                          Control Server (Phase 5) (100ms broadcast interval) + REST API (/api/status, /api/start,
                                                  /api/stop, /api/configure, /api/events, /api/attention). Eliminates
                                                  manual multi-terminal juggling while reusing exact Phase 4 transport core.

  2026-11-05              Threaded SystemManager  Thread-isolated lifecycle management for UdpReceiver, ImpairmentRelay,
                          Lifecycle (Phase 5)     and UdpSender. Cooperative stop() hooks with linger breaks prevent port
                                                  collisions and deadlocks; preserves full standalone CLI capability.

  2026-11-05              Live Dynamic Impairment In-place atomic update of DirectionScheduler parameters on running
                          Reconfiguration         ImpairmentRelay without socket tear-down or dropped sessions.
                          (Phase 5)

  2026-11-05              Human Attention         Automated anomaly detection flagging loss > 25%, stall duration > 800ms,
                          Heuristics (Phase 5)    component crash, or incomplete delivery. Operator-dismissable alerts.

  2026-11-05              Strict Light-Themed UI  White/light-slate (#f8fafc) background, subtle blue accents (#2563eb),
                          Design System (Phase 5) pure card surfaces (#ffffff), 3-second heartbeat ping. Strictly no dark
                                                  themes, heavy animations, or external framework dependencies.

  2026-11-05              Single Startup Command  Single entrypoint `python -m server.app` launches dashboard and control
                          & Clean Machine Setup   server; 100% self-contained standard library + FastAPI; 0 external
                          (Phase 6)               database, Redis, or Kafka dependencies; 128 automated unit tests pass.
  -----------------------------------------------------------------------

Update this log with actual implementation decisions, measured values,
deviations, and reasons. Replace assumptions when experiments provide
better evidence.
