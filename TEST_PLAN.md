# TAR Test Plan

## 1. Purpose

This plan verifies functional correctness, loss recovery, reordering
behaviour, resource bounds, and performance for the TAR experimental
reliable transport.

Tests must be repeatable. The network simulator must accept a fixed
random seed and log impairment decisions so a failure can be reproduced.

## 2. Test environment and default profile

  Setting                                                     Default
  ------------------------ ------------------------------------------
  Logical record rate                               2,000 records/sec
  Data packet loss                                                 5%
  ACK packet loss                                                  5%
  Reordering probability                                           2%
  Network stall                                                400 ms
  Initial ACK policy              10 packets or 5 ms, whichever first
  Timer granularity                                              1 ms
  Maximum ACK delay                                              5 ms
  Initial reorder window     Minimum RTT / 4 after RTT initialization
  Reorder-window cap                                     Smoothed RTT
  Receiver delivery                                  Strictly ordered
  Sender window                              Configurable and bounded
  Random seed                                    Fixed per named test

Clarify in the test report whether the configured loss and reordering
probabilities apply independently per datagram and per direction. The
initial design applies them independently to both data and ACK
directions.

## 3. Functional tests (Automated E2E Reliability Suite T01–T18)

  -------------------------------------------------------------------------------------------------------
  ID    Scenario             Setup / Impairments                 Expected Result & Status
  ----- -------------------- ----------------------------------- ----------------------------------------
  T00   Generator validation >= 100 edge-case records            All pass schema; MTU bounded [PASS]
  T01   Clean path baseline  No loss, no reorder, no stall       100/100 in order, 0 retransmissions [PASS]
  T02   5% data loss         5% forward loss                     100/100 recovered via SACK/RACK [PASS]
  T03   2% reordering        2% forward jitter (10-30ms)         ReorderBuffer in-order delivery [PASS]
  T04   400 ms link stall    Forward stall (0.05s int, 400ms)    Delivered without drop; stall gap < 1s [PASS]
  T05   Spurious recovery    Delayed original arrival            Spurious detection & metric increment [PASS]
  T06   Adaptive window      Repeated spurious events            Window multiplier grows 1.0x to 4.0x [PASS]
  T07   16-flight decay      16 clean recovery rounds            Multiplier returns toward baseline [PASS]
  T08   PTO tail loss probe  Drop tail packet                    Probe elicits SACK without loss mark [PASS]
  T09   10% reverse ACK loss 10% reverse ACK loss                Progress recovered; all records delivered [PASS]
  T10   Lost retransmission  Retransmission dropped              RTO backoff (2.0s); 3rd attempt succeeds [PASS]
  T11   Duplicate packets    Deliver duplicate packets           Duplicates suppressed; 0 duplicate app recs [PASS]
  T12   32-range floor bound 100 discontiguous packet IDs        Ranges bounded <= 32; floor monotonic [PASS]
  T13   Combined stress      5% loss + 2% reorder + 400ms stall  100/100 in order; stall bound < 1s met [PASS]
  T14   FIN with gaps repair FIN received with missing records   Receiver withholds FIN_ACK; SACK repairs [PASS]
  T15   Retry exhaustion     10 dropped retransmissions          Terminal FAILED state triggered [PASS]
  T16   Missing PID retire   Permanently missing original PID    Watermarks advance; memory pruned cleanly [PASS]
  T17   Buffer pass-through  Buffer full at capacity (5,000)     In-order packet passes through and flushes [PASS]
  T18   Repair cancellation  ACK arrives for repair in-flight    Pending repair canceled; BUFFERED state [PASS]
  -------------------------------------------------------------------------------------------------------

## 4. Algorithm comparison experiments

Use the same record stream, impairment seed, and network conditions for
each configuration.

### Baseline A: Fixed RTO

Use a conventional timeout-based retransmission baseline. Document its
timer settings.

### Baseline B: RACK-inspired fixed window

Use later-ACK evidence and a fixed reordering window, without
adaptation.

### TAR: Full hybrid

Use unique transmission IDs, selective ACK ranges, RACK-inspired
time-based detection, adaptive reordering tolerance, PTO-inspired
probing, and bounded fallback RTO.

Compare:

-   Total retransmissions.
-   Spurious retransmissions.
-   Mean and percentile recovery time.
-   Maximum application delivery gap.
-   Throughput.
-   Receiver buffer occupancy.
-   Probe count and fallback-RTO count.

Do not assume TAR wins. Report measured outcomes, including cases where
adaptation delays recovery.

## 5. Reordering sensitivity experiment

Run with:

-   0% reordering.
-   2% reordering.
-   5% reordering.
-   10% reordering.

Keep other parameters and the random-seed policy controlled. Record:

-   Spurious retransmission count.
-   Reordering-window history.
-   Genuine-loss recovery time.
-   Total retransmissions.
-   Delivery gap.

## 6. Load-scaling experiment

Run at:

-   2,000 records/sec.
-   5,000 records/sec.
-   10,000 records/sec.
-   20,000 records/sec.

For each rate, record:

-   Offered record rate.
-   Actual sender transmission rate.
-   Receiver delivery rate.
-   CPU usage.
-   Memory usage.
-   Sender in-flight count.
-   Receiver reorder-buffer size.
-   Retransmission rate.
-   Maximum delivery gap.

The assignment's target is approximately 2,000 records/sec. Higher rates
are stress experiments, not required pass thresholds unless separately
specified.

## 7. Correctness invariants

At the end of a successful run:

1.  Delivered record IDs are exactly the expected sequence from 1
    through the final record ID.
2.  No record ID appears more than once in application delivery.
3.  No record is delivered before its predecessor.
4.  Receiver `next_expected_record` equals `final_record_id + 1`.
5.  Sender has received a valid FIN_ACK for the same connection and
    final record ID.
6.  No unresolved logical record is reported as successful.
7.  Every retry-exhausted record causes explicit failure.
8.  All packet IDs are unique within the connection.

## 8. Timing and performance definitions

-   **Throughput:** Logical records delivered per second, measured over
    a clearly stated interval.
-   **Recovery time:** Time from the simulator's drop event to
    successful receiver delivery of the affected logical record.
    Drop-event timestamps are simulator diagnostics; use a consistent
    local measurement method.
-   **Delivery gap:** Time between consecutive application record
    deliveries at the receiver.
-   **Spurious retransmission:** A retransmission caused by a loss
    decision where the original transmission is subsequently
    acknowledged as having arrived.
-   **Buffer occupancy:** Number of logical records currently waiting in
    the reorder buffer.

Use monotonic clocks for durations within each process. If combining
event times from different processes, use relay sequence/log correlation
or an explicit clock synchronization strategy; do not subtract unrelated
monotonic clock values.

## 9. Failure injection and reproducibility

The simulator should support:

-   Fixed random seed.
-   Configurable loss probability by direction and packet type.
-   Configurable reordering probability and delay distribution.
-   Explicit 400 ms stall injection.
-   Targeted drop of a selected packet ID or record ID.
-   Targeted ACK drop.
-   Targeted retransmission drop.
-   Tail-packet drop.
-   Event log containing decision, packet ID, record ID, direction, and
    local event time.

Targeted injection is necessary to make awkward cases happen on demand.

## 10. Reporting template

For every experiment, record:

  Field                              Result
  ---------------------------------- --------
  Test ID / configuration            
  Random seed                        
  Offered records and rate           
  Delivered records                  
  Missing records                    
  Duplicate deliveries               
  Out-of-order deliveries            
  Total transmissions                
  Retransmissions                    
  Spurious retransmissions           
  Mean / p95 recovery time           
  Maximum delivery gap               
  Maximum reorder-buffer occupancy   
  Maximum app-queue size             
  Throughput (measured rec/sec)      
  Event-loop lag (mean / max ms)     
  Batch timing interval (ms)         
  ACK frequency / ACK-to-DATA ratio  
  CPU / memory                       
  Pass/fail and explanation          

A test passes only when its expected correctness properties are met.
Performance targets must be reported as measured values, not inferred
from a successful functional test.

## 11. Phase 3 Test Matrix & Empirical Results

The Network Impairment Relay was verified against 14 automated test cases in `tests/test_relay.py` (all 14 passed, total 58 tests passing across suite):

| Test ID | Test Name | Target Property | Result |
| :--- | :--- | :--- | :--- |
| **TR01** | `test_zero_impairment_regression` | 100% clean bidirectional delivery, raw byte preservation | PASS |
| **TR02** | `test_deterministic_loss_repeatability` | Deterministic loss seed reproducibility & 3-sigma compliance | PASS |
| **TR03** | `test_packet_reordering_independent` | Jitter delay and session-scoped out-of-order tracking | PASS |
| **TR04** | `test_stall_queue_and_release` | 400 ms link stall holding packets and draining without loss | PASS |
| **TR05** | `test_stall_preserves_configured_reordering` | Reordering preservation across stall boundary | PASS |
| **TR06** | `test_overlapping_stall_exact_scenario` | Overlapping stall extending boundary (10.0s -> 10.4s -> 10.43s) | PASS |
| **TR07** | `test_previously_queued_packets_during_stall` | Pre-existing queue shift without heap corruption | PASS |
| **TR08** | `test_independent_forward_and_reverse_scheduling` | Forward stall never blocks reverse traffic | PASS |
| **TR09** | `test_combined_impairments_stress` | 5% loss + 10% reorder + 400ms stall robustly handled | PASS |
| **TR10** | `test_targeted_fault_drop_and_delay` | Targeted drop of packet #7, targeted delay of packet #3 | PASS |
| **TR11** | `test_targeted_stall_single_direction` | Directional stall affecting only forward path | PASS |
| **TR12** | `test_malformed_and_oversized_datagram_safety` | Non-JSON, garbage bytes, and 4KB packets safely handled | PASS |
| **TR13** | `test_e2e_three_process_clean_run` | 3-process loopback integration test (100 records) | PASS |
| **TR14** | `test_tr14_manual_stall_configuration_reproduction` | Manual stall config reproduction (--loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42) | PASS |

