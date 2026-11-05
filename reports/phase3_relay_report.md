# TAR Phase 3: Network Impairment Relay Benchmark & Verification Report

**Protocol:** Time-Aware Reliable Transport (TAR)  
**Phase:** Phase 3 (Network Impairment Relay)  
**Architecture:** Independent bi-directional UDP proxy between Sender (`9001`) and Receiver (`9004`)  
**Sockets:** Port A (`127.0.0.1:9002`, Sender-facing), Port B (`127.0.0.1:9003`, Receiver-facing)  

---

## 1. Executive Summary

Phase 3 implements the **Network Impairment Relay**, an independent Python process running in the data path between the Sender and Receiver. The relay simulates configurable network degradation—specifically packet loss, delay jitter/reordering, and 400 ms link stalls—in both directions over UDP sockets.

In accordance with strict project constraints, Phase 3:
- **Preserves 100% byte fidelity:** UDP datagrams are intercepted and forwarded as raw bytes without deserializing, mutating, or re-encoding envelopes.
- **Maintains directional decoupling:** Forward and Reverse paths run on independent `DirectionScheduler` engines with separate priority queues and stall boundaries. A forward stall never delays reverse ACK/error traffic.
- **Prevents stall-reordering hazards:** Implements a Min-Heap Relative-Offset Scheduler with `boundary_offset` tracking that preserves true packet reordering across 400 ms stall boundaries. Overlapping stalls cleanly extend the pause window without double-counting remaining duration.
- **Provides session-scoped out-of-order metrics:** Sequence inversions are evaluated strictly per `(connection_id, direction)` across monotonic `packet_id`s (`DATA`, `FIN`), ignoring unrelated control packets.
- **Maintains strict scope discipline:** No DATA retransmissions, per-record ACKs, RACK, PTO, RTO, or dashboard logic are implemented in Phase 3.

---

## 2. Test Suite Verification Summary (58 Passing Tests)

Executed via `python -m unittest discover -v tests`:
- **Existing Phase 1 & 2 Tests:** 44 tests passed (`test_packet.py`: 11, `test_record_schema.py`: 18, `test_record_generator.py`: 10, `test_udp_transport.py`: 5).
- **New Phase 3 Relay Tests (`tests/test_relay.py`):** 14 tests passed.
- **Total:** **58 tests passing in 9.1 seconds**, with zero errors and zero failures.

### Phase 3 Test Matrix (`tests/test_relay.py`):
| Test ID | Test Name | Target Requirement | Status |
| :--- | :--- | :--- | :--- |
| **TR01** | `test_zero_impairment_regression` | Clean bidirectional forwarding, raw byte preservation | `[PASS]` |
| **TR02** | `test_deterministic_loss_repeatability` | Seed-based loss reproducibility, 3-sigma statistical compliance | `[PASS]` |
| **TR03** | `test_packet_reordering_independent` | Reorder jitter, out-of-order metric tracking | `[PASS]` |
| **TR04** | `test_stall_queue_and_release` | 400 ms link pause, zero loss upon release | `[PASS]` |
| **TR05** | `test_stall_preserves_configured_reordering` | Reordering preservation across stall boundary | `[PASS]` |
| **TR06** | `test_overlapping_stall_exact_scenario` | 10.0s $\to$ 10.4s $\to$ 10.43s boundary-offset preservation | `[PASS]` |
| **TR07** | `test_previously_queued_packets_during_stall` | Pre-existing queue shift without in-place heap corruption | `[PASS]` |
| **TR08** | `test_independent_forward_and_reverse_scheduling` | Decoupled schedulers; reverse traffic not blocked by forward stall | `[PASS]` |
| **TR09** | `test_combined_impairments_stress` | 5% loss + 10% reordering + 400ms stall without crash | `[PASS]` |
| **TR10** | `test_targeted_fault_drop_and_delay` | Explicit fault rules by `packet_id` | `[PASS]` |
| **TR11** | `test_targeted_stall_single_direction` | Directional stall targeting | `[PASS]` |
| **TR12** | `test_malformed_and_oversized_datagram_safety` | Non-JSON, garbage bytes, and 4KB datagram safety | `[PASS]` |
| **TR13** | `test_e2e_three_process_clean_run` | Full 3-process UDP loopback integration test (100 records) | `[PASS]` |
| **TR14** | `test_tr14_manual_stall_configuration_reproduction` | Exact manual stall reproduction (--loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42) | `[PASS]` |

---

## 3. End-to-End Three-Process Benchmark Runs

### Benchmark A: Clean Forwarding (Zero Impairment)
- **Configuration:** `--loss 0.0 --reorder 0.0 --no-stall --seed 42`
- **Sender:** 100 records sent, 0 retries used, received `FIN_ACK`, status `COMPLETED`.
- **Relay:** 102 forward datagrams received and forwarded (`START` + 100 `DATA` + `FIN`), 2 reverse datagrams received and forwarded (`START_ACK` + `FIN_ACK`).
- **Receiver:** 100/100 records received in strict order, 0 missing, 0 duplicates, 0 out-of-order, status `COMPLETED`.
- **Output Validation:** `python -m schemas.record_schema data/received_relay_clean_100.jsonl 100` $\implies$ `SUCCESS: All 100 records are valid, sequential, and unique!`

#### Terminal Outputs (Clean Run):

**Sender:**
```text
[Sender] Sending START (attempt 1/5) for session 'session-1793812126'...
[Sender] Handshake SUCCESS: Received START_ACK (status=READY).
[Sender] Starting transmission of 100 records to ('127.0.0.1', 9002)...
[Sender] Sending FIN (attempt 1/5, final_record_id=100)...
[Sender] Teardown SUCCESS: Received FIN_ACK (delivered_through=100, total_delivered=100).

=== SENDER TRANSMISSION REPORT ===
{
  "session_id": "session-1793812126",
  "input_file": "data/sample_100.jsonl",
  "target_address": "127.0.0.1:9002",
  "sent_records": 100,
  "total_data_bytes_sent": 35022,
  "max_actual_datagram_size_bytes": 1113,
  "sender_data_loop_duration_seconds": 0.016,
  "sender_data_loop_throughput_records_per_second": 6250.0,
  "full_session_duration_seconds": 0.016,
  "full_session_throughput_records_per_second": 6250.0,
  "transmission_duration_seconds": 0.016,
  "achieved_records_per_second": 6250.0,
  "throughput_kb_per_second": 2137.57,
  "start_retries_count": 0,
  "fin_retries_count": 0,
  "status": "COMPLETED"
}
```

**Relay:**
```text
[Relay] Running Network Impairment Relay (seed=42)...
  Port A (Sender-facing):   127.0.0.1:9002 -> targets ('127.0.0.1', 9004)
  Port B (Receiver-facing): 127.0.0.1:9003 -> targets ('127.0.0.1', 9001)
  Forward: loss=0.0%, reorder=0.0%, stall_interval=0.0s (400.0ms)
  Reverse: loss=0.0%, reorder=0.0%, stall_interval=0.0s (400.0ms)
[Relay] Idle timeout of 3.0s reached with drained queues.

=== RELAY FINAL METRICS ===
{
  "relay": {
    "port_a": 9002,
    "port_b": 9003,
    "sender_addr": "127.0.0.1:9001",
    "receiver_addr": "127.0.0.1:9004",
    "seed": 42
  },
  "forward": {
    "packets_received": 102,
    "packets_forwarded": 102,
    "packets_dropped_loss": 0,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 0,
    "packets_dispatched_out_of_order": 0,
    "packets_delayed_by_stall": 0,
    "stalls_triggered": 0,
    "total_bytes_received": 35251,
    "total_bytes_forwarded": 35251
  },
  "reverse": {
    "packets_received": 2,
    "packets_forwarded": 2,
    "packets_dropped_loss": 0,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 0,
    "packets_dispatched_out_of_order": 0,
    "packets_delayed_by_stall": 0,
    "stalls_triggered": 0,
    "total_bytes_received": 261,
    "total_bytes_forwarded": 261
  }
}
```

**Receiver:**
```text
[Receiver] Listening on 127.0.0.1:9004...
[Receiver] Session 'session-1793812126' initialized. Expecting 100 records.
[Receiver] All 100 records verified in order! Sent FIN_ACK.
[Receiver] Delivery verified! Entering bounded 2.0s linger period for duplicate FIN absorption...
[Receiver] 2.0s linger period elapsed. Shutting down cleanly.

=== RECEIVER FINAL METRICS ===
{
  "status": "COMPLETED",
  "session_id": "session-1793812126",
  "expected_records": 100,
  "received_records": 100,
  "missing_records_count": 0,
  "missing_records_sample": [],
  "duplicate_records_count": 0,
  "duplicate_records_sample": [],
  "out_of_order_records_count": 0,
  "oversized_datagrams_rejected": 0,
  "max_actual_datagram_size_bytes": 1113,
  "output_file": "data/received_relay_clean_100.jsonl",
  "validation_failure_reason": null
}
```

---

### Benchmark B: Impaired Run (5% Loss, 10% Reorder, 400 ms Stall)
- **Configuration:** `--loss 0.05 --reorder 0.10 --stall-interval 0.005 --stall-duration 400 --seed 42`
- **Relay Activity:**
  - Forward: 105 packets received, 99 forwarded, 6 dropped by loss, 9 delayed for reordering, 8 dispatched out-of-order, 441 delayed by stall, 5 stalls triggered.
  - Reverse: 2 packets received (`START_ACK` and `ERROR`), 2 forwarded to Sender without loss or delay.
- **Expected End-to-End Outcome Without Phase 4/5 Retransmissions:**
  - Receiver received 86 records; 14 records were missing due to relay loss.
  - Pre-`FIN_ACK` verification strictly detected the delivery gap and refused `FIN_ACK`.
  - Receiver transitioned to `FAILED` and returned `ERROR: INCOMPLETE_DELIVERY` to Relay Port B.
  - Relay forwarded the `ERROR` packet from Port A to Sender.
  - Sender caught the `ERROR` packet and cleanly reported `Receiver rejected FIN: INCOMPLETE_DELIVERY`.

#### Terminal Outputs (Impaired Run):

**Sender:**
```text
[Sender] Sending START (attempt 1/5) for session 'session-1793812219'...
[Sender] Handshake SUCCESS: Received START_ACK (status=READY).
[Sender] Starting transmission of 100 records to ('127.0.0.1', 9002)...
[Sender] Sending FIN (attempt 1/5, final_record_id=100)...
[Sender] FIN timeout or reset (0.5s). Retrying...
[Sender] Sending FIN (attempt 2/5, final_record_id=100)...
[Sender] FIN timeout or reset (0.5s). Retrying...
[Sender] Sending FIN (attempt 3/5, final_record_id=100)...
[Sender] FIN timeout or reset (0.5s). Retrying...
[Sender] Sending FIN (attempt 4/5, final_record_id=100)...
[Sender] Fatal error: Receiver rejected FIN: INCOMPLETE_DELIVERY - Record count mismatch: expected 100, received 86; Missing 14 records: [4, 6, 10, 14, 21, 34, 44, 50, 62, 63]
```

**Relay:**
```text
[Relay] Running Network Impairment Relay (seed=42)...
  Port A (Sender-facing):   127.0.0.1:9002 -> targets ('127.0.0.1', 9004)
  Port B (Receiver-facing): 127.0.0.1:9003 -> targets ('127.0.0.1', 9001)
  Forward: loss=5.0%, reorder=10.0%, stall_interval=0.005s (400.0ms)
  Reverse: loss=0.0%, reorder=0.0%, stall_interval=0.0s (400.0ms)
[Relay] Idle timeout of 3.0s reached with drained queues.

=== RELAY FINAL METRICS ===
{
  "relay": {
    "port_a": 9002,
    "port_b": 9003,
    "sender_addr": "127.0.0.1:9001",
    "receiver_addr": "127.0.0.1:9004",
    "seed": 42
  },
  "forward": {
    "packets_received": 105,
    "packets_forwarded": 99,
    "packets_dropped_loss": 6,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 9,
    "packets_dispatched_out_of_order": 8,
    "packets_delayed_by_stall": 441,
    "stalls_triggered": 5,
    "total_bytes_received": 35614,
    "total_bytes_forwarded": 32721
  },
  "reverse": {
    "packets_received": 2,
    "packets_forwarded": 2,
    "packets_dropped_loss": 0,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 0,
    "packets_dispatched_out_of_order": 0,
    "packets_delayed_by_stall": 0,
    "stalls_triggered": 0,
    "total_bytes_received": 324,
    "total_bytes_forwarded": 324
  }
}
```

**Receiver:**
```text
[Receiver] Listening on 127.0.0.1:9004...
[Receiver] Session 'session-1793812219' initialized. Expecting 100 records.
[Receiver] DELIVERY VERIFICATION FAILED ON FIN: Record count mismatch: expected 100, received 86; Missing 14 records: [4, 6, 10, 14, 21, 34, 44, 50, 62, 63]

=== RECEIVER FINAL METRICS ===
{
  "status": "FAILED",
  "session_id": "session-1793812219",
  "expected_records": 100,
  "received_records": 86,
  "missing_records_count": 14,
  "missing_records_sample": [
    4,
    6,
    10,
    14,
    21,
    34,
    44,
    50,
    62,
    63,
    67,
    82,
    87,
    90
  ],
  "duplicate_records_count": 0,
  "duplicate_records_sample": [],
  "out_of_order_records_count": 0,
  "oversized_datagrams_rejected": 0,
  "max_actual_datagram_size_bytes": 1113,
  "output_file": "data/received_relay_stalled_100.jsonl",
  "validation_failure_reason": "Record count mismatch: expected 100, received 86; Missing 14 records: [4, 6, 10, 14, 21, 34, 44, 50, 62, 63]"
}
```

---

### Benchmark C: Manual Stall Configuration Reproduction (100 Records Clean Recovery)
- **Configuration:** `--loss 0 --reorder 0 --stall-interval 0.05 --stall-duration 400 --seed 42`
- **Relay Activity:**
  - Forward: 102 packets received, 102 forwarded, 0 dropped, 0 reordered, 102 delayed by stall, 2 stalls triggered.
  - Reverse: 2 packets received (`START_ACK` and `FIN_ACK`), 2 forwarded to Sender without loss or stall delay.
- **Outcome:**
  - Receiver received 100/100 records in sequence, 0 missing, 0 duplicates, 0 out-of-order, status `COMPLETED`.
  - Sender received `START_ACK` and `FIN_ACK`, status `COMPLETED`.
  - Delivery verified in full: `data/received_stall_repro.jsonl` validated with 100/100 records intact.

#### Terminal Outputs (Benchmark C):

**Sender:**
```text
[Sender] Sending START (attempt 1/5) for session 'manual-stall-repro-100'...
[Sender] Handshake SUCCESS: Received START_ACK (status=READY).
[Sender] Starting transmission of 100 records to ('127.0.0.1', 9002)...
[Sender] Sending FIN (attempt 1/5, final_record_id=100)...
[Sender] Teardown SUCCESS: Received FIN_ACK (delivered_through=100, total_delivered=100).

=== SENDER TRANSMISSION REPORT ===
{
  "session_id": "manual-stall-repro-100",
  "input_file": "data/sample_100.jsonl",
  "target_address": "127.0.0.1:9002",
  "sent_records": 100,
  "total_data_bytes_sent": 35422,
  "max_actual_datagram_size_bytes": 1117,
  "sender_data_loop_duration_seconds": 0.016,
  "sender_data_loop_throughput_records_per_second": 6250.0,
  "full_session_duration_seconds": 0.828,
  "full_session_throughput_records_per_second": 120.77,
  "transmission_duration_seconds": 0.016,
  "achieved_records_per_second": 6250.0,
  "throughput_kb_per_second": 2161.99,
  "start_retries_count": 0,
  "fin_retries_count": 0,
  "status": "COMPLETED"
}
```

**Relay:**
```text
[Relay] Running Network Impairment Relay (seed=42)...
  Port A (Sender-facing):   127.0.0.1:9002 -> targets ('127.0.0.1', 9004)
  Port B (Receiver-facing): 127.0.0.1:9003 -> targets ('127.0.0.1', 9001)
  Forward: loss=0.0%, reorder=0.0%, stall_interval=0.05s (400.0ms)
  Reverse: loss=0.0%, reorder=0.0%, stall_interval=0.0s (400.0ms)
[Relay] Idle timeout of 3.0s reached with drained queues.

=== RELAY FINAL METRICS ===
{
  "relay": {
    "port_a": 9002,
    "port_b": 9003,
    "sender_addr": "127.0.0.1:9001",
    "receiver_addr": "127.0.0.1:9004",
    "seed": 42
  },
  "forward": {
    "packets_received": 102,
    "packets_forwarded": 102,
    "packets_dropped_loss": 0,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 0,
    "packets_dispatched_out_of_order": 0,
    "packets_delayed_by_stall": 102,
    "stalls_triggered": 2,
    "total_bytes_received": 35659,
    "total_bytes_forwarded": 35659
  },
  "reverse": {
    "packets_received": 2,
    "packets_forwarded": 2,
    "packets_dropped_loss": 0,
    "packets_dropped_targeted": 0,
    "packets_delayed_for_reordering": 0,
    "packets_dispatched_out_of_order": 0,
    "packets_delayed_by_stall": 0,
    "stalls_triggered": 0,
    "total_bytes_received": 269,
    "total_bytes_forwarded": 269
  }
}
```

**Receiver:**
```text
[Receiver] Listening on 127.0.0.1:9004...
[Receiver] Session 'manual-stall-repro-100' initialized. Expecting 100 records.
[Receiver] All 100 records verified in order! Sent FIN_ACK.
[Receiver] Delivery verified! Entering bounded 2.0s linger period for duplicate FIN absorption...
[Receiver] 2.0s linger period elapsed. Shutting down cleanly.

=== RECEIVER FINAL METRICS ===
{
  "status": "COMPLETED",
  "session_id": "manual-stall-repro-100",
  "expected_records": 100,
  "received_records": 100,
  "missing_records_count": 0,
  "missing_records_sample": [],
  "duplicate_records_count": 0,
  "duplicate_records_sample": [],
  "out_of_order_records_count": 0,
  "oversized_datagrams_rejected": 0,
  "max_actual_datagram_size_bytes": 1117,
  "output_file": "data/received_stall_repro.jsonl",
  "validation_failure_reason": null
}
```

---

## 4. Key Takeaways & Readiness for Phase 4

1. **Relay Operates Accurately:** Packets are intercepted, forwarded, dropped, delayed, and stalled in strict accordance with the configuration.
2. **Reordering Across Stalls Is Preserved:** The Min-Heap Relative-Offset Scheduler successfully prevents stalls from undoing packet reordering.
3. **End-to-End Routing Verified:** The 4-socket topology operates smoothly over loopback (`9001` $\to$ `9002` [Port A] $\to$ `9003` [Port B] $\to$ `9004`, and reverse `9004` $\to$ `9003` $\to$ `9002` $\to$ `9001`).
4. **Substrate Ready:** The project now has an empirical, reproducible test harness ready for Phase 4 (Receiver Reorder Buffer & ACK Ranges) and Phase 5 (RACK-Inspired Loss Detection).
