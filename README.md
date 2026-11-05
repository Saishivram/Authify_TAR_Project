# TAR (Time-Aware Reliable Transport)

TAR is an experimental, single-stream, reliable, ordered transport protocol operating over UDP datagrams with compact UTF-8 JSON wire framing. It incorporates mechanisms inspired by RACK-TLP (RFC 8985) and QUIC loss detection (RFC 9002).

---

## Current Project Status

- **Phase 1: Record Generator & Schema Validation** `[COMPLETED]`
  - Synthetic dataset generator producing deterministic sequential logical records.
  - Edge cases: normal, empty (0-byte value), multi-byte Unicode scripts/emojis, large (400–800 bytes), and boundary (~996 bytes) records.
  - Three-metric byte accounting (`value_bytes`, `serialized_logical_payload_bytes`, `estimated_complete_data_datagram_bytes`).
  - Standalone 100-record individual inspection evidence.
- **Phase 2: Basic UDP Sender–Receiver Communication** `[COMPLETED]`
  - Independent `Sender` and `Receiver` processes using Python standard-library UDP sockets.
  - Strict measurement and enforcement of the 1,200-byte wire datagram ceiling using `recvfrom(65535)` to receive oversized datagrams whole for rejection, and `SO_RCVBUF` to prevent OS queue drops during bursts.
  - Idempotent `START` $\to$ `START_ACK` session handshake with bounded retries (5x, 500 ms).
  - Sequential, single-attempt DATA transmission (100 and 2,000 records).
  - Strict pre-`FIN_ACK` delivery verification: validates total record count, sequential unique IDs, and zero gaps/duplicates before acknowledging `FIN`. Incomplete delivery is rejected and never acknowledged.
  - Idempotent `FIN` teardown with bounded 2.0-second linger period: duplicate `FIN` triggers re-transmission of cached `FIN_ACK`, followed by clean shutdown.
  - Separate metric accounting: distinct sender DATA-loop throughput vs full end-to-end session throughput; sent, received, missing, duplicate, and out-of-order records.
- **Phase 3: Network Impairment Relay** `[COMPLETED]`
  - Independent bi-directional proxy process intercepting traffic on Port A (`127.0.0.1:9002`) and Port B (`127.0.0.1:9003`).
  - 100% raw byte preservation without deserializing, mutating, or re-encoding protocol envelopes.
  - Completely decoupled forward and reverse `DirectionScheduler` engines: stalls or delays in one direction never block traffic in the other.
  - Min-Heap Relative-Offset Scheduler with `boundary_offset` tracking that preserves true packet reordering across 400 ms stall boundaries. Overlapping stalls cleanly extend the pause window without double-counting remaining duration.
  - Rate-based time-interval stalls (default 5.0s interval, 400 ms duration; 0 to disable) preventing stall storms at high transmission rates.
  - Session-scoped out-of-order metrics tracking sequence inversions strictly per `(connection_id, direction)` across monotonic `packet_id`s (`DATA`, `FIN`).
  - Deterministic random seeds (`--seed`) and targeted fault injection (`TargetedRule`).
- **Phase 4: Reliability and Recovery Engine** `[COMPLETED]`
  - Dual-watermark SACK Tracker (`transport/sack.py`): tracks `ack_received_through_packet_id` (contiguous prefix) and `ack_range_floor_packet_id` (retained floor), bounding history to <= 32 ranges.
  - Reorder Buffer (`transport/reorder_buffer.py`): in-order pass-through invariant ($record\_id == next\_expected\_record$ bypasses full capacity), cascading drain, and duplicate suppression.
  - RFC 6298 RTT Estimator & Adaptive Multiplier (`transport/rtt.py`): Karn's unambiguous sampling, bounded ACK delay, adaptive reordering multiplier ($1.0\times$ to $4.0\times$), and 16-flight clean decay engine.
  - Recovery Coordinator (`transport/recovery.py`): centralized per-record 7-state machine (`QUEUED`, `IN_FLIGHT`, `BUFFERED_AT_RECEIVER`, `REPAIR_PENDING`, `REPAIR_IN_FLIGHT`, `DELIVERED`, `FAILED`), feedback-driven repair cancellation, RACK loss detection, separate PTO probing, and fallback exponential RTO backoff.
  - Gap-Aware FIN Lifecycle Teardown: withholds `FIN_ACK` upon delivery gaps and sends SACK to trigger sender repairs until complete delivery is verified.
  - Automated E2E Reliability Suite (`tests/test_reliability_e2e.py`): 18 comprehensive integration scenarios (T01 through T18) passing over loopback sockets.
  - Scaling Benchmarks (`benchmarks/run_phase4_benchmarks.py`): evaluated from 2,000 to 20,000 records across 9 scenarios (B01-B09); achieved up to 10,576 rec/s clean throughput and verified $\Delta t_{delivery}^{max} \le 1.0\text{s}$ stall bound across all impaired scenarios.
- **Phase 5: Control Server & Web Dashboard** `[COMPLETED]`
  - FastAPI Control Server (`server/app.py`): REST API and WebSocket streaming server orchestrating Sender, Receiver, and Relay without manual terminal switching.
  - Process & System Manager (`server/manager.py`): Non-blocking threaded component lifecycle management, live metric aggregation, dynamic on-the-fly network impairment reconfiguration, and operator attention alert management.
  - Clean Light-Themed Web Dashboard (`server/static/`): Strictly light-coloured aesthetic (`#f8fafc` background, `#ffffff` cards, `#2563eb` subtle blue accents, readable dark typography), featuring System Controls, Delivery Monitoring with animated progress bar, Network Conditions sliders, Live Network Statistics, and a filterable Event History stream.
  - Automated Tests: 128 tests passing (`Ran 128 tests in 36.836s. OK`).
- **Phase 6: Final Integration & Submission Preparation** `[COMPLETED]`
  - Clean-Machine Single Startup Command: Entire system orchestratable via single command (`python -m server.app`).
  - Zero External Infrastructure: 100% self-contained standard library + FastAPI; no Docker, Redis, Kafka, or external databases.
  - Video Demonstration Script & Checklist (`reports/demo_script.md`): Structured 7–9 minute presentation script covering all 9 required evaluation points.
  - Complete End-to-End Verification: Tested live 2,000-record transfer with 5% loss, 2% reorder, and 400ms stall via dashboard; verified 100% ordered delivery with max gap 0.157s.

---

## Project Structure

```text
s:/TAR_Project/
│
├── ALGORITHM_SPEC.md                      # Core algorithm and recovery specification
├── PACKET_FORMAT.md                       # Wire format, packet envelope, and datagram specs
├── DECISIONS.md                           # Architecture choices, trade-offs, and decision log
├── TEST_PLAN.md                           # Verification methodology and test matrix (T00-T18)
├── README.md                              # Project overview, architecture, and commands
│
├── schemas/                               # Phase 1: Record schema & validation
│   ├── __init__.py
│   └── record_schema.py                   # Schema validators, byte helpers, MTU constants
│
├── generator/                             # Phase 1: Record generator & inspector
│   ├── __init__.py
│   ├── record_generator.py                # Deterministic synthetic record generator
│   └── inspector.py                       # 3-metric statistical inspector and report generator
│
├── transport/                             # Phase 2, 3, & 4: Transport, relay & recovery modules
│   ├── __init__.py                        # Exports all packet types, coordinators, and processes
│   ├── packet.py                          # Wire framing, AckPacket, ProbePacket, ResetPacket, MTU limit
│   ├── sack.py                            # SackTracker, dual watermarks, range retirement (<32)
│   ├── reorder_buffer.py                  # ReorderBuffer, in-order pass-through, cascading drain
│   ├── rtt.py                             # RttEstimator (RFC 6298), PTO tracker, adaptive multiplier
│   ├── recovery.py                        # RecoveryCoordinator, 7-state machine, RACK, fallback RTO
│   ├── sender.py                          # UdpSender with windowing, pacing, and recovery loop
│   ├── receiver.py                        # UdpReceiver with SACK feedback and gap-aware FIN repair
│   └── relay.py                           # DirectionScheduler, ImpairmentRelay, min-heap scheduler
│
├── server/                                # Phase 5: Control Server & Web Dashboard
│   ├── __init__.py
│   ├── app.py                             # FastAPI server, REST API, WebSocket streaming
│   ├── manager.py                         # Process & SystemManager, telemetry, dynamic impairments
│   └── static/                            # Clean, light-themed web interface
│       ├── index.html                     # Responsive UI layout & control panels
│       ├── styles.css                     # Light aesthetic (#f8fafc), subtle blue accents (#2563eb)
│       └── app.js                         # Live WebSocket client & interactive controls
│
├── benchmarks/                            # Performance & scaling benchmarks
│   └── run_phase4_benchmarks.py           # 3-process loopback scaling benchmark runner (2K - 20K)
│
├── sender.py                              # Top-level CLI entry point for Sender
├── receiver.py                            # Top-level CLI entry point for Receiver
├── relay.py                               # Top-level CLI entry point for Impairment Relay
│
├── data/                                  # Canonical datasets and received outputs
│   ├── sample_100.jsonl                   # 100-record dataset (seed=42)
│   ├── records_2000.jsonl                 # 2,000-record dataset (seed=42)
│   ├── records_5000.jsonl                 # 5,000-record dataset (seed=42)
│   ├── records_10000.jsonl                # 10,000-record dataset (seed=42)
│   ├── records_20000.jsonl                # 20,000-record dataset (seed=42)
│   ├── received_100.jsonl                 # Direct received 100-record output
│   ├── received_2000.jsonl                # Direct received 2,000-record output
│   └── received_bench_*.jsonl             # Benchmark outputs across scaling profiles
│
├── reports/                               # Inspection, transmission, & reliability reports
│   ├── first_100_records_inspection.md    # Dedicated 100-record table evidence
│   ├── phase2_transmission_report.md      # Direct transmission benchmark report
│   ├── phase3_relay_report.md             # Phase 3 relay benchmark & verification report
│   ├── phase4_reliability_report.md       # Phase 4 reliability & scaling benchmark report
│   ├── phase4_reliability_report.json     # Phase 4 machine-readable scaling metrics
│   ├── phase5_dashboard_report.md         # Phase 5 dashboard & control server report
│   └── demo_script.md                     # Video demonstration script & evaluation checklist
│
└── tests/                                 # Automated test suite (128 passing tests)
    ├── __init__.py
    ├── test_record_schema.py              # Schema, stream, encoding, and ordering tests (18 tests)
    ├── test_record_generator.py           # Generator determinism, edge-case tests (10 tests)
    ├── test_packet.py                     # Packet serialization, validation, SACK ranges (21 tests)
    ├── test_sack.py                       # SACK tracker, dual watermarks, 32-range floor (9 tests)
    ├── test_reorder_buffer.py             # ReorderBuffer cascading flush, pass-through (5 tests)
    ├── test_rtt.py                        # RFC 6298 RTT estimator, PTO, adaptive decay (7 tests)
    ├── test_recovery.py                   # 7-state coordinator, RACK, fallback RTO (8 tests)
    ├── test_reliability_e2e.py            # End-to-end reliability integration suite T01-T18 (18 tests)
    ├── test_udp_transport.py              # Phase 2 baseline regression (5 tests)
    ├── test_relay.py                      # Phase 3 relay unit and integration tests (14 tests)
    └── test_server.py                     # Phase 5 SystemManager & FastAPI endpoints (13 tests)
```

---

## Port Topology

| Component | Default Address | Function |
| :--- | :--- | :--- |
| **Sender** | `127.0.0.1:9001` | Reads dataset, transmits START/DATA/PROBE/FIN packets |
| **Relay (Port A)** | `127.0.0.1:9002` | Sender-facing socket for network impairments |
| **Relay (Port B)** | `127.0.0.1:9003` | Receiver-facing socket for network impairments |
| **Receiver** | `127.0.0.1:9004` | Reorders packets, generates SACK, delivers strictly in order |

*Forward Path:* Sender (`9001`) $\to$ Relay Port A (`9002`) $\to$ Relay Port B (`9003`) $\to$ Receiver (`9004`).  
*Reverse Path:* Receiver (`9004`) $\to$ Relay Port B (`9003`) $\to$ Relay Port A (`9002`) $\to$ Sender (`9001`).

---

## Clean-Machine Setup & Quickstart

TAR is designed to run on any clean machine with Python 3.10+ installed. It requires zero external infrastructure (no databases, Redis, Kafka, or Docker containers).

### Prerequisites
- **Python**: 3.10, 3.11, or 3.12 (standard library supplies UDP networking, threading, and JSON).
- **Dependencies**: Only `fastapi`, `uvicorn`, and `websockets` for the Phase 5 dashboard:
  ```powershell
  pip install fastapi uvicorn websockets
  ```

### Single Startup Command (Dashboard & Control Server)
To start the control server and access the web dashboard:
```powershell
python -m server.app
```
Then navigate to **`http://localhost:8000/`** in your browser. From the UI, click **"Start Session"** or **"Launch Complete Transport Session"** to execute a full 2,000-record transfer across the impaired relay with live telemetry streaming.

---

## Windows PowerShell Commands

### 1. Run Complete Automated Test Suite (128 Tests)
```powershell
python -m unittest discover -v tests
```

### 2. Launch Phase 5 Web Dashboard & Control Server
```powershell
python -m server.app
# or
uvicorn server.app:app --host 127.0.0.1 --port 8000
```
Open **http://localhost:8000/** in your browser to access the live dashboard.

### 3. Run Phase 4 End-to-End Reliability Suite (T01–T18)
```powershell
python -m unittest -v tests.test_reliability_e2e
```

### 4. Run Phase 4 Performance Scaling Benchmarks (2K to 20K Records)
```powershell
python benchmarks/run_phase4_benchmarks.py
```

### 5. Three-Process Run Through Impairment Relay (Standalone Terminals)

**Terminal 1 (Start Receiver on Port 9004 with gap-aware FIN repair):**
```powershell
python receiver.py --port 9004 --output data/received_relay.jsonl --enable-fin-repair
```

**Terminal 2 (Start Relay on Ports 9002 & 9003 with 5% loss, 2% reorder, 400ms stall):**
```powershell
python relay.py --loss 0.05 --reorder 0.02 --stall-interval 5.0 --stall-duration 400 --seed 42
```
*(For clean forwarding with zero impairments: `python relay.py --loss 0.0 --reorder 0.0 --no-stall`)*

**Terminal 3 (Start Sender targeting Relay Port 9002 with 200 send window):**
```powershell
python sender.py --input data/records_2000.jsonl --target-port 9002 --bind-port 9001 --window 200
```

**Terminal 3 (Validate Received Output):**
```powershell
python -m schemas.record_schema data/received_relay.jsonl 2000
```
