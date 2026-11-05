# TAR (Time-Aware Reliable Transport)

TAR is an experimental, single-stream, reliable, ordered transport protocol operating over UDP datagrams with compact UTF-8 JSON wire framing. It incorporates mechanisms inspired by RACK-TLP (RFC 8985) and QUIC loss detection (RFC 9002).

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
├── AI_CONVERSATION_TRANSCRIPT.md          # Complete AI pair-programming conversation history (31 turns)
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
