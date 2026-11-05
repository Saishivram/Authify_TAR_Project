# Phase 5 — Control Server and Dashboard Verification Report

**Project:** TAR (Time-Aware Reliable Transport)  
**Phase:** 5 (Control Server & Web Dashboard)  
**Date:** November 2026  
**Status:** **100% PASSED & VERIFIED**  
**Test Suite Status:** 128 / 128 tests passing (`OK`)

---

## 1. Executive Summary

Phase 5 introduces a lightweight, high-performance web-based control server and monitoring dashboard for the TAR protocol. Built on **FastAPI** and the Python standard library, it eliminates the need to manually juggle multiple terminal windows when evaluating the transport system.

The dashboard adheres strictly to the design specification:
- **Clean, Light-Coloured Interface:** Neutral light grey/slate background (`#f8fafc`), clean white card containers (`#ffffff`), subtle borders (`#e2e8f0`), and crisp blue accents (`#2563eb`). It explicitly avoids dark themes, heavy animations, and rainbow gradients.
- **Process Orchestration:** Non-blocking lifecycle management for the `Receiver` (Port 9004), `Impairment Relay` (Ports 9002/9003), and `Sender` (Port 9001). Components can be started and stopped individually or executed as coordinated end-to-end sessions (from 100 to 20,000 records).
- **Dynamic Network Impairments:** Sliders and numerical inputs allow operators to tune forward packet loss (0–25%), reordering (0–15%), link stall duration (0–1000 ms), stall interval (0–10 s), and reverse ACK loss (0–20%). Settings apply live on-the-fly to the running relay forward/reverse schedulers.
- **Real-Time WebSocket Telemetry:** Live 200 ms push stream delivering instantaneous metrics on ordered delivery progress, throughput, in-flight transmissions, packet drops, out-of-order dispatches, retransmissions, PTO probes, and maximum empirical delivery gap ($\Delta t_{delivery}^{max}$).
- **Event History & Human Attention:** A filterable, timestamped protocol milestone log and a dedicated operator attention banner for anomalies.

All 115 previous tests across Phases 1–4 continue to pass with zero regressions, and 13 new tests validate the server and manager, bringing the suite to **128 passing tests**.

---

## 2. Architecture & Implementation

### 2.1 Backend Modules (`server/`)

```text
server/
├── __init__.py               # Package marker
├── app.py                    # FastAPI application, REST endpoints, WebSocket streaming, static mounts
├── manager.py                # SystemManager: thread lifecycle, telemetry polling, live config updates
└── static/                   # Pure vanilla frontend assets
    ├── index.html            # Dashboard layout and accessible card structure
    ├── styles.css            # Light-theme styling (#f8fafc / #2563eb / #ffffff)
    └── app.js                # Auto-reconnecting WebSocket client and DOM updater
```

### 2.2 SystemManager Capabilities (`server/manager.py`)

1. **Component Thread Management:**
   - Runs `UdpReceiver`, `ImpairmentRelay`, and `UdpSender` inside daemon background threads with clean cooperative cancellation (`stop()`) and thread joins (`join(timeout=0.5)`).
   - Tracks explicit state machines per component: `STOPPED`, `RUNNING`, `COMPLETED`, `FAILED`.
2. **Live Telemetry & Anomaly Detection:**
   - Background poller samples metrics at 150 ms intervals.
   - Detects delivery gap SLA violations ($\Delta t_{delivery}^{max} > 1.0\text{s}$) and pushes notifications to the Operator Attention queue.
   - Automatically detects packet loss, reorder events, and retransmission surges, logging structured events.
3. **Dynamic Network Reconfiguration:**
   - Modifies `relay.forward_scheduler.config` and `relay.reverse_scheduler.config` on-the-fly without restarting the relay process.
4. **Event History Ring Buffer:**
   - Retains the last 500 timestamped events across categories: `HANDSHAKE`, `DATA`, `RETRANSMISSION`, `PACKET_LOSS`, `RECOVERY`, `COMPLETION`, `CONFIG`, `SYSTEM`, `ERROR`.

### 2.3 REST API & WebSocket Endpoints (`server/app.py`)

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/` | `GET` | Serves the light-themed web dashboard (`index.html`) |
| `/api/status` | `GET` | Returns full telemetry snapshot (components, delivery, network stats, active config, attention items, recent events) |
| `/api/start` | `POST` | Starts individual components or an orchestrated full session |
| `/api/stop` | `POST` | Stops individual components or all running components cleanly |
| `/api/configure` | `POST` | Dynamically updates network impairments live |
| `/api/events` | `GET` | Retrieves recent event history logs with optional limit/category filters |
| `/api/events/clear` | `POST` | Clears event history logs |
| `/api/attention/dismiss` | `POST` | Dismisses an operator attention alert by ID |
| `/api/datasets` | `GET` | Lists available `.jsonl` datasets in `data/` |
| `/ws` | `WebSocket` | Real-time 200 ms push stream delivering live JSON telemetry snapshots |

---

## 3. Web Dashboard Sections

1. **Top Header:** System title, connection status badge with pulsing status dot (`Connected` / `Reconnecting...`), and global quick actions (`Start Session`, `Stop All`).
2. **Human Attention Banner:** Highlights anomalies requiring operator review (e.g. delivery gap SLA violations, component crashes, extreme loss rates). Displays a subtle green "System Normal" badge when operating nominally.
3. **System Controls:**
   - Independent status rows and Start/Stop buttons for Receiver (9004), Relay (9002/9003), and Sender (9001).
   - Coordinated run launcher: selector for 100, 2,000, 5,000, 10,000, or 20,000 records, window size, and pacing interval.
4. **Network Conditions (Relay Impairments):**
   - Synchronized sliders and numeric labels for Forward Loss (0–25%), Forward Reordering (0–15%), Stall Duration (0–1000 ms), Stall Interval (0–10 s), and Reverse ACK Loss (0–20%).
   - "Apply Network Conditions" button with instant visual feedback and active configuration summary.
5. **Delivery Monitoring:**
   - 4 large metric tiles: Expected Records, Delivered In-Order, Sent Datagrams, Pending Records.
   - Ordered delivery progress bar with percentage and animated width transition.
   - Throughput (rec/s), in-flight transmission counter, and receiver gap reorder buffer count.
6. **Live Network Statistics:**
   - Metrics grid: Forward Packets Dropped, Forward Packets Reordered, Reverse ACKs Sent/Dropped, Retransmissions Dispatched, PTO Probes Sent, Max Delivery Gap ($\Delta t_{delivery}^{max}$), Smoothed RTT / RTO, and Wire Datagram MTU enforcement ($\le 1200$ bytes).
7. **Event History:**
   - Filter pills (`All`, `Handshake`, `Repairs`, `Drops`, `Errors`) and a `Clear Log` button.
   - Monospace table displaying Timestamp, Source Pill (Sender/Receiver/Relay), Type Badge, and Event Message.

---

## 4. Test Verification & Results

### 4.1 Server Test Suite (`tests/test_server.py`)

A dedicated suite of 13 tests was created to verify all Phase 5 functionality:

```text
test_attention_dismiss_endpoint ................................... ok
test_configure_network_endpoint ................................... ok
test_events_and_clear_endpoint .................................... ok
test_get_datasets_endpoint ........................................ ok
test_get_status_endpoint .......................................... ok
test_static_dashboard_endpoint .................................... ok
test_websocket_telemetry_streaming ................................ ok
test_manager_attention_management ................................. ok
test_manager_dynamic_configuration ................................ ok
test_manager_event_logging_and_filtering .......................... ok
test_manager_initialization ....................................... ok
test_manager_receiver_relay_lifecycle ............................. ok
test_manager_start_all_e2e ........................................ ok

----------------------------------------------------------------------
Ran 13 tests in 3.312s. OK.
```

### 4.2 Complete Project Test Suite

The entire 128-test suite was executed across all phases:

```powershell
python -m unittest discover -v tests
```

**Results:**
- `test_record_schema.py`: 18 tests `OK`
- `test_record_generator.py`: 10 tests `OK`
- `test_packet.py`: 21 tests `OK`
- `test_sack.py`: 9 tests `OK`
- `test_reorder_buffer.py`: 5 tests `OK`
- `test_rtt.py`: 7 tests `OK`
- `test_recovery.py`: 8 tests `OK`
- `test_reliability_e2e.py`: 18 tests `OK`
- `test_udp_transport.py`: 5 tests `OK`
- `test_relay.py`: 14 tests `OK`
- `test_server.py`: 13 tests `OK`

**Final Total:**
```text
----------------------------------------------------------------------
Ran 128 tests in 36.228s

OK
```

---

## 5. Startup & Usage Instructions

### Starting the Dashboard

```powershell
# Option A: Python module
python -m server.app

# Option B: Direct Uvicorn CLI
uvicorn server.app:app --host 127.0.0.1 --port 8000
```

### Accessing the Web Interface

Open your browser to:
```
http://localhost:8000/
```

### Quick Workflow
1. Select the desired dataset (e.g., `2,000 Records`).
2. Adjust the impairment sliders (e.g., 5% loss, 2% reorder, 400 ms stall) and click **Apply Network Conditions**.
3. Click **Launch Complete Transport Session** or use the topbar **Start Session** button.
4. Watch the real-time delivery progress bar fill, observe retransmissions repairing simulated drops, and inspect live throughput.
5. Review the timestamped event history table for detailed protocol state transitions.
