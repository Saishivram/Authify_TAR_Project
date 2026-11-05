"""
server/app.py
=============
FastAPI Control Server for the TAR Protocol (Phase 5).

Provides:
- REST API for controlling Receiver, Relay, and Sender processes.
- Dynamic network impairment configuration endpoints.
- WebSocket stream broadcasting live telemetry every 200ms to frontend clients.
- Static file serving for the clean light-themed web dashboard.
"""

import asyncio
from contextlib import asynccontextmanager
import glob
import json
import os
import sys
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Ensure project root is on path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from server.manager import SystemManager

# Global SystemManager singleton
manager: SystemManager = SystemManager()
connected_websockets: Set[WebSocket] = set()
broadcast_task: Optional[asyncio.Task] = None


async def telemetry_broadcaster():
    """Periodically broadcasts live telemetry snapshots to all connected WebSocket clients."""
    while True:
        try:
            if connected_websockets and manager:
                snapshot = manager.get_status_snapshot()
                msg = json.dumps(snapshot)
                stale_sockets = set()
                for ws in list(connected_websockets):
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        stale_sockets.add(ws)

                for stale in stale_sockets:
                    connected_websockets.discard(stale)

            await asyncio.sleep(0.2)  # 200ms update frequency
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages telemetry broadcaster on startup and cleans up on shutdown."""
    global broadcast_task
    broadcast_task = asyncio.create_task(telemetry_broadcaster())
    yield
    if broadcast_task:
        broadcast_task.cancel()
    if manager:
        manager.close()

app = FastAPI(
    title="TAR Protocol Control Server",
    description="Control server and real-time telemetry streaming for TAR transport",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Request & Response Schemas
# -----------------------------------------------------------------------------


class StartRequest(BaseModel):
    component: str = Field("all", description="'all', 'receiver', 'relay', or 'sender'")
    record_count: int = Field(2000, description="Record count for session")
    dataset_path: Optional[str] = Field(None, description="Path to input JSONL file")
    send_window: int = Field(200, description="Sender window limit")
    pace_interval: float = Field(0.0, description="Sender pacing interval in seconds")


class StopRequest(BaseModel):
    component: str = Field("all", description="'all', 'receiver', 'relay', or 'sender'")


class ConfigureRequest(BaseModel):
    loss_rate: Optional[float] = Field(None, ge=0.0, le=1.0, description="Forward loss rate [0.0 - 1.0]")
    reorder_rate: Optional[float] = Field(None, ge=0.0, le=1.0, description="Forward reorder rate [0.0 - 1.0]")
    stall_duration_ms: Optional[float] = Field(None, ge=0.0, description="Stall duration in ms")
    stall_interval_sec: Optional[float] = Field(None, ge=0.0, description="Stall interval in sec (0 to disable)")
    reverse_loss_rate: Optional[float] = Field(None, ge=0.0, le=1.0, description="Reverse ACK loss rate [0.0 - 1.0]")


class DismissAttentionRequest(BaseModel):
    id: str = Field(..., description="ID of the attention alert to dismiss")


# -----------------------------------------------------------------------------
# REST API Endpoints
# -----------------------------------------------------------------------------


@app.get("/api/status")
async def get_status():
    """Returns full live telemetry snapshot of all components and metrics."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    return manager.get_status_snapshot()


@app.post("/api/start")
async def start_component(req: StartRequest):
    """Starts individual transport components or an orchestrated full session."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")

    comp = req.component.lower()
    if comp == "all":
        ok = manager.start_all(
            record_count=req.record_count,
            dataset_path=req.dataset_path,
            send_window=req.send_window,
            pace_interval=req.pace_interval,
        )
        return {"status": "ok" if ok else "error", "message": f"Started all components ({req.record_count} records)"}
    elif comp == "receiver":
        ok = manager.start_receiver()
        return {"status": "ok" if ok else "error", "message": "Started Receiver on port 9004"}
    elif comp == "relay":
        ok = manager.start_relay()
        return {"status": "ok" if ok else "error", "message": "Started Relay on ports 9002/9003"}
    elif comp == "sender":
        dataset = req.dataset_path or f"data/records_{req.record_count}.jsonl"
        ok = manager.start_sender(
            input_file=dataset,
            send_window=req.send_window,
            pace_interval=req.pace_interval,
        )
        return {"status": "ok" if ok else "error", "message": f"Started Sender ({dataset})"}
    else:
        raise HTTPException(status_code=400, detail=f"Unknown component: {req.component}")


@app.post("/api/stop")
async def stop_component(req: StopRequest):
    """Stops individual transport components or all components."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")

    comp = req.component.lower()
    if comp == "all":
        manager.stop_all()
        return {"status": "ok", "message": "Stopped all components"}
    elif comp == "receiver":
        manager.stop_receiver()
        return {"status": "ok", "message": "Stopped Receiver"}
    elif comp == "relay":
        manager.stop_relay()
        return {"status": "ok", "message": "Stopped Relay"}
    elif comp == "sender":
        manager.stop_sender()
        return {"status": "ok", "message": "Stopped Sender"}
    else:
        raise HTTPException(status_code=400, detail=f"Unknown component: {req.component}")


@app.post("/api/configure")
async def configure_network(req: ConfigureRequest):
    """Updates network impairments dynamically; applies immediately to running relay."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")

    updated = manager.configure_network(
        loss_rate=req.loss_rate,
        reorder_rate=req.reorder_rate,
        stall_duration_ms=req.stall_duration_ms,
        stall_interval_sec=req.stall_interval_sec,
        reverse_loss_rate=req.reverse_loss_rate,
    )
    return {"status": "ok", "config": updated}


@app.get("/api/events")
async def get_events(limit: int = Query(50, ge=1, le=500), category: Optional[str] = Query(None)):
    """Retrieves recent event history logs."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    return {"events": manager.get_events(limit=limit, category=category)}


@app.post("/api/events/clear")
async def clear_events():
    """Clears all event history logs."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    manager.clear_events()
    return {"status": "ok", "message": "Event history cleared"}


@app.post("/api/attention/dismiss")
async def dismiss_attention(req: DismissAttentionRequest):
    """Dismisses an operator attention alert."""
    if not manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    found = manager.dismiss_attention_item(req.id)
    return {"status": "ok" if found else "not_found"}


@app.get("/api/datasets")
async def list_datasets():
    """Lists available input datasets in the data directory."""
    data_dir = os.path.join(PROJECT_ROOT, "data")
    datasets = []
    if os.path.exists(data_dir):
        for filepath in glob.glob(os.path.join(data_dir, "*.jsonl")):
            filename = os.path.basename(filepath)
            if not filename.startswith("received_"):
                # Count lines
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        count = sum(1 for _ in f)
                    datasets.append({
                        "filename": filename,
                        "path": f"data/{filename}",
                        "record_count": count,
                        "size_bytes": os.path.getsize(filepath),
                    })
                except Exception:
                    pass
    datasets.sort(key=lambda d: d["record_count"])
    return {"datasets": datasets}


# -----------------------------------------------------------------------------
# WebSocket Live Streaming
# -----------------------------------------------------------------------------


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time telemetry streaming (200ms push)."""
    await websocket.accept()
    connected_websockets.add(websocket)
    try:
        # Send initial state immediately
        if manager:
            await websocket.send_text(json.dumps(manager.get_status_snapshot()))

        while True:
            # Keep connection alive; accept client messages if any
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        connected_websockets.discard(websocket)
    except Exception:
        connected_websockets.discard(websocket)


# -----------------------------------------------------------------------------
# Frontend Static Files Serving
# -----------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serves the main dashboard user interface."""
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return HTMLResponse("<h1>TAR Dashboard</h1><p>index.html not found in server/static/</p>")


def main():
    """Runs the FastAPI server with Uvicorn."""
    import uvicorn
    uvicorn.run("server.app:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
