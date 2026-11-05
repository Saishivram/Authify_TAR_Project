"""
tests/test_server.py
====================
Unit and integration test suite for the TAR Control Server and SystemManager (Phase 5).
Tests process lifecycle management, telemetry aggregation, dynamic network configuration,
event logging, operator attention alerts, and FastAPI REST/WebSocket endpoints.
"""

import json
import os
import sys
import time
import unittest

from fastapi.testclient import TestClient

from server.app import app, manager as global_mgr
from server.manager import SystemManager


class TestSystemManager(unittest.TestCase):
    """Tests the SystemManager process orchestration and telemetry engine."""

    def setUp(self):
        self.mgr = SystemManager()

    def tearDown(self):
        self.mgr.close()

    def test_manager_initialization(self):
        """Verifies initial manager state, default configuration, and clean telemetry."""
        snapshot = self.mgr.get_status_snapshot()
        self.assertIn("components", snapshot)
        self.assertIn("delivery", snapshot)
        self.assertIn("network_stats", snapshot)
        self.assertIn("config", snapshot)
        self.assertIn("human_attention", snapshot)
        self.assertIn("recent_events", snapshot)

        # Initial component states
        self.assertEqual(snapshot["components"]["receiver"]["state"], "STOPPED")
        self.assertEqual(snapshot["components"]["relay"]["state"], "STOPPED")
        self.assertEqual(snapshot["components"]["sender"]["state"], "STOPPED")

        # Initial default configuration
        cfg = snapshot["config"]
        self.assertAlmostEqual(cfg["loss_rate"], 0.05)
        self.assertAlmostEqual(cfg["reorder_rate"], 0.02)
        self.assertEqual(cfg["stall_duration_ms"], 400.0)

    def test_manager_event_logging_and_filtering(self):
        """Tests event appending, categorization, filtering, and clearing."""
        self.mgr.clear_events()
        self.assertEqual(len(self.mgr.get_events()), 1)  # Clear event recorded

        self.mgr.add_event("INFO", "SENDER", "HANDSHAKE", "Sending START packet")
        self.mgr.add_event("WARNING", "RELAY", "PACKET_LOSS", "Simulated packet drop")
        self.mgr.add_event("ERROR", "RECEIVER", "ERROR", "Socket timeout")

        all_events = self.mgr.get_events(limit=50)
        self.assertEqual(len(all_events), 4)

        # Filter by category
        handshakes = self.mgr.get_events(limit=50, category="HANDSHAKE")
        self.assertEqual(len(handshakes), 1)
        self.assertEqual(handshakes[0]["message"], "Sending START packet")

        drops = self.mgr.get_events(limit=50, category="PACKET_LOSS")
        self.assertEqual(len(drops), 1)
        self.assertEqual(drops[0]["message"], "Simulated packet drop")

        errors = self.mgr.get_events(limit=50, category="ERROR")
        self.assertEqual(len(errors), 1)

    def test_manager_attention_management(self):
        """Tests adding, retrieving, dismissing, and clearing operator attention items."""
        self.mgr.clear_attention_items()
        self.assertEqual(len(self.mgr.attention_items), 0)

        self.mgr.add_attention_item("stall_warn", "Stall SLA Exceeded", "Gap was 1.5s", "warning")
        self.mgr.add_attention_item("conn_err", "Connection Error", "Receiver unreachable", "error")

        self.assertEqual(len(self.mgr.attention_items), 2)
        snapshot = self.mgr.get_status_snapshot()
        self.assertEqual(len(snapshot["human_attention"]), 2)

        # Dismiss one
        dismissed = self.mgr.dismiss_attention_item("stall_warn")
        self.assertTrue(dismissed)
        self.assertEqual(len(self.mgr.attention_items), 1)
        self.assertIn("conn_err", self.mgr.attention_items)

        # Dismiss invalid
        self.assertFalse(self.mgr.dismiss_attention_item("non_existent"))

        # Clear remaining
        self.mgr.clear_attention_items()
        self.assertEqual(len(self.mgr.attention_items), 0)

    def test_manager_dynamic_configuration(self):
        """Tests dynamic impairment reconfiguration and high-loss attention detection."""
        # Update parameters
        new_cfg = self.mgr.configure_network(
            loss_rate=0.08,
            reorder_rate=0.04,
            stall_duration_ms=250.0,
            stall_interval_sec=3.0,
            reverse_loss_rate=0.02,
        )
        self.assertAlmostEqual(new_cfg["loss_rate"], 0.08)
        self.assertAlmostEqual(new_cfg["reorder_rate"], 0.04)
        self.assertEqual(new_cfg["stall_duration_ms"], 250.0)
        self.assertEqual(new_cfg["stall_interval_sec"], 3.0)
        self.assertAlmostEqual(new_cfg["reverse_loss_rate"], 0.02)

        # Extreme loss rate (> 0.25) should trigger an attention item
        self.mgr.configure_network(loss_rate=0.30)
        self.assertIn("high_loss", self.mgr.attention_items)

        # Restoring safe loss rate should clear the attention item
        self.mgr.configure_network(loss_rate=0.05)
        self.assertNotIn("high_loss", self.mgr.attention_items)

    def test_manager_receiver_relay_lifecycle(self):
        """Tests starting and stopping receiver and relay on independent ports."""
        test_rec_port = 29014
        test_r_port_a = 29012
        test_r_port_b = 29013

        # Start Receiver
        rec_started = self.mgr.start_receiver(port=test_rec_port, output_path="data/test_rec.jsonl")
        self.assertTrue(rec_started)
        self.assertEqual(self.mgr.receiver_state, "RUNNING")

        # Starting again while running should return False
        self.assertFalse(self.mgr.start_receiver(port=test_rec_port))

        # Start Relay
        relay_started = self.mgr.start_relay(
            port_a=test_r_port_a,
            port_b=test_r_port_b,
            sender_port=29011,
            receiver_port=test_rec_port,
        )
        self.assertTrue(relay_started)
        self.assertEqual(self.mgr.relay_state, "RUNNING")

        # Stop both
        self.mgr.stop_relay()
        self.assertEqual(self.mgr.relay_state, "STOPPED")

        self.mgr.stop_receiver()
        self.assertEqual(self.mgr.receiver_state, "STOPPED")

    def test_manager_start_all_e2e(self):
        """Tests complete coordinated start_all execution transferring 100 records."""
        # Clean channel for instant test run
        self.mgr.configure_network(loss_rate=0.0, reorder_rate=0.0, stall_interval_sec=0.0)
        started = self.mgr.start_all(
            record_count=100,
            dataset_path="data/sample_100.jsonl",
            receiver_port=29004,
            relay_port_a=29002,
            relay_port_b=29003,
            sender_port=29001,
        )
        self.assertTrue(started)

        # Wait for sender completion (typically ~100ms)
        t0 = time.monotonic()
        while self.mgr.sender_state == "RUNNING" and (time.monotonic() - t0) < 6.0:
            time.sleep(0.05)

        self.assertEqual(self.mgr.sender_state, "COMPLETED")

        # Give receiver a moment to flush and record
        time.sleep(0.1)
        snapshot = self.mgr.get_status_snapshot()
        self.assertEqual(snapshot["delivery"]["delivered_records"], 100)
        self.assertEqual(snapshot["delivery"]["progress_percent"], 100.0)
        self.mgr.stop_all()


class TestFastApiEndpoints(unittest.TestCase):
    """Tests the FastAPI REST API and WebSocket streaming endpoints."""

    def setUp(self):
        self.client = TestClient(app)

    def tearDown(self):
        from server.app import manager
        if manager:
            manager.stop_all()

    def test_get_status_endpoint(self):
        """Tests GET /api/status returns 200 with valid snapshot schema."""
        resp = self.client.get("/api/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("components", data)
        self.assertIn("delivery", data)
        self.assertIn("network_stats", data)
        self.assertIn("config", data)

    def test_get_datasets_endpoint(self):
        """Tests GET /api/datasets returns list of available test datasets."""
        resp = self.client.get("/api/datasets")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("datasets", data)
        self.assertIsInstance(data["datasets"], list)
        if len(data["datasets"]) > 0:
            first = data["datasets"][0]
            self.assertIn("filename", first)
            self.assertIn("record_count", first)

    def test_configure_network_endpoint(self):
        """Tests POST /api/configure modifies settings and returns updated config."""
        payload = {
            "loss_rate": 0.07,
            "reorder_rate": 0.03,
            "stall_duration_ms": 300.0,
            "stall_interval_sec": 4.0,
            "reverse_loss_rate": 0.01,
        }
        resp = self.client.post("/api/configure", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "ok")
        cfg = data["config"]
        self.assertAlmostEqual(cfg["loss_rate"], 0.07)
        self.assertAlmostEqual(cfg["reorder_rate"], 0.03)
        self.assertEqual(cfg["stall_duration_ms"], 300.0)

    def test_events_and_clear_endpoint(self):
        """Tests GET /api/events and POST /api/events/clear."""
        # Clear
        resp = self.client.post("/api/events/clear")
        self.assertEqual(resp.status_code, 200)

        # Get
        resp = self.client.get("/api/events?limit=10")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("events", data)
        self.assertTrue(len(data["events"]) >= 1)

    def test_attention_dismiss_endpoint(self):
        """Tests POST /api/attention/dismiss."""
        # Add test attention item via server manager
        from server.app import manager
        if manager:
            manager.add_attention_item("test_alert", "Test Alert", "Unit test alert", "warning")

        resp = self.client.post("/api/attention/dismiss", json={"id": "test_alert"})
        self.assertEqual(resp.status_code, 200)

    def test_static_dashboard_endpoint(self):
        """Tests GET / serves index.html dashboard."""
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers["content-type"])
        self.assertIn("TAR", resp.text)
        self.assertIn("Delivery Monitoring", resp.text)

    def test_websocket_telemetry_streaming(self):
        """Tests WebSocket connection receives live telemetry snapshot."""
        with self.client.websocket_connect("/ws") as ws:
            # Client should receive initial status snapshot immediately
            msg = ws.receive_text()
            data = json.loads(msg)
            self.assertIn("components", data)
            self.assertIn("delivery", data)

            # Test ping/pong
            ws.send_text("ping")
            pong = ws.receive_text()
            data2 = json.loads(pong)
            self.assertEqual(data2.get("type"), "pong")


if __name__ == "__main__":
    unittest.main()
