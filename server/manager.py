"""
server/manager.py
=================
System and Process Manager for the TAR Protocol Control Server (Phase 5).

Coordinates the lifecycle of UdpReceiver, ImpairmentRelay, and UdpSender.
Provides:
1. Threaded execution with non-blocking lifecycle controls (start/stop/restart).
2. Live telemetry polling and high-frequency metric aggregation.
3. Dynamic network impairment configuration updates on-the-fly.
4. Timestamped Event History ring buffer.
5. Operator Human Attention alert manager.
"""

from collections import deque
from datetime import datetime
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from generator.record_generator import generate_dataset
from schemas.record_schema import validate_jsonl_file
from transport.receiver import UdpReceiver
from transport.relay import ImpairmentRelay, RelayConfig, RelayDirectionConfig
from transport.sender import UdpSender


class SystemManager:
    """
    Central controller managing TAR transport processes, live telemetry, and operator events.
    """

    def __init__(self):
        self.lock = threading.RLock()

        # Component instances and execution threads
        self.receiver: Optional[UdpReceiver] = None
        self.receiver_thread: Optional[threading.Thread] = None
        self.receiver_state: str = "STOPPED"
        self.receiver_metrics: Dict[str, Any] = {}
        self.receiver_error: Optional[str] = None
        self.receiver_port: int = 9004

        self.relay: Optional[ImpairmentRelay] = None
        self.relay_thread: Optional[threading.Thread] = None
        self.relay_state: str = "STOPPED"
        self.relay_metrics: Dict[str, Any] = {}
        self.relay_error: Optional[str] = None
        self.relay_port_a: int = 9002
        self.relay_port_b: int = 9003

        self.sender: Optional[UdpSender] = None
        self.sender_thread: Optional[threading.Thread] = None
        self.sender_state: str = "STOPPED"
        self.sender_metrics: Dict[str, Any] = {}
        self.sender_error: Optional[str] = None
        self.sender_bind_port: int = 9001

        # Network Impairment Settings
        self.config: Dict[str, Any] = {
            "loss_rate": 0.05,
            "reorder_rate": 0.02,
            "reorder_min_delay_ms": 20.0,
            "reorder_max_delay_ms": 50.0,
            "stall_interval_sec": 5.0,
            "stall_duration_ms": 400.0,
            "reverse_loss_rate": 0.0,
        }

        # Active Session Metadata
        self.session_info: Dict[str, Any] = {
            "session_id": None,
            "record_count": 0,
            "dataset_path": "data/records_2000.jsonl",
            "output_path": "data/received_dashboard.jsonl",
            "start_time": None,
            "end_time": None,
            "status": "IDLE",
        }

        # Event History ring buffer (up to 500 entries)
        self.events: deque = deque(maxlen=500)
        self.event_counter: int = 0

        # Human Attention items
        self.attention_items: Dict[str, Dict[str, Any]] = {}
        self.attention_counter: int = 0

        # Metric tracking helpers
        self._last_delivered: int = 0
        self._last_delivery_time: float = time.monotonic()
        self._prev_relay_dropped: int = 0
        self._prev_relay_reordered: int = 0
        self._prev_retransmissions: int = 0
        self._prev_probes: int = 0

        # Background telemetry poller
        self._running = True
        self._poller_thread = threading.Thread(target=self._telemetry_loop, daemon=True)
        self._poller_thread.start()

        self.add_event(
            level="INFO",
            source="SYSTEM",
            event_type="SYSTEM",
            message="TAR Control Server initialized and ready.",
        )

    # -------------------------------------------------------------------------
    # Event Logging & Human Attention
    # -------------------------------------------------------------------------

    def add_event(self, level: str, source: str, event_type: str, message: str) -> Dict[str, Any]:
        """Appends a new timestamped event to the history ring buffer."""
        with self.lock:
            self.event_counter += 1
            now = datetime.now()
            evt = {
                "id": self.event_counter,
                "timestamp": now.strftime("%H:%M:%S.%f")[:-3],
                "iso_time": now.isoformat(),
                "level": level.upper(),  # INFO, SUCCESS, WARNING, ERROR
                "source": source.upper(),  # SENDER, RECEIVER, RELAY, SYSTEM
                "type": event_type.upper(),  # HANDSHAKE, DATA, RETRANSMISSION, PACKET_LOSS, RECOVERY, TEARDOWN, CONFIG, SYSTEM, ERROR
                "message": message,
            }
            self.events.append(evt)
            return evt

    def get_events(self, limit: int = 100, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns recent events, optionally filtered by category."""
        with self.lock:
            all_evts = list(self.events)
            if category and category.upper() != "ALL":
                cat = category.upper()
                all_evts = [e for e in all_evts if e["type"] == cat or e["source"] == cat]
            return all_evts[-limit:]

    def clear_events(self) -> None:
        """Clears all logged events."""
        with self.lock:
            self.events.clear()
            self.add_event("INFO", "SYSTEM", "SYSTEM", "Event history cleared by operator.")

    def add_attention_item(self, item_id: str, title: str, description: str, severity: str = "warning") -> None:
        """Adds or updates an unresolved anomaly requiring operator attention."""
        with self.lock:
            self.attention_items[item_id] = {
                "id": item_id,
                "title": title,
                "description": description,
                "severity": severity,  # warning, error, info
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            }

    def dismiss_attention_item(self, item_id: str) -> bool:
        """Dismisses an attention item by ID."""
        with self.lock:
            if item_id in self.attention_items:
                del self.attention_items[item_id]
                return True
            return False

    def clear_attention_items(self) -> None:
        """Clears all operator attention items."""
        with self.lock:
            self.attention_items.clear()

    # -------------------------------------------------------------------------
    # Receiver Management
    # -------------------------------------------------------------------------

    def start_receiver(
        self,
        port: int = 9004,
        output_path: str = "data/received_dashboard.jsonl",
        reorder_capacity: int = 10000,
    ) -> bool:
        """Starts the UdpReceiver in a background daemon thread."""
        with self.lock:
            if self.receiver_state == "RUNNING":
                return False

            self.receiver_port = port
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            self.receiver = UdpReceiver(
                host="127.0.0.1",
                port=port,
                output_path=output_path,
                reorder_capacity=reorder_capacity,
                enable_fin_repair=True,
            )
            self.receiver_state = "RUNNING"
            self.receiver_error = None
            self.session_info["output_path"] = output_path

            def _run():
                try:
                    self.add_event(
                        "INFO",
                        "RECEIVER",
                        "HANDSHAKE",
                        f"Receiver listening on 127.0.0.1:{port} (output: {output_path})",
                    )
                    metrics = self.receiver.run_until_complete(timeout=120.0)
                    with self.lock:
                        self.receiver_metrics = metrics
                        if metrics.get("status") == "COMPLETED":
                            self.receiver_state = "COMPLETED"
                            self.add_event(
                                "SUCCESS",
                                "RECEIVER",
                                "COMPLETION",
                                f"Receiver verified {metrics.get('received_records', 0)} records in order. Max delivery gap: {metrics.get('max_delivery_gap_seconds', 0)}s",
                            )
                        elif self.receiver_state != "STOPPED":
                            self.receiver_state = "FAILED"
                            self.add_event(
                                "ERROR",
                                "RECEIVER",
                                "ERROR",
                                f"Receiver ended with status: {metrics.get('status')}",
                            )
                except Exception as exc:
                    with self.lock:
                        self.receiver_state = "FAILED"
                        self.receiver_error = str(exc)
                        self.add_event("ERROR", "RECEIVER", "ERROR", f"Receiver crashed: {exc}")
                        self.add_attention_item(
                            "receiver_crash",
                            "Receiver Component Failure",
                            f"Receiver thread encountered exception: {exc}",
                            "error",
                        )

            self.receiver_thread = threading.Thread(target=_run, daemon=True)
            self.receiver_thread.start()
            return True

    def stop_receiver(self) -> None:
        """Stops the running UdpReceiver cleanly."""
        with self.lock:
            if self.receiver:
                try:
                    self.receiver.stop()
                except Exception:
                    pass
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(timeout=0.5)
            self.receiver_state = "STOPPED"
            self.add_event("INFO", "RECEIVER", "SYSTEM", "Receiver stopped cleanly.")

    # -------------------------------------------------------------------------
    # Relay Management & Live Impairment Reconfiguration
    # -------------------------------------------------------------------------

    def start_relay(
        self,
        port_a: int = 9002,
        port_b: int = 9003,
        sender_port: int = 9001,
        receiver_port: int = 9004,
    ) -> bool:
        """Starts the Network Impairment Relay in a background daemon thread."""
        with self.lock:
            if self.relay_state == "RUNNING":
                return False

            self.relay_port_a = port_a
            self.relay_port_b = port_b

            forward_cfg = RelayDirectionConfig(
                loss_rate=self.config["loss_rate"],
                reorder_rate=self.config["reorder_rate"],
                reorder_min_delay_ms=self.config["reorder_min_delay_ms"],
                reorder_max_delay_ms=self.config["reorder_max_delay_ms"],
                stall_interval_sec=self.config["stall_interval_sec"],
                stall_duration_ms=self.config["stall_duration_ms"],
            )

            reverse_cfg = RelayDirectionConfig(
                loss_rate=self.config["reverse_loss_rate"],
                reorder_rate=0.0,
                stall_interval_sec=0.0,
            )

            relay_cfg = RelayConfig(
                port_a=port_a,
                port_b=port_b,
                sender_addr=("127.0.0.1", sender_port),
                receiver_addr=("127.0.0.1", receiver_port),
                seed=42,
                forward=forward_cfg,
                reverse=reverse_cfg,
            )

            self.relay = ImpairmentRelay(config=relay_cfg)
            self.relay_state = "RUNNING"
            self.relay_error = None
            self._prev_relay_dropped = 0
            self._prev_relay_reordered = 0

            def _run():
                try:
                    self.add_event(
                        "INFO",
                        "RELAY",
                        "SYSTEM",
                        f"Relay online (Port A: {port_a} <-> Port B: {port_b}) | Loss: {self.config['loss_rate']*100:.1f}%, Reorder: {self.config['reorder_rate']*100:.1f}%, Stall: {self.config['stall_duration_ms']}ms/{self.config['stall_interval_sec']}s",
                    )
                    metrics = self.relay.run(max_duration=120.0, idle_timeout=15.0)
                    with self.lock:
                        self.relay_metrics = metrics
                        if self.relay_state != "STOPPED":
                            self.relay_state = "STOPPED"
                            self.add_event("INFO", "RELAY", "SYSTEM", "Relay idle timeout reached; shut down.")
                except Exception as exc:
                    with self.lock:
                        self.relay_state = "FAILED"
                        self.relay_error = str(exc)
                        self.add_event("ERROR", "RELAY", "ERROR", f"Relay crashed: {exc}")
                        self.add_attention_item(
                            "relay_crash",
                            "Relay Component Failure",
                            f"Relay thread encountered exception: {exc}",
                            "error",
                        )

            self.relay_thread = threading.Thread(target=_run, daemon=True)
            self.relay_thread.start()
            return True

    def stop_relay(self) -> None:
        """Stops the running ImpairmentRelay cleanly."""
        with self.lock:
            if self.relay:
                try:
                    self.relay.stop()
                except Exception:
                    pass
            if self.relay_thread and self.relay_thread.is_alive():
                self.relay_thread.join(timeout=0.5)
            self.relay_state = "STOPPED"
            self.add_event("INFO", "RELAY", "SYSTEM", "Relay stopped cleanly.")

    def configure_network(
        self,
        loss_rate: Optional[float] = None,
        reorder_rate: Optional[float] = None,
        stall_duration_ms: Optional[float] = None,
        stall_interval_sec: Optional[float] = None,
        reverse_loss_rate: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Dynamically updates network impairment configuration.
        Applies changes live to the running relay forward/reverse schedulers.
        """
        with self.lock:
            if loss_rate is not None:
                self.config["loss_rate"] = max(0.0, min(1.0, float(loss_rate)))
            if reorder_rate is not None:
                self.config["reorder_rate"] = max(0.0, min(1.0, float(reorder_rate)))
            if stall_duration_ms is not None:
                self.config["stall_duration_ms"] = max(0.0, float(stall_duration_ms))
            if stall_interval_sec is not None:
                self.config["stall_interval_sec"] = max(0.0, float(stall_interval_sec))
            if reverse_loss_rate is not None:
                self.config["reverse_loss_rate"] = max(0.0, min(1.0, float(reverse_loss_rate)))

            # If relay is actively running, update scheduler configs on-the-fly
            if self.relay and self.relay_state == "RUNNING":
                try:
                    self.relay.forward_scheduler.config.loss_rate = self.config["loss_rate"]
                    self.relay.forward_scheduler.config.reorder_rate = self.config["reorder_rate"]
                    self.relay.forward_scheduler.config.stall_duration_ms = self.config["stall_duration_ms"]
                    self.relay.forward_scheduler.config.stall_interval_sec = self.config["stall_interval_sec"]
                    self.relay.reverse_scheduler.config.loss_rate = self.config["reverse_loss_rate"]
                except Exception as exc:
                    self.add_event("ERROR", "RELAY", "CONFIG", f"Failed applying live config: {exc}")

            # Check if operator attention needed for extreme settings
            if self.config["loss_rate"] > 0.25:
                self.add_attention_item(
                    "high_loss",
                    "High Forward Loss Rate",
                    f"Forward packet loss is set to {self.config['loss_rate']*100:.1f}%, which may cause severe throughput degradation.",
                    "warning",
                )
            else:
                self.dismiss_attention_item("high_loss")

            self.add_event(
                "INFO",
                "RELAY",
                "CONFIG",
                f"Network impairments updated: Loss={self.config['loss_rate']*100:.1f}%, Reorder={self.config['reorder_rate']*100:.1f}%, Stall={self.config['stall_duration_ms']}ms every {self.config['stall_interval_sec']}s, ACK Loss={self.config['reverse_loss_rate']*100:.1f}%",
            )
            return dict(self.config)

    # -------------------------------------------------------------------------
    # Sender Management
    # -------------------------------------------------------------------------

    def start_sender(
        self,
        input_file: str = "data/records_2000.jsonl",
        send_window: int = 200,
        pace_interval: float = 0.0,
        session_id: Optional[str] = None,
        bind_port: Optional[int] = None,
    ) -> bool:
        """Starts the UdpSender in a background daemon thread."""
        with self.lock:
            if self.sender_state == "RUNNING":
                return False

            if not os.path.exists(input_file):
                self.add_event("ERROR", "SENDER", "ERROR", f"Input file not found: {input_file}")
                return False

            # Route to Relay Port A if relay is running, else direct to Receiver
            target_port = self.relay_port_a if self.relay_state == "RUNNING" else self.receiver_port
            session = session_id or f"session-dash-{int(time.time())}"
            if bind_port:
                self.sender_bind_port = bind_port

            self.sender = UdpSender(
                target_host="127.0.0.1",
                target_port=target_port,
                bind_port=self.sender_bind_port,
                send_window=send_window,
                retry_timeout=1.0,
            )
            self.sender_state = "RUNNING"
            self.sender_error = None
            self.session_info["session_id"] = session
            self.session_info["start_time"] = datetime.now().isoformat()
            self.session_info["status"] = "RUNNING"
            self._prev_retransmissions = 0
            self._prev_probes = 0

            def _run():
                try:
                    self.add_event(
                        "INFO",
                        "SENDER",
                        "HANDSHAKE",
                        f"Sender transmitting from {input_file} targeting 127.0.0.1:{target_port} (session: {session})",
                    )
                    metrics = self.sender.transmit_file(
                        input_file_path=input_file,
                        session_id=session,
                        pace_interval=pace_interval,
                    )
                    with self.lock:
                        self.sender_metrics = metrics
                        self.sender_state = "COMPLETED"
                        self.session_info["end_time"] = datetime.now().isoformat()
                        self.session_info["status"] = "COMPLETED"
                        self.add_event(
                            "SUCCESS",
                            "SENDER",
                            "COMPLETION",
                            f"Sender finished session '{session}'! Delivered {metrics.get('sent_records', 0)} records @ {metrics.get('achieved_records_per_second', 0)} rec/s (Duration: {metrics.get('transmission_duration_seconds', 0)}s)",
                        )
                except Exception as exc:
                    with self.lock:
                        self.sender_state = "FAILED"
                        self.sender_error = str(exc)
                        self.session_info["status"] = "FAILED"
                        self.add_event("ERROR", "SENDER", "ERROR", f"Sender failed: {exc}")
                        self.add_attention_item(
                            "sender_crash",
                            "Sender Transmission Failure",
                            f"Sender encountered error: {exc}",
                            "error",
                        )

            self.sender_thread = threading.Thread(target=_run, daemon=True)
            self.sender_thread.start()
            return True

    def stop_sender(self) -> None:
        """Stops the running UdpSender cleanly."""
        with self.lock:
            if self.sender:
                try:
                    self.sender.stop()
                except Exception:
                    pass
            if self.sender_thread and self.sender_thread.is_alive():
                self.sender_thread.join(timeout=0.5)
            self.sender_state = "STOPPED"
            self.session_info["status"] = "STOPPED"
            self.add_event("INFO", "SENDER", "SYSTEM", "Sender stopped cleanly.")

    # -------------------------------------------------------------------------
    # Orchestration: Start All / Stop All
    # -------------------------------------------------------------------------

    def start_all(
        self,
        record_count: int = 2000,
        dataset_path: Optional[str] = None,
        receiver_port: int = 9004,
        relay_port_a: int = 9002,
        relay_port_b: int = 9003,
        sender_port: int = 9001,
        send_window: int = 200,
        pace_interval: float = 0.0,
    ) -> bool:
        """
        Orchestrates an end-to-end transport session:
        1. Prepares dataset.
        2. Starts Receiver.
        3. Starts Impairment Relay.
        4. Starts Sender.
        """
        with self.lock:
            # Map count to standard datasets if path not explicitly given
            if not dataset_path:
                dataset_map = {
                    100: "data/sample_100.jsonl",
                    2000: "data/records_2000.jsonl",
                    5000: "data/records_5000.jsonl",
                    10000: "data/records_10000.jsonl",
                    20000: "data/records_20000.jsonl",
                }
                dataset_path = dataset_map.get(record_count, f"data/records_{record_count}.jsonl")

            # Ensure dataset exists
            if not os.path.exists(dataset_path):
                self.add_event("INFO", "SYSTEM", "DATA", f"Generating {record_count} records -> {dataset_path}...")
                os.makedirs(os.path.dirname(os.path.abspath(dataset_path)), exist_ok=True)
                generate_dataset(record_count=record_count, seed=42, output_path=dataset_path)

            self.session_info["record_count"] = record_count
            self.session_info["dataset_path"] = dataset_path
            self.session_info["status"] = "RUNNING"
            self.session_info["start_time"] = datetime.now().isoformat()
            self.session_info["end_time"] = None

            # Reset previous metrics for clean session tracking
            self.receiver_metrics = {}
            self.sender_metrics = {}
            self._last_delivered = 0
            self._prev_relay_dropped = 0
            self._prev_relay_reordered = 0
            self._prev_retransmissions = 0
            self._prev_probes = 0

            # Stop any already active components
            self.stop_all()
            time.sleep(0.08)

            # 1. Start Receiver
            rec_ok = self.start_receiver(port=receiver_port, output_path="data/received_dashboard.jsonl")
            time.sleep(0.08)

            # 2. Start Relay
            relay_ok = self.start_relay(
                port_a=relay_port_a,
                port_b=relay_port_b,
                sender_port=sender_port,
                receiver_port=receiver_port,
            )
            time.sleep(0.08)

            # 3. Start Sender
            sender_ok = self.start_sender(
                input_file=dataset_path,
                send_window=send_window,
                pace_interval=pace_interval,
                bind_port=sender_port,
            )

            return rec_ok and relay_ok and sender_ok

    def stop_all(self) -> None:
        """Stops all running components in orderly sequence."""
        self.stop_sender()
        self.stop_relay()
        self.stop_receiver()
        if self.session_info.get("status") == "RUNNING":
            self.session_info["status"] = "STOPPED"
        self.add_event("INFO", "SYSTEM", "SYSTEM", "All components stopped.")

    # -------------------------------------------------------------------------
    # Telemetry Sampling & Anomaly Detection Loop
    # -------------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        """Runs in background sampling telemetry and detecting protocol milestones."""
        while self._running:
            try:
                time.sleep(0.15)
                with self.lock:
                    now = time.monotonic()

                    # 1. Inspect Receiver Metrics & Delivery Gaps
                    if self.receiver:
                        r_metrics = self.receiver.get_metrics()
                        self.receiver_metrics = r_metrics
                        delivered = r_metrics.get("receiver_delivered_through", 0)

                        if delivered > self._last_delivered:
                            self._last_delivered = delivered
                            self._last_delivery_time = now

                        # Check if delivery gap exceeded threshold
                        max_gap = r_metrics.get("max_delivery_gap_seconds", 0.0)
                        if max_gap > 1.0:
                            self.add_attention_item(
                                "max_gap_exceeded",
                                "Delivery Gap SLA Exceeded",
                                f"Max delivery stall gap ({max_gap:.3f}s) exceeded target of 1.0s.",
                                "warning",
                            )
                        elif "max_gap_exceeded" in self.attention_items and max_gap <= 1.0:
                            self.dismiss_attention_item("max_gap_exceeded")

                    # 2. Inspect Relay Metrics for Drops & Reordering Events
                    if self.relay and self.relay_state == "RUNNING":
                        try:
                            rel_m = self.relay.get_metrics()
                            self.relay_metrics = rel_m
                            fwd = rel_m.get("forward", {})
                            dropped = fwd.get("packets_dropped_loss", 0)
                            reordered = fwd.get("packets_dispatched_out_of_order", 0)

                            if dropped > self._prev_relay_dropped:
                                delta = dropped - self._prev_relay_dropped
                                self._prev_relay_dropped = dropped
                                self.add_event(
                                    "WARNING",
                                    "RELAY",
                                    "PACKET_LOSS",
                                    f"Relay dropped {delta} forward packet(s) via loss simulation (total dropped: {dropped})",
                                )

                            if reordered > self._prev_relay_reordered:
                                delta = reordered - self._prev_relay_reordered
                                self._prev_relay_reordered = reordered
                                self.add_event(
                                    "INFO",
                                    "RELAY",
                                    "DATA",
                                    f"Relay dispatched {delta} reordered packet(s) (total out-of-order: {reordered})",
                                )
                        except Exception:
                            pass

                    # 3. Inspect Sender Metrics for Retransmissions & Probes
                    if self.sender and self.sender_state == "RUNNING":
                        s_metrics = self.sender.get_metrics()
                        self.sender_metrics = s_metrics
                        retrans = s_metrics.get("retransmissions_count", 0)
                        probes = s_metrics.get("probes_count", 0)

                        if retrans > self._prev_retransmissions:
                            delta = retrans - self._prev_retransmissions
                            self._prev_retransmissions = retrans
                            self.add_event(
                                "WARNING",
                                "SENDER",
                                "RETRANSMISSION",
                                f"Sender dispatched {delta} loss repair(s) (total retransmissions: {retrans})",
                            )

                        if probes > self._prev_probes:
                            delta = probes - self._prev_probes
                            self._prev_probes = probes
                            self.add_event(
                                "INFO",
                                "SENDER",
                                "RECOVERY",
                                f"Sender dispatched {delta} PTO tail probe(s) (total probes: {probes})",
                            )

                    # 4. Component Health Monitoring for Human Attention
                    if self.receiver_state == "FAILED":
                        self.add_attention_item(
                            "receiver_failed",
                            "Receiver Failure",
                            f"Receiver terminated with error: {self.receiver_error or 'Socket error'}. Operator action: restart receiver.",
                            "error",
                        )
                    elif "receiver_failed" in self.attention_items and self.receiver_state != "FAILED":
                        self.dismiss_attention_item("receiver_failed")

                    if self.sender_state == "FAILED":
                        self.add_attention_item(
                            "sender_failed",
                            "Sender Failure",
                            f"Sender terminated with error: {self.sender_error or 'Transmission failure'}. Operator action: check connectivity/retry limits.",
                            "error",
                        )
                    elif "sender_failed" in self.attention_items and self.sender_state != "FAILED":
                        self.dismiss_attention_item("sender_failed")

                    if self.relay_state == "FAILED":
                        self.add_attention_item(
                            "relay_failed",
                            "Relay Failure",
                            f"Network impairment relay terminated with error: {self.relay_error or 'Socket error'}. Packet forwarding halted.",
                            "error",
                        )
                    elif "relay_failed" in self.attention_items and self.relay_state != "FAILED":
                        self.dismiss_attention_item("relay_failed")

                    # 5. Premature Incomplete Session Detection
                    if self.session_info.get("status") == "STOPPED":
                        exp = self.session_info.get("record_count", 0)
                        deliv = self.receiver_metrics.get("receiver_delivered_through", 0)
                        if exp > 0 and deliv < exp:
                            self.add_attention_item(
                                "session_incomplete",
                                "Incomplete Transmission Session",
                                f"Transport session stopped with {exp - deliv} records undelivered ({deliv}/{exp} delivered).",
                                "warning",
                            )
                    elif self.session_info.get("status") in ("COMPLETED", "RUNNING", "IDLE"):
                        if "session_incomplete" in self.attention_items:
                            self.dismiss_attention_item("session_incomplete")

            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Snapshot Telemetry Aggregation for REST / WebSockets
    # -------------------------------------------------------------------------

    def get_status_snapshot(self) -> Dict[str, Any]:
        """Assembles a clean, comprehensive telemetry snapshot for the UI."""
        with self.lock:
            # Component live metrics
            rec_m = self.receiver.get_metrics() if self.receiver else self.receiver_metrics
            rel_m = self.relay.get_metrics() if self.relay else self.relay_metrics
            sen_m = self.sender.get_metrics() if self.sender else self.sender_metrics

            session_status = self.session_info.get("status", "IDLE")
            expected = self.session_info.get("record_count", 0)
            if sen_m.get("expected_records", 0) > 0:
                expected = sen_m["expected_records"]
            elif rec_m.get("expected_records", 0) > 0:
                expected = rec_m["expected_records"]

            sent = sen_m.get("sent_records", 0)
            delivered = rec_m.get("receiver_delivered_through", 0)

            if session_status == "IDLE" and sent == 0 and delivered == 0:
                pending = 0
                progress = 0.0
                throughput_sent = 0.0
            else:
                pending = max(0, expected - delivered) if expected > 0 else 0
                progress = round((delivered / expected * 100.0), 1) if expected > 0 else 0.0
                throughput_sent = sen_m.get("throughput_rec_per_sec", 0.0)
                if self.sender_state == "COMPLETED" and "achieved_records_per_second" in sen_m:
                    throughput_sent = sen_m["achieved_records_per_second"]

            fwd = rel_m.get("forward", {}) if rel_m else {}
            rev = rel_m.get("reverse", {}) if rel_m else {}

            return {
                "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                "components": {
                    "receiver": {
                        "name": "Receiver",
                        "state": self.receiver_state,
                        "port": self.receiver_port,
                        "error": self.receiver_error,
                        "metrics": rec_m,
                    },
                    "relay": {
                        "name": "Impairment Relay",
                        "state": self.relay_state,
                        "port_a": self.relay_port_a,
                        "port_b": self.relay_port_b,
                        "error": self.relay_error,
                        "metrics": rel_m,
                    },
                    "sender": {
                        "name": "Sender",
                        "state": self.sender_state,
                        "bind_port": self.sender_bind_port,
                        "error": self.sender_error,
                        "metrics": sen_m,
                    },
                },
                "delivery": {
                    "expected_records": expected,
                    "sent_records": sent,
                    "delivered_records": delivered,
                    "pending_records": pending,
                    "progress_percent": progress,
                    "throughput_records_per_sec": throughput_sent,
                    "in_flight_records": sen_m.get("in_flight_count", 0),
                    "reorder_buffer_buffered": rec_m.get("reorder_buffer_remaining", 0),
                },
                "network_stats": {
                    "forward_packets_received": fwd.get("packets_received", sent),
                    "forward_packets_forwarded": fwd.get("packets_forwarded", 0),
                    "forward_packets_dropped": fwd.get("packets_dropped_loss", 0),
                    "forward_packets_reordered": fwd.get("packets_dispatched_out_of_order", 0),
                    "reverse_packets_sent": rec_m.get("acks_sent_count", 0),
                    "reverse_packets_dropped": rev.get("packets_dropped_loss", 0),
                    "retransmissions": sen_m.get("retransmissions_count", 0),
                    "probes": sen_m.get("probes_count", 0),
                    "max_delivery_gap_sec": rec_m.get("max_delivery_gap_seconds", 0.0),
                    "srtt_ms": sen_m.get("srtt_ms", 0.0),
                    "rto_ms": sen_m.get("rto_ms", 200.0),
                    "max_datagram_size_bytes": max(
                        sen_m.get("max_actual_datagram_size_bytes", 0),
                        rec_m.get("max_actual_datagram_size_bytes", 0),
                    ),
                },
                "config": dict(self.config),
                "session": dict(self.session_info),
                "human_attention": list(self.attention_items.values()),
                "recent_events": self.get_events(limit=40),
            }

    def close(self) -> None:
        """Shuts down manager and background poller."""
        self._running = False
        self.stop_all()
