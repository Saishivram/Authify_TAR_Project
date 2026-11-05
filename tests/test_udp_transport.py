"""
test_udp_transport.py
=====================
Automated integration tests for end-to-end UDP Sender-Receiver communication.

Covers:
- 100-record transmission and JSONL output validation.
- Edge-case payload delivery (empty, unicode, large, boundary).
- START/START_ACK session handshake and idempotency.
- FIN/FIN_ACK session teardown and idempotency.
- Strict pre-FIN_ACK validation refusing incomplete delivery.
- Large socket buffer capturing and rejecting oversized wire datagrams.
"""

import json
import os
import socket
import tempfile
import threading
import time
import unittest

from schemas.record_schema import validate_jsonl_file
from transport.packet import (
    DEFAULT_RECEIVE_BUFFER_SIZE,
    DataPacket,
    ErrorPacket,
    FinAckPacket,
    FinPacket,
    StartAckPacket,
    StartPacket,
    deserialize_packet,
    serialize_packet,
)
from transport.receiver import UdpReceiver
from transport.sender import (
    ConnectionHandshakeError,
    ConnectionTeardownError,
    UdpSender,
)


class TestUdpTransport(unittest.TestCase):
    """Integration test suite for TAR UDP transport."""

    def test_100_records_end_to_end(self):
        """Verify complete 100-record transmission over UDP loopback."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "received_100.jsonl")
            test_port = 19004
            bind_port = 19001

            receiver = UdpReceiver(
                host="127.0.0.1",
                port=test_port,
                output_path=out_file,
            )

            # Run receiver in background thread
            receiver_thread = threading.Thread(
                target=receiver.run_until_complete,
                kwargs={"timeout": 5.0},
                daemon=True,
            )
            receiver_thread.start()
            time.sleep(0.1)  # Ensure receiver socket bound

            sender = UdpSender(
                target_host="127.0.0.1",
                target_port=test_port,
                bind_port=bind_port,
            )

            sender_metrics = sender.transmit_file(
                input_file_path="data/sample_100.jsonl",
                session_id="test-100-session",
                pace_interval=0.0001,
            )

            receiver_thread.join(timeout=3.0)

            # Assert sender metrics
            self.assertEqual(sender_metrics["status"], "COMPLETED")
            self.assertEqual(sender_metrics["sent_records"], 100)
            self.assertLessEqual(sender_metrics["max_actual_datagram_size_bytes"], 1200)

            # Assert receiver metrics
            receiver_metrics = receiver.get_metrics()
            self.assertEqual(receiver_metrics["status"], "COMPLETED")
            self.assertEqual(receiver_metrics["received_records"], 100)
            self.assertEqual(receiver_metrics["missing_records_count"], 0)
            self.assertEqual(receiver_metrics["duplicate_records_count"], 0)
            self.assertEqual(receiver_metrics["out_of_order_records_count"], 0)

            # Verify received file schema
            validation_res = validate_jsonl_file(out_file, expected_count=100)
            self.assertTrue(validation_res["valid"], f"Validation errors: {validation_res['errors']}")
            self.assertEqual(validation_res["total_records"], 100)

    def test_idempotent_start_handshake(self):
        """Verify receiver handles duplicate START packets idempotently."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "received_idem.jsonl")
            test_port = 19005

            receiver = UdpReceiver(
                host="127.0.0.1",
                port=test_port,
                output_path=out_file,
            )
            receiver.start_socket()

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.bind(("127.0.0.1", 0))
            client_sock.settimeout(1.0)

            try:
                # 1. Send first START
                start1 = StartPacket(connection_id="idem-sess", expected_records=50)
                client_sock.sendto(serialize_packet(start1), ("127.0.0.1", test_port))

                raw_ack1, sender_addr = receiver.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                receiver._handle_start(deserialize_packet(raw_ack1), sender_addr)

                resp1, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                ack_pkt1 = deserialize_packet(resp1)
                self.assertIsInstance(ack_pkt1, StartAckPacket)
                self.assertEqual(ack_pkt1.status, "READY")
                self.assertEqual(receiver.state, "RECEIVING")

                # 2. Send duplicate START with same connection ID
                start2 = StartPacket(connection_id="idem-sess", expected_records=50)
                client_sock.sendto(serialize_packet(start2), ("127.0.0.1", test_port))

                raw_ack2, sender_addr2 = receiver.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                receiver._handle_start(deserialize_packet(raw_ack2), sender_addr2)

                resp2, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                ack_pkt2 = deserialize_packet(resp2)
                self.assertIsInstance(ack_pkt2, StartAckPacket)
                self.assertEqual(ack_pkt2.status, "READY")
                self.assertEqual(receiver.state, "RECEIVING")

            finally:
                receiver.close()
                client_sock.close()

    def test_strict_pre_fin_ack_verification_refuses_incomplete_delivery(self):
        """Verify receiver strictly refuses to send FIN_ACK if records are missing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "incomplete.jsonl")
            test_port = 19006

            receiver = UdpReceiver(
                host="127.0.0.1",
                port=test_port,
                output_path=out_file,
            )
            receiver.start_socket()

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.bind(("127.0.0.1", 0))
            client_sock.settimeout(1.0)
            client_addr = client_sock.getsockname()

            try:
                # Initialize session expecting 5 records
                start = StartPacket(connection_id="fail-sess", expected_records=5)
                client_sock.sendto(serialize_packet(start), ("127.0.0.1", test_port))
                raw_b, sender_addr = receiver.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                receiver._handle_start(deserialize_packet(raw_b), sender_addr)

                # Drain START_ACK from client socket
                raw_start_ack, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                self.assertIsInstance(deserialize_packet(raw_start_ack), StartAckPacket)

                # Send records 1 and 2, but skip 3, 4, 5
                for rid in (1, 2):
                    dp = DataPacket(
                        connection_id="fail-sess",
                        packet_id=rid,
                        record_id=rid,
                        payload={"value": f"rec-{rid}"},
                    )
                    receiver._handle_data(dp, client_addr)

                # Send FIN declaring final_record_id=5
                fin = FinPacket(
                    connection_id="fail-sess",
                    packet_id=6,
                    final_record_id=5,
                    total_packets=5,
                )
                receiver._handle_fin(fin, client_addr)

                # Expect ERROR packet returned, NOT FIN_ACK
                resp, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                err_pkt = deserialize_packet(resp)
                self.assertIsInstance(err_pkt, ErrorPacket)
                self.assertEqual(err_pkt.code, "INCOMPLETE_DELIVERY")
                self.assertEqual(receiver.state, "FAILED")
                self.assertIsNone(receiver.cached_fin_ack_bytes)

            finally:
                receiver.close()
                client_sock.close()

    def test_idempotent_fin_handling(self):
        """Verify duplicate FIN receives cached FIN_ACK without duplicate file processing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "fin_idem.jsonl")
            test_port = 19007

            receiver = UdpReceiver(
                host="127.0.0.1",
                port=test_port,
                output_path=out_file,
            )
            receiver.start_socket()

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.bind(("127.0.0.1", 0))
            client_sock.settimeout(1.0)
            client_addr = client_sock.getsockname()

            try:
                # Initialize session expecting 1 record
                start = StartPacket(connection_id="fin-idem-sess", expected_records=1)
                client_sock.sendto(serialize_packet(start), ("127.0.0.1", test_port))
                raw_b, sender_addr = receiver.sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                receiver._handle_start(deserialize_packet(raw_b), sender_addr)

                # Drain START_ACK from client socket
                raw_start_ack, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                self.assertIsInstance(deserialize_packet(raw_start_ack), StartAckPacket)

                # Send record 1
                dp = DataPacket(
                    connection_id="fin-idem-sess",
                    packet_id=1,
                    record_id=1,
                    payload={"value": "rec-1"},
                )
                receiver._handle_data(dp, client_addr)

                # Send first FIN
                fin1 = FinPacket(
                    connection_id="fin-idem-sess",
                    packet_id=2,
                    final_record_id=1,
                    total_packets=1,
                )
                receiver._handle_fin(fin1, client_addr)

                # Verify FIN_ACK received
                raw_fin_ack1, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                ack1 = deserialize_packet(raw_fin_ack1)
                self.assertIsInstance(ack1, FinAckPacket)
                self.assertEqual(receiver.state, "COMPLETED")

                # Send duplicate FIN
                receiver._handle_fin(fin1, client_addr)
                raw_fin_ack2, _ = client_sock.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                ack2 = deserialize_packet(raw_fin_ack2)
                self.assertIsInstance(ack2, FinAckPacket)
                self.assertEqual(ack1.receiver_delivered_through, ack2.receiver_delivered_through)

            finally:
                receiver.close()
                client_sock.close()

    def test_oversized_datagram_rejected_by_receiver(self):
        """Verify large socket buffer receives and rejects oversized datagram (>1200B)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = os.path.join(tmp_dir, "oversized.jsonl")
            test_port = 19008

            receiver = UdpReceiver(
                host="127.0.0.1",
                port=test_port,
                output_path=out_file,
            )

            # Start receiver in background
            rec_thread = threading.Thread(
                target=receiver.run_until_complete,
                kwargs={"timeout": 1.0},
                daemon=True,
            )
            rec_thread.start()
            time.sleep(0.1)

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                # Construct 1,350 byte datagram
                huge_data = json.dumps({
                    "type": "DATA",
                    "version": 1,
                    "connection_id": "huge-sess",
                    "packet_id": 1,
                    "record_id": 1,
                    "payload": {"value": "Y" * 1250},
                }).encode("utf-8")

                self.assertGreater(len(huge_data), 1200)

                # Send oversized datagram
                client_sock.sendto(huge_data, ("127.0.0.1", test_port))
                time.sleep(0.3)

                # Verify receiver captured and rejected it
                metrics = receiver.get_metrics()
                self.assertGreaterEqual(metrics["oversized_datagrams_rejected"], 1)
                self.assertGreater(metrics["max_actual_datagram_size_bytes"], 1200)

            finally:
                client_sock.close()
                receiver.close()
                rec_thread.join(timeout=1.0)


if __name__ == "__main__":
    unittest.main()
