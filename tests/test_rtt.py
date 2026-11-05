"""
tests/test_rtt.py
=================
Unit tests for RttEstimator: RFC 6298 smoothing, bounds, backoff, PTO, and adaptive reordering window.
"""

import unittest
from transport.rtt import RttEstimator


class TestRttEstimator(unittest.TestCase):
    def test_initial_state(self):
        estimator = RttEstimator()
        self.assertFalse(estimator.has_sample)
        self.assertIsNone(estimator.srtt)
        self.assertIsNone(estimator.rttvar)
        self.assertEqual(estimator.rto, 1.0)
        self.assertEqual(estimator.get_pto(), 1.0)

    def test_first_measurement(self):
        estimator = RttEstimator(min_rto=0.5, max_rto=4.0)
        # 50 ms raw RTT, 2 ms ack delay -> 48 ms sample
        sample = estimator.update_rtt(send_time=100.0, recv_time=100.050, ack_delay_ms=2.0)
        self.assertAlmostEqual(sample, 0.048, places=5)
        self.assertTrue(estimator.has_sample)
        self.assertAlmostEqual(estimator.srtt, 0.048, places=5)
        self.assertAlmostEqual(estimator.rttvar, 0.024, places=5)
        # Calculated RTO: 0.048 + 4 * 0.024 = 0.144 -> clamped to min_rto (0.5)
        self.assertEqual(estimator.rto, 0.5)

    def test_subsequent_measurements(self):
        estimator = RttEstimator(min_rto=0.1, max_rto=4.0)
        # First sample: 100 ms
        estimator.update_rtt(send_time=100.0, recv_time=100.100, ack_delay_ms=0.0)
        self.assertAlmostEqual(estimator.srtt, 0.100, places=5)
        self.assertAlmostEqual(estimator.rttvar, 0.050, places=5)

        # Second sample: 200 ms
        estimator.update_rtt(send_time=200.0, recv_time=200.200, ack_delay_ms=0.0)
        # rttvar = 0.75 * 0.050 + 0.25 * |0.100 - 0.200| = 0.0375 + 0.025 = 0.0625
        # srtt = 0.875 * 0.100 + 0.125 * 0.200 = 0.0875 + 0.025 = 0.1125
        self.assertAlmostEqual(estimator.srtt, 0.1125, places=5)
        self.assertAlmostEqual(estimator.rttvar, 0.0625, places=5)

    def test_exponential_backoff_and_reset(self):
        estimator = RttEstimator(min_rto=0.5, max_rto=4.0)
        estimator.update_rtt(send_time=100.0, recv_time=100.050)
        self.assertEqual(estimator.rto, 0.5)

        # Backoff doubling
        self.assertEqual(estimator.backoff_rto(), 1.0)
        self.assertEqual(estimator.backoff_rto(), 2.0)
        self.assertEqual(estimator.backoff_rto(), 4.0)
        # Capped at max_rto
        self.assertEqual(estimator.backoff_rto(), 4.0)

        # Reset recalculates from SRTT and RTTVAR
        self.assertEqual(estimator.reset_rto(), 0.5)

    def test_pto_calculation(self):
        estimator = RttEstimator()
        # Initial PTO is initial_rto (1.0) before samples
        self.assertEqual(estimator.get_pto(), 1.0)

        # Sample with 10 ms RTT -> PTO would be ~0.010 + 4 * 0.005 + 0.005 = 0.035 -> floored to 0.2s (200ms)
        estimator.update_rtt(send_time=10.0, recv_time=10.010)
        self.assertEqual(estimator.get_pto(), 0.2)

        # Large sample with 200 ms RTT -> SRTT=0.2, RTTVAR=0.1
        # PTO = 0.2 + 4 * 0.1 + 0.005 = 0.605s > 0.2s
        estimator = RttEstimator()
        estimator.update_rtt(send_time=10.0, recv_time=10.200)
        self.assertAlmostEqual(estimator.get_pto(), 0.605, places=4)

    def test_ack_delay_capping(self):
        estimator = RttEstimator(max_ack_delay=0.005)
        # Receiver reports 50 ms ack_delay, but cap is 5 ms (0.005 s)
        # Raw RTT = 100 ms -> sample should be 100 - 5 = 95 ms
        sample = estimator.update_rtt(send_time=10.0, recv_time=10.100, ack_delay_ms=50.0)
        self.assertAlmostEqual(sample, 0.095, places=5)

    def test_adaptive_multiplier_and_16_flight_decay(self):
        estimator = RttEstimator()
        self.assertEqual(estimator.reordering_window_multiplier, 1.0)

        # Spurious retransmissions expand multiplier up to 4.0
        estimator.on_spurious_retransmission()
        self.assertEqual(estimator.reordering_window_multiplier, 2.0)
        estimator.on_spurious_retransmission()
        self.assertEqual(estimator.reordering_window_multiplier, 3.0)
        estimator.on_spurious_retransmission()
        self.assertEqual(estimator.reordering_window_multiplier, 4.0)
        estimator.on_spurious_retransmission()
        self.assertEqual(estimator.reordering_window_multiplier, 4.0)

        # 15 clean flights do not decay yet
        for _ in range(15):
            estimator.on_clean_flight()
        self.assertEqual(estimator.reordering_window_multiplier, 4.0)
        self.assertEqual(estimator.clean_flight_count, 15)

        # 16th clean flight decays multiplier by 1.0
        estimator.on_clean_flight()
        self.assertEqual(estimator.reordering_window_multiplier, 3.0)
        self.assertEqual(estimator.clean_flight_count, 0)

        # Unclean flight resets counter
        for _ in range(10):
            estimator.on_clean_flight()
        self.assertEqual(estimator.clean_flight_count, 10)
        estimator.on_unclean_flight()
        self.assertEqual(estimator.clean_flight_count, 0)


if __name__ == "__main__":
    unittest.main()
