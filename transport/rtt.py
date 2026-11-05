"""
transport/rtt.py
================
RFC 6298 RTT Estimator, PTO (Probe Timeout) calculator, and adaptive reordering window tracker.

Algorithms:
- Standard RFC 6298 exponential smoothing (alpha=1/8, beta=1/4).
- Karn's algorithm: Only unambiguous transmissions are sampled.
- Bounded ACK delay subtraction (capped at 5ms, 0.5ms minimum sample floor).
- Min RTO floor: 500 ms (0.5 s), Max RTO ceiling: 4.0 s.
- PTO: SRTT + 4 * RTTVAR + max_ack_delay with 200 ms floor.
- Adaptive reordering window with 16-flight clean decay engine.
"""

import math
from typing import Optional


class RttEstimator:
    """
    Maintains smoothed RTT (SRTT), RTT variation (RTTVAR), Retransmission Timeout (RTO),
    Probe Timeout (PTO), and adaptive reordering window multiplier.
    """

    def __init__(
        self,
        min_rto: float = 0.5,
        max_rto: float = 4.0,
        initial_rto: float = 1.0,
        clock_granularity: float = 0.001,
        max_ack_delay: float = 0.005,
    ):
        if min_rto <= 0 or max_rto <= min_rto:
            raise ValueError(f"Invalid RTO bounds: min={min_rto}, max={max_rto}")

        self.min_rto = float(min_rto)
        self.max_rto = float(max_rto)
        self.initial_rto = float(initial_rto)
        self.clock_granularity = float(clock_granularity)
        self.max_ack_delay = float(max_ack_delay)

        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        self.rto: float = float(initial_rto)

        # Adaptive reordering window state
        self.reordering_window_multiplier: float = 1.0
        self.clean_flight_count: int = 0

    @property
    def has_sample(self) -> bool:
        """Returns True if at least one valid RTT sample has been processed."""
        return self.srtt is not None

    def update_rtt(
        self,
        send_time: float,
        recv_time: float,
        ack_delay_ms: float = 0.0,
    ) -> Optional[float]:
        """
        Updates SRTT, RTTVAR, and RTO from an unambiguous transmission sample.

        Args:
            send_time: Monotonic timestamp when packet was sent.
            recv_time: Monotonic timestamp when ACK was received.
            ack_delay_ms: ACK delay reported by receiver in milliseconds.

        Returns:
            The calculated sample RTT in seconds, or None if invalid.
        """
        raw_rtt = recv_time - send_time
        if not math.isfinite(raw_rtt) or raw_rtt < 0:
            return None

        # Sanitize reported ack_delay_ms
        if not math.isfinite(ack_delay_ms) or ack_delay_ms < 0:
            ack_delay_sec = 0.0
        else:
            ack_delay_sec = min(ack_delay_ms / 1000.0, self.max_ack_delay)

        # Bounded ACK delay subtraction with 0.5 ms minimum floor
        if raw_rtt > ack_delay_sec:
            sample = max(0.0005, raw_rtt - ack_delay_sec)
        else:
            sample = max(0.0005, raw_rtt)

        # RFC 6298 smoothing
        if self.srtt is None or self.rttvar is None:
            self.srtt = sample
            self.rttvar = sample / 2.0
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample

        self.reset_rto()
        return sample

    def reset_rto(self) -> float:
        """Recalculates RTO from current SRTT and RTTVAR without backoff."""
        if self.srtt is not None and self.rttvar is not None:
            rto_calc = self.srtt + max(self.clock_granularity, 4.0 * self.rttvar)
            self.rto = max(self.min_rto, min(self.max_rto, rto_calc))
        else:
            self.rto = self.initial_rto
        return self.rto

    def backoff_rto(self) -> float:
        """Applies exponential backoff to RTO on timeout (doubles up to max_rto)."""
        self.rto = min(self.max_rto, self.rto * 2.0)
        return self.rto

    def get_pto(self) -> float:
        """
        Calculates Probe Timeout (PTO) duration.
        PTO = SRTT + 4 * RTTVAR + max_ack_delay, with 200ms floor.
        When unsampled, defaults to 200ms (0.2s) to probe tail before coarse RTO.
        """
        if self.srtt is not None and self.rttvar is not None:
            pto = self.srtt + 4.0 * self.rttvar + self.max_ack_delay
            return max(0.2, pto)
        else:
            return self.initial_rto

    def get_reordering_window(self) -> float:
        """
        Calculates RACK reordering window in seconds:
        reordering_window = max(multiplier * RTTVAR, 2ms).
        """
        if self.rttvar is not None:
            base = self.rttvar
        else:
            base = 0.01  # Default 10ms if uninitialized
        return max(0.002, self.reordering_window_multiplier * base)

    def on_spurious_retransmission(self) -> None:
        """
        Triggered when an incoming ACK arrives for an original packet previously marked lost.
        Expands the adaptive reordering multiplier and resets the 16-flight decay counter.
        """
        self.reordering_window_multiplier = min(4.0, self.reordering_window_multiplier + 1.0)
        self.clean_flight_count = 0

    def on_clean_flight(self) -> None:
        """
        Triggered when a recovery flight completes with zero spurious retransmissions.
        Every 16 consecutive clean flights decays the multiplier toward 1.0.
        """
        self.clean_flight_count += 1
        if self.clean_flight_count >= 16:
            self.reordering_window_multiplier = max(1.0, self.reordering_window_multiplier - 1.0)
            self.clean_flight_count = 0

    def on_unclean_flight(self) -> None:
        """Resets clean flight counter on any flight failure or spurious retransmission."""
        self.clean_flight_count = 0
