"""
transport/relay.py
==================
Network Impairment Relay for the TAR (Time-Aware Reliable Transport) system (Phase 3).

Responsibilities:
1. Binds two independent UDP sockets on loopback:
   - Port A (Sender-facing): default 127.0.0.1:9002
   - Port B (Receiver-facing): default 127.0.0.1:9003
2. Intercepts and forwards datagrams in both directions:
   - Forward: Sender (9001) -> Relay Port A (9002) -> forwarded from Port B (9003) to Receiver (9004)
   - Reverse: Receiver (9004) -> Relay Port B (9003) -> forwarded from Port A (9002) to Sender (9001)
3. Preserves exact raw UDP datagram bytes without deserializing, modifying, or re-encoding envelopes.
4. Evaluates independent Forward and Reverse DirectionSchedulers with:
   - Configurable probabilistic packet loss (default 5%)
   - Configurable probabilistic packet reordering with delay jitter (default 2%, 20-50ms)
   - Configurable rate-based time-interval stalls (default 400ms duration, occasional interval)
   - Deterministic targeted fault injection (drop, delay, or stall specific packets by ID, type, or rule)
5. Min-Heap Relative-Offset Scheduler:
   - Accurately tracks boundary_offset for stalled/reordered packets.
   - Safely rebuilds heap during stall extensions without double-counting remaining stall time.
   - Strictly preserves configured reordering across stall boundaries.
   - Enforces that no packet is dispatched while its direction is stalled.
6. Separate, decoupled metric accounting for Forward and Reverse directions.
7. Supports pluggable / mockable monotonic clock for deterministic unit testing.
"""

from dataclasses import dataclass, field, replace
import json
import random
import select
import socket
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from transport.packet import DEFAULT_RECEIVE_BUFFER_SIZE


@dataclass
class TargetedRule:
    """Explicit fault injection rule matching specific packets."""
    action: str  # "DROP", "DELAY", "STALL"
    target_packet_id: Optional[int] = None
    target_record_id: Optional[int] = None
    target_type: Optional[str] = None  # "DATA", "START", "FIN", "FIN_ACK", "START_ACK", "ERROR"
    delay_ms: float = 0.0
    stall_ms: float = 0.0
    direction: str = "ANY"  # "FORWARD", "REVERSE", "ANY"

    def matches(self, pkt_info: Dict[str, Any], direction: str) -> bool:
        if self.direction != "ANY" and self.direction != direction:
            return False
        if self.target_packet_id is not None and pkt_info.get("packet_id") != self.target_packet_id:
            return False
        if self.target_record_id is not None and pkt_info.get("record_id") != self.target_record_id:
            return False
        if self.target_type is not None and pkt_info.get("type") != self.target_type:
            return False
        return True


@dataclass
class RelayDirectionConfig:
    loss_rate: float = 0.05            # Default: 5% packet loss
    reorder_rate: float = 0.02         # Default: 2% packet reordering
    reorder_min_delay_ms: float = 20.0 # Reorder jitter min
    reorder_max_delay_ms: float = 50.0 # Reorder jitter max
    stall_interval_sec: float = 5.0    # Time-based occasional stall interval (0 to disable)
    stall_duration_ms: float = 400.0   # 400 ms stall duration


@dataclass
class RelayConfig:
    port_a: int = 9002
    port_b: int = 9003
    sender_addr: Tuple[str, int] = ("127.0.0.1", 9001)
    receiver_addr: Tuple[str, int] = ("127.0.0.1", 9004)
    seed: Optional[int] = 42
    forward: RelayDirectionConfig = field(default_factory=RelayDirectionConfig)
    reverse: RelayDirectionConfig = field(default_factory=RelayDirectionConfig)
    targeted_rules: List[TargetedRule] = field(default_factory=list)


@dataclass(order=True)
class ScheduledItem:
    dispatch_time: float                   # Primary sort key (monotonic seconds)
    arrival_counter: int                   # Secondary sort key (arrival tie-breaker)
    raw_bytes: bytes = field(compare=False)
    target_addr: Tuple[str, int] = field(compare=False)
    egress_socket: Any = field(compare=False)
    direction: str = field(compare=False)
    packet_info: Dict[str, Any] = field(compare=False)
    reorder_jitter_sec: float = field(compare=False, default=0.0)
    boundary_offset: Optional[float] = field(compare=False, default=None)
    is_reorder_selected: bool = field(compare=False, default=False)


def peek_packet_info(raw_bytes: bytes) -> Dict[str, Any]:
    """
    Safely inspects JSON packet headers without modifying raw bytes.
    Handles non-JSON or malformed datagrams cleanly.
    """
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
        if isinstance(data, dict):
            return {
                "type": data.get("type"),
                "connection_id": data.get("connection_id"),
                "packet_id": data.get("packet_id"),
                "record_id": data.get("record_id"),
                "byte_len": len(raw_bytes),
                "valid": True,
            }
    except Exception:
        pass
    return {"byte_len": len(raw_bytes), "valid": False}


class DirectionScheduler:
    """
    Independent scheduling engine for a single traffic direction (FORWARD or REVERSE).
    Implements min-heap relative-offset scheduling with safe stall shifting.
    """

    def __init__(
        self,
        direction_name: str,
        config: RelayDirectionConfig,
        rng: random.Random,
        time_func: Callable[[], float] = time.monotonic,
    ):
        self.direction = direction_name
        self.config = config
        self.rng = rng
        self.time_func = time_func

        self.heap: List[ScheduledItem] = []
        self.stall_end_time: float = 0.0
        self.arrival_counter: int = 0
        self.last_stall_trigger_time: float = self.time_func()

        # Sequence tracking for meaningful session-scoped out-of-order detection
        self.highest_dispatched_by_session: Dict[str, int] = {}

        # Separate per-direction metric accounting
        self.metrics: Dict[str, Any] = {
            "packets_received": 0,
            "packets_forwarded": 0,
            "packets_dropped_loss": 0,
            "packets_dropped_targeted": 0,
            "packets_delayed_for_reordering": 0,
            "packets_dispatched_out_of_order": 0,
            "packets_delayed_by_stall": 0,
            "stalls_triggered": 0,
            "total_bytes_received": 0,
            "total_bytes_forwarded": 0,
        }

    def trigger_stall(self, duration_sec: float, now: float) -> float:
        """
        Triggers or extends a link stall for this direction.
        Preserves relative release-time offsets of scheduled packets without
        adding remaining duration of the old stall a second time.
        Safely rebuilds the min-heap.
        """
        import heapq

        if now < self.stall_end_time:
            # Overlapping stall: extend existing stall boundary cleanly
            new_stall_end = self.stall_end_time + duration_sec
        else:
            new_stall_end = now + duration_sec

        # Rebuild heap safely: adjust dispatch times of items falling inside new stall window
        new_heap: List[ScheduledItem] = []
        for item in self.heap:
            if item.dispatch_time < new_stall_end:
                # If item already had a boundary offset from an existing stall, preserve it
                if item.boundary_offset is not None:
                    offset = item.boundary_offset
                else:
                    # Original offset relative to the stall trigger point
                    offset = max(0.0, item.dispatch_time - now)

                new_dispatch_time = new_stall_end + offset
                new_item = replace(
                    item,
                    dispatch_time=new_dispatch_time,
                    boundary_offset=offset,
                )
                self.metrics["packets_delayed_by_stall"] += 1
                new_heap.append(new_item)
            else:
                new_heap.append(item)

        heapq.heapify(new_heap)
        self.heap = new_heap
        self.stall_end_time = new_stall_end
        self.metrics["stalls_triggered"] += 1
        return self.stall_end_time

    def handle_packet(
        self,
        raw_bytes: bytes,
        target_addr: Tuple[str, int],
        egress_socket: Any,
        now: float,
        targeted_rules: Optional[List[TargetedRule]] = None,
    ) -> Optional[ScheduledItem]:
        """
        Processes an incoming datagram through the impairment pipeline:
        1. Peeks headers.
        2. Evaluates targeted fault injection rules.
        3. Evaluates probabilistic loss.
        4. Evaluates rate-based time-interval stalls.
        5. Evaluates delay jitter / reordering.
        6. Schedules dispatch into the min-heap.
        Returns the ScheduledItem, or None if dropped.
        """
        import heapq

        self.arrival_counter += 1
        self.metrics["packets_received"] += 1
        self.metrics["total_bytes_received"] += len(raw_bytes)

        pkt_info = peek_packet_info(raw_bytes)

        # 1. Evaluate Targeted Rules
        targeted_rule = None
        if targeted_rules:
            for rule in targeted_rules:
                if rule.matches(pkt_info, self.direction):
                    targeted_rule = rule
                    break

        if targeted_rule:
            if targeted_rule.action == "DROP":
                self.metrics["packets_dropped_targeted"] += 1
                return None
            elif targeted_rule.action == "STALL":
                duration_sec = targeted_rule.stall_ms / 1000.0
                self.trigger_stall(duration_sec, now)

        # 2. Evaluate Probabilistic Packet Loss (if not targeted)
        if not targeted_rule and self.config.loss_rate > 0.0:
            if self.rng.random() < self.config.loss_rate:
                self.metrics["packets_dropped_loss"] += 1
                return None

        # 3. Evaluate Rate-Based Time-Interval Stall
        # Note: A stall may trigger on the first packet if the configured interval
        # has already elapsed since scheduler initialization.
        if self.config.stall_interval_sec > 0:
            if (now - self.last_stall_trigger_time) >= self.config.stall_interval_sec:
                duration_sec = self.config.stall_duration_ms / 1000.0
                end_time = self.trigger_stall(duration_sec, now)
                self.last_stall_trigger_time = end_time

        # 4. Evaluate Reordering / Delay Jitter
        is_reorder = False
        reorder_jitter_sec = 0.0

        if targeted_rule and targeted_rule.action == "DELAY":
            reorder_jitter_sec = max(0.0, targeted_rule.delay_ms / 1000.0)
            is_reorder = True
            self.metrics["packets_delayed_for_reordering"] += 1
        elif self.config.reorder_rate > 0.0 and self.rng.random() < self.config.reorder_rate:
            min_s = self.config.reorder_min_delay_ms / 1000.0
            max_s = self.config.reorder_max_delay_ms / 1000.0
            reorder_jitter_sec = self.rng.uniform(min_s, max_s)
            is_reorder = True
            self.metrics["packets_delayed_for_reordering"] += 1

        # 5. Compute Scheduled Dispatch Time
        if now < self.stall_end_time:
            # Active stall: schedule relative to stall boundary
            boundary_offset = reorder_jitter_sec
            dispatch_time = self.stall_end_time + reorder_jitter_sec
            self.metrics["packets_delayed_by_stall"] += 1
        else:
            boundary_offset = None
            dispatch_time = now + reorder_jitter_sec

        item = ScheduledItem(
            dispatch_time=dispatch_time,
            arrival_counter=self.arrival_counter,
            raw_bytes=raw_bytes,
            target_addr=target_addr,
            egress_socket=egress_socket,
            direction=self.direction,
            packet_info=pkt_info,
            reorder_jitter_sec=reorder_jitter_sec,
            boundary_offset=boundary_offset,
            is_reorder_selected=is_reorder,
        )

        heapq.heappush(self.heap, item)
        return item

    def pop_eligible(self, now: float) -> List[ScheduledItem]:
        """
        Pops and returns all scheduled items that are eligible for dispatch at monotonic `now`.
        Strict constraint: Ensures NO packet is dispatched while this direction is stalled.
        Updates session-scoped out-of-order metrics.
        """
        import heapq

        # If direction is currently stalled, no packets may leave
        if now < self.stall_end_time:
            return []

        eligible: List[ScheduledItem] = []
        while self.heap and self.heap[0].dispatch_time <= now:
            item = heapq.heappop(self.heap)
            eligible.append(item)

            self.metrics["packets_forwarded"] += 1
            self.metrics["total_bytes_forwarded"] += len(item.raw_bytes)

            # Check meaningful out-of-order delivery within the same session
            cid = item.packet_info.get("connection_id")
            pid = item.packet_info.get("packet_id")
            ptype = item.packet_info.get("type")

            # Only track monotonic sequence inversions for packets carrying session packet_ids
            if cid and pid is not None and ptype in ("DATA", "FIN"):
                highest = self.highest_dispatched_by_session.get(cid, 0)
                if pid < highest:
                    self.metrics["packets_dispatched_out_of_order"] += 1
                else:
                    self.highest_dispatched_by_session[cid] = pid

        return eligible

    def next_wait_seconds(self, now: float, max_wait: float = 0.1) -> float:
        """Computes optimal sleep/select timeout until next scheduled event."""
        if now < self.stall_end_time:
            return max(0.0, min(self.stall_end_time - now, max_wait))
        if self.heap:
            return max(0.0, min(self.heap[0].dispatch_time - now, max_wait))
        return max_wait


class ImpairmentRelay:
    """
    Standard-library UDP Network Impairment Relay (Phase 3).
    Binds Port A (9002) and Port B (9003) and routes traffic bi-directionally
    through independent Forward and Reverse DirectionSchedulers.
    """

    def __init__(
        self,
        config: Optional[RelayConfig] = None,
        time_func: Callable[[], float] = time.monotonic,
    ):
        self.config = config or RelayConfig()
        self.time_func = time_func

        self.rng = random.Random(self.config.seed)
        self.forward_scheduler = DirectionScheduler(
            direction_name="FORWARD",
            config=self.config.forward,
            rng=self.rng,
            time_func=self.time_func,
        )
        self.reverse_scheduler = DirectionScheduler(
            direction_name="REVERSE",
            config=self.config.reverse,
            rng=self.rng,
            time_func=self.time_func,
        )

        self.sock_a: Optional[socket.socket] = None
        self.sock_b: Optional[socket.socket] = None
        self.running: bool = False

    def start_sockets(self) -> None:
        """Initializes and binds UDP sockets for Port A and Port B."""
        # Port A: Sender-facing socket (127.0.0.1:9002)
        self.sock_a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.sock_a.ioctl(socket.SIO_UDP_CONNRESET, False)
            except OSError:
                pass
        try:
            self.sock_a.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        except OSError:
            pass
        self.sock_a.bind(("127.0.0.1", self.config.port_a))
        self.sock_a.setblocking(False)

        # Port B: Receiver-facing socket (127.0.0.1:9003)
        self.sock_b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.sock_b.ioctl(socket.SIO_UDP_CONNRESET, False)
            except OSError:
                pass
        try:
            self.sock_b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        except OSError:
            pass
        self.sock_b.bind(("127.0.0.1", self.config.port_b))
        self.sock_b.setblocking(False)

    def close(self) -> None:
        """Closes all relay sockets cleanly."""
        self.running = False
        if self.sock_a:
            try:
                self.sock_a.close()
            except Exception:
                pass
            self.sock_a = None
        if self.sock_b:
            try:
                self.sock_b.close()
            except Exception:
                pass
            self.sock_b = None

    def stop(self) -> None:
        """Signals the relay to stop cleanly."""
        self.close()

    def poll_and_dispatch(self, now: Optional[float] = None) -> int:
        """
        Executes one non-blocking tick of socket reads and min-heap dispatches.
        Returns the number of datagrams actually transmitted during this tick.
        """
        if now is None:
            now = self.time_func()

        dispatched_count = 0

        # 1. Read available incoming datagrams from Port A (Forward: Sender -> Receiver)
        if self.sock_a:
            while True:
                try:
                    raw_bytes, sender_addr = self.sock_a.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                    # Forward direction egress is Port B targeting receiver_addr
                    self.forward_scheduler.handle_packet(
                        raw_bytes=raw_bytes,
                        target_addr=self.config.receiver_addr,
                        egress_socket=self.sock_b,
                        now=now,
                        targeted_rules=self.config.targeted_rules,
                    )
                except (BlockingIOError, socket.error):
                    break

        # 2. Read available incoming datagrams from Port B (Reverse: Receiver -> Sender)
        if self.sock_b:
            while True:
                try:
                    raw_bytes, recv_addr = self.sock_b.recvfrom(DEFAULT_RECEIVE_BUFFER_SIZE)
                    # Reverse direction egress is Port A targeting sender_addr
                    self.reverse_scheduler.handle_packet(
                        raw_bytes=raw_bytes,
                        target_addr=self.config.sender_addr,
                        egress_socket=self.sock_a,
                        now=now,
                        targeted_rules=self.config.targeted_rules,
                    )
                except (BlockingIOError, socket.error):
                    break

        # 3. Dispatch eligible items from Forward Scheduler
        for item in self.forward_scheduler.pop_eligible(now):
            try:
                item.egress_socket.sendto(item.raw_bytes, item.target_addr)
                dispatched_count += 1
            except Exception as exc:
                print(f"[Relay] Error sending forward datagram: {exc}", file=sys.stderr)

        # 4. Dispatch eligible items from Reverse Scheduler
        for item in self.reverse_scheduler.pop_eligible(now):
            try:
                item.egress_socket.sendto(item.raw_bytes, item.target_addr)
                dispatched_count += 1
            except Exception as exc:
                print(f"[Relay] Error sending reverse datagram: {exc}", file=sys.stderr)

        return dispatched_count

    def run(self, max_duration: Optional[float] = None, idle_timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Main non-blocking event loop using select.select() for sub-millisecond precision.
        Runs until max_duration expires, idle_timeout occurs with empty queues, or close() is called.
        """
        if not self.sock_a or not self.sock_b:
            self.start_sockets()

        self.running = True
        start_time = self.time_func()
        last_activity_time = start_time

        print(
            f"[Relay] Running Network Impairment Relay (seed={self.config.seed})...\n"
            f"  Port A (Sender-facing):   127.0.0.1:{self.config.port_a} -> targets {self.config.receiver_addr}\n"
            f"  Port B (Receiver-facing): 127.0.0.1:{self.config.port_b} -> targets {self.config.sender_addr}\n"
            f"  Forward: loss={self.config.forward.loss_rate*100}%, reorder={self.config.forward.reorder_rate*100}%, "
            f"stall_interval={self.config.forward.stall_interval_sec}s ({self.config.forward.stall_duration_ms}ms)\n"
            f"  Reverse: loss={self.config.reverse.loss_rate*100}%, reorder={self.config.reverse.reorder_rate*100}%, "
            f"stall_interval={self.config.reverse.stall_interval_sec}s ({self.config.reverse.stall_duration_ms}ms)"
        )

        try:
            while self.running:
                now = self.time_func()

                if max_duration and (now - start_time) > max_duration:
                    print(f"[Relay] Max duration of {max_duration}s reached.")
                    break

                # Compute optimal sleep time from both schedulers
                wait_fwd = self.forward_scheduler.next_wait_seconds(now)
                wait_rev = self.reverse_scheduler.next_wait_seconds(now)
                timeout = min(wait_fwd, wait_rev, 0.05)

                socks = [s for s in (self.sock_a, self.sock_b) if s is not None]
                if not socks:
                    break

                try:
                    readable, _, _ = select.select(socks, [], [], timeout)
                except OSError:
                    break

                dispatched = self.poll_and_dispatch(now=self.time_func())

                if readable or dispatched > 0:
                    last_activity_time = self.time_func()
                elif idle_timeout and (self.time_func() - last_activity_time) > idle_timeout:
                    # Check if queues are drained
                    if not self.forward_scheduler.heap and not self.reverse_scheduler.heap:
                        print(f"[Relay] Idle timeout of {idle_timeout}s reached with drained queues.")
                        break

        finally:
            self.close()

        return self.get_metrics()

    def get_metrics(self) -> Dict[str, Any]:
        """Returns comprehensive separated metrics for both directions."""
        return {
            "relay": {
                "port_a": self.config.port_a,
                "port_b": self.config.port_b,
                "sender_addr": f"{self.config.sender_addr[0]}:{self.config.sender_addr[1]}",
                "receiver_addr": f"{self.config.receiver_addr[0]}:{self.config.receiver_addr[1]}",
                "seed": self.config.seed,
            },
            "forward": dict(self.forward_scheduler.metrics),
            "reverse": dict(self.reverse_scheduler.metrics),
        }
