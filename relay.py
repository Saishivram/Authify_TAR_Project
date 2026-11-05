"""
relay.py
========
Top-level CLI entry point for the TAR Network Impairment Relay (Phase 3).
"""

import argparse
import json
import sys
from typing import List

from transport.relay import (
    ImpairmentRelay,
    RelayConfig,
    RelayDirectionConfig,
    TargetedRule,
)


def parse_args():
    parser = argparse.ArgumentParser(description="TAR Network Impairment Relay (Phase 3)")
    parser.add_argument("--port-a", type=int, default=9002, help="Sender-facing bind port (default: 9002)")
    parser.add_argument("--port-b", type=int, default=9003, help="Receiver-facing bind port (default: 9003)")
    parser.add_argument("--sender-host", type=str, default="127.0.0.1", help="Sender host (default: 127.0.0.1)")
    parser.add_argument("--sender-port", type=int, default=9001, help="Sender port (default: 9001)")
    parser.add_argument("--receiver-host", type=str, default="127.0.0.1", help="Receiver host (default: 127.0.0.1)")
    parser.add_argument("--receiver-port", type=int, default=9004, help="Receiver port (default: 9004)")

    # Forward impairment arguments
    parser.add_argument("--loss", type=float, default=0.05, help="Forward packet loss rate [0.0 - 1.0] (default: 0.05)")
    parser.add_argument("--reorder", type=float, default=0.02, help="Forward packet reorder rate [0.0 - 1.0] (default: 0.02)")
    parser.add_argument("--reorder-min-delay", type=float, default=20.0, help="Min reorder delay in ms (default: 20.0)")
    parser.add_argument("--reorder-max-delay", type=float, default=50.0, help="Max reorder delay in ms (default: 50.0)")
    parser.add_argument("--stall-interval", type=float, default=5.0, help="Forward time-based stall interval in seconds (default: 5.0; 0 to disable)")
    parser.add_argument("--stall-duration", type=float, default=400.0, help="Forward stall duration in ms (default: 400.0)")
    parser.add_argument("--no-stall", action="store_true", help="Disable all periodic stalls on forward and reverse paths")

    # Reverse impairment arguments
    parser.add_argument("--reverse-loss", type=float, default=0.0, help="Reverse packet loss rate [0.0 - 1.0] (default: 0.0)")
    parser.add_argument("--reverse-reorder", type=float, default=0.0, help="Reverse packet reorder rate [0.0 - 1.0] (default: 0.0)")
    parser.add_argument("--reverse-stall-interval", type=float, default=0.0, help="Reverse stall interval in seconds (default: 0.0)")
    parser.add_argument("--reverse-stall-duration", type=float, default=400.0, help="Reverse stall duration in ms (default: 400.0)")

    # Control arguments
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic impairments (default: 42)")
    parser.add_argument("--idle-timeout", type=float, default=20.0, help="Idle timeout in seconds to auto-exit when quiet (default: 20.0)")
    parser.add_argument("--max-duration", type=float, default=None, help="Maximum run duration in seconds (default: unlimited)")
    parser.add_argument("--json-metrics", type=str, default=None, help="Optional output path to write JSON metrics upon exit")

    return parser.parse_args()


def main():
    args = parse_args()

    fwd_stall_interval = 0.0 if args.no_stall else args.stall_interval
    rev_stall_interval = 0.0 if args.no_stall else args.reverse_stall_interval

    forward_cfg = RelayDirectionConfig(
        loss_rate=args.loss,
        reorder_rate=args.reorder,
        reorder_min_delay_ms=args.reorder_min_delay,
        reorder_max_delay_ms=args.reorder_max_delay,
        stall_interval_sec=fwd_stall_interval,
        stall_duration_ms=args.stall_duration,
    )

    reverse_cfg = RelayDirectionConfig(
        loss_rate=args.reverse_loss,
        reorder_rate=args.reverse_reorder,
        reorder_min_delay_ms=args.reorder_min_delay,
        reorder_max_delay_ms=args.reorder_max_delay,
        stall_interval_sec=rev_stall_interval,
        stall_duration_ms=args.reverse_stall_duration,
    )

    config = RelayConfig(
        port_a=args.port_a,
        port_b=args.port_b,
        sender_addr=(args.sender_host, args.sender_port),
        receiver_addr=(args.receiver_host, args.receiver_port),
        seed=args.seed,
        forward=forward_cfg,
        reverse=reverse_cfg,
    )

    relay = ImpairmentRelay(config=config)

    try:
        metrics = relay.run(max_duration=args.max_duration, idle_timeout=args.idle_timeout)
        print("\n=== RELAY FINAL METRICS ===")
        print(json.dumps(metrics, indent=2))

        if args.json_metrics:
            with open(args.json_metrics, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2)

        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[Relay] Interrupted by user.")
        relay.close()
        sys.exit(130)
    except Exception as exc:
        print(f"\n[Relay] Fatal error: {exc}", file=sys.stderr)
        relay.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
