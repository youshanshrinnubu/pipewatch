"""CLI demo for the throttle module."""
from __future__ import annotations

import argparse
import json

from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric
from pipewatch.throttle import Throttle, ThrottleConfig


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Demonstrate alert throttling")
    p.add_argument("--window", type=float, default=60.0, help="Throttle window in seconds")
    p.add_argument("--max", type=int, default=3, dest="max_per_window", help="Max alerts per window")
    p.add_argument("--count", type=int, default=5, help="Number of alerts to simulate")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    return p


def main(argv=None) -> None:
    args = _build_parser().parse_args(argv)
    cfg = ThrottleConfig(window_seconds=args.window, max_per_window=args.max_per_window)
    throttle = Throttle(cfg)

    metric = PipelineMetric(pipeline="demo", status="ok", records=100, failures=5)
    alert = Alert(pipeline="demo", severity="warning", message="high failure rate", metric=metric)

    results = []
    for i in range(args.count):
        allowed = throttle.allow(alert)
        st = throttle.status(alert)
        results.append({"attempt": i + 1, "allowed": allowed, **st})

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            flag = "ALLOW" if r["allowed"] else "BLOCK"
            print(f"[{flag}] attempt={r['attempt']} count={r['count']} remaining={r['remaining']}")


if __name__ == "__main__":
    main()
