"""cycler_cli.py — CLI entry point for round-robin pipeline cycling."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pipewatch.cycler import Cycler
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-cycler",
        description="Round-robin cycle through pipeline metrics.",
    )
    p.add_argument("--steps", type=int, default=3, help="Number of cycle steps to simulate (default: 3)")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    p.add_argument("--all", dest="show_all", action="store_true", help="Show all pipelines at once (peek)")
    return p


def _demo_metrics() -> list:
    now = datetime.now(timezone.utc).isoformat()
    return [
        PipelineMetric("sales", "ok", 1000, 5, now),
        PipelineMetric("inventory", "warning", 500, 60, now),
        PipelineMetric("orders", "ok", 200, 1, now),
        PipelineMetric("returns", "error", 100, 90, now),
    ]


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    cycler = Cycler()
    cycler.load(_demo_metrics())

    if args.show_all:
        results = cycler.peek_all()
        if args.as_json:
            print(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            for r in results:
                print(f"[{r.position}/{r.total}] {r.pipeline}: {r.metric.status} ({r.metric.failed_records}/{r.metric.total_records} failed)")
        return 0

    records = []
    for step in range(args.steps):
        result = cycler.current() if step == 0 else cycler.next()
        if result is None:
            break
        records.append(result)

    if not records:
        print("No pipelines to cycle.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in records], indent=2))
    else:
        for r in records:
            print(f"Step [{r.position}/{r.total}] {r.pipeline}: status={r.metric.status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
