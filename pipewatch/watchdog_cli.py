"""CLI entry point for watchdog staleness checks."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta

from pipewatch.metrics import PipelineMetric
from pipewatch.watchdog import check_staleness


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Check for stale pipelines")
    p.add_argument("--warning", type=float, default=300.0, help="Warning threshold in seconds")
    p.add_argument("--critical", type=float, default=900.0, help="Critical threshold in seconds")
    p.add_argument("--stale-only", action="store_true", help="Only show stale pipelines")
    p.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")
    return p


def _make_demo_metrics() -> list[PipelineMetric]:
    now = datetime.now(timezone.utc)
    return [
        PipelineMetric("fresh_pipe", "ok", 1000, 5, now - timedelta(seconds=60)),
        PipelineMetric("warn_pipe", "ok", 500, 10, now - timedelta(seconds=400)),
        PipelineMetric("dead_pipe", "error", 0, 0, now - timedelta(seconds=1000)),
    ]


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    metrics = _make_demo_metrics()
    results = check_staleness(metrics, args.warning, args.critical)

    if args.stale_only:
        results = [r for r in results if r.is_stale]

    if not results:
        print("No stale pipelines detected.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            flag = f"[{r.severity.upper()}]" if r.is_stale else "[OK]"
            print(f"{flag} {r.pipeline}: last seen {r.age_seconds:.0f}s ago")
    return 0


if __name__ == "__main__":
    sys.exit(main())
