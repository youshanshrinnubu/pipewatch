"""CLI entry-point for sentinel threshold checks."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.metrics import PipelineMetric
from pipewatch.sentinel import SentinelRule, check_all_sentinels


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Check pipeline metrics against sentinel threshold rules."
    )
    p.add_argument(
        "--max-failure-rate",
        type=float,
        default=0.1,
        metavar="RATE",
        help="Maximum allowed failure rate (default: 0.10)",
    )
    p.add_argument(
        "--max-failed-records",
        type=int,
        default=None,
        metavar="N",
        help="Maximum allowed failed record count",
    )
    p.add_argument(
        "--forbidden-status",
        nargs="*",
        default=["error"],
        metavar="STATUS",
        help="Statuses that are forbidden (default: error)",
    )
    p.add_argument(
        "--triggered-only",
        action="store_true",
        help="Only show results where sentinel was triggered",
    )
    p.add_argument("--json", action="store_true", help="Output as JSON")
    return p


def _demo_metrics() -> List[PipelineMetric]:
    return [
        PipelineMetric(pipeline="sales", total_records=1000, failed_records=5, status="ok"),
        PipelineMetric(pipeline="inventory", total_records=200, failed_records=60, status="warning"),
        PipelineMetric(pipeline="orders", total_records=0, failed_records=0, status="error"),
    ]


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    metrics = _demo_metrics()
    rule = SentinelRule(
        pipeline=None,
        max_failure_rate=args.max_failure_rate,
        max_failed_records=args.max_failed_records,
        forbidden_statuses=args.forbidden_status or [],
    )
    results = check_all_sentinels(metrics, [rule])

    if args.triggered_only:
        results = [r for r in results if r.triggered]

    if not results:
        print("No sentinel results.")
        return 0

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            status = "TRIGGERED" if r.triggered else "ok"
            detail = "; ".join(r.violations) if r.violations else "within limits"
            print(f"[{status}] {r.metric.pipeline}: {detail}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
