"""CLI entry-point for pipewatch.stacker."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.metrics import PipelineMetric
from pipewatch.stacker import Stacker


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-stacker",
        description="Accumulate pipeline metrics into fixed-depth stacks.",
    )
    p.add_argument(
        "--capacity", type=int, default=5,
        help="Maximum entries per pipeline stack (default: 5).",
    )
    p.add_argument(
        "--json", dest="as_json", action="store_true",
        help="Emit JSON output.",
    )
    return p


def _demo_metrics() -> list:
    """Return a small demo dataset for CLI smoke-testing."""
    import time
    base = int(time.time())
    rows = [
        ("sales", "ok", 1000, 10),
        ("sales", "ok", 1100, 5),
        ("sales", "warning", 900, 90),
        ("inventory", "ok", 500, 2),
        ("inventory", "error", 0, 0),
    ]
    metrics = []
    for i, (name, status, total, failed) in enumerate(rows):
        metrics.append(
            PipelineMetric(
                pipeline_name=name,
                status=status,
                total_records=total,
                failed_records=failed,
                timestamp=base + i,
            )
        )
    return metrics


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    metrics = _demo_metrics()
    stacker = Stacker(capacity=args.capacity)
    stacker.push_all(metrics)

    if args.as_json:
        output = [s.to_dict() for s in stacker.all_stacks()]
        print(json.dumps(output, indent=2))
    else:
        print(stacker.format_text())

    return 0


if __name__ == "__main__":
    sys.exit(main())
