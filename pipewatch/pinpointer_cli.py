"""CLI entry-point for the pinpointer module."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.pinpointer import pinpoint_all, to_dict
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Identify the single worst metric per pipeline."
    )
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument(
        "--pipeline",
        metavar="NAME",
        help="Restrict output to a single pipeline",
    )
    p.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit JSON output",
    )
    p.add_argument(
        "--min-score",
        type=float,
        default=None,
        metavar="FLOAT",
        help="Only show results with score <= this value (0.0–1.0)",
    )
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if not metrics:
        print("No metrics found.", file=sys.stderr)
        return 1

    results = pinpoint_all(metrics)

    if args.pipeline:
        results = {k: v for k, v in results.items() if k == args.pipeline}

    if args.min_score is not None:
        results = {
            k: v for k, v in results.items() if v.score <= args.min_score
        }

    if not results:
        print("No pinpoint results match the given criteria.")
        return 0

    if args.as_json:
        print(json.dumps([to_dict(v) for v in results.values()], indent=2))
    else:
        for r in results.values():
            print(
                f"{r.pipeline}: score={r.score:.2f}  status={r.worst_metric.status}"
                f"  reason={r.reason}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
