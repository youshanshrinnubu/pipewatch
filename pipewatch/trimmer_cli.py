"""trimmer_cli.py — CLI entry point for the trimmer module."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.snapshot import SnapshotStore
from pipewatch.trimmer import TrimConfig, format_trimmed, trim_all


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-trimmer",
        description="Remove low-signal metrics based on quality thresholds.",
    )
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument(
        "--min-records",
        type=int,
        default=0,
        metavar="N",
        help="Minimum total_records required to keep a metric (default: 0)",
    )
    p.add_argument(
        "--max-failure-rate",
        type=float,
        default=1.0,
        metavar="F",
        help="Maximum allowed failure rate 0-1 (default: 1.0)",
    )
    p.add_argument(
        "--require-status",
        nargs="+",
        metavar="STATUS",
        help="Only keep metrics with these statuses",
    )
    p.add_argument(
        "--json", action="store_true", dest="as_json", help="Output as JSON"
    )
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    config = TrimConfig(
        min_total_records=args.min_records,
        max_failure_rate=args.max_failure_rate,
        require_status=args.require_status,
    )

    results = trim_all(metrics, config)

    if not results:
        print("No metrics to trim.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_trimmed(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
