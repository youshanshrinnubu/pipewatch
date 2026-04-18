"""CLI for comparing two snapshot files."""

import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.comparator import compare_snapshots, format_comparison


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Compare two pipewatch snapshot files")
    p.add_argument("before", help="Path to the 'before' snapshot file")
    p.add_argument("after", help="Path to the 'after' snapshot file")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--changed-only", action="store_true",
        help="Only show pipelines with status change or failure rate delta"
    )
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    before_store = SnapshotStore(args.before)
    after_store = SnapshotStore(args.after)

    before_metrics = before_store.load()
    after_metrics = after_store.load()

    if not before_metrics and not after_metrics:
        print("Both snapshots are empty.")
        sys.exit(0)

    results = compare_snapshots(before_metrics, after_metrics)

    if args.changed_only:
        results = [
            r for r in results
            if r.status_changed or abs(r.failure_rate_delta) > 0 or r.only_in_before or r.only_in_after
        ]

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_comparison(results))


if __name__ == "__main__":
    main()
