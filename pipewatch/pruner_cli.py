"""CLI entry point for pruning snapshot stores."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.pruner import prune_store
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Prune old metrics from a snapshot store.")
    p.add_argument("--store", default="pipewatch_snapshots.json", help="Path to snapshot file")
    p.add_argument("--max-age", type=float, default=None, help="Max age of metrics in seconds")
    p.add_argument("--max-count", type=int, default=None, help="Max number of metrics to keep per pipeline")
    p.add_argument("--json", action="store_true", help="Output results as JSON")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.max_age is None and args.max_count is None:
        print("Error: specify --max-age and/or --max-count", file=sys.stderr)
        return 1

    store = SnapshotStore(path=args.store)
    results = prune_store(store, max_age_seconds=args.max_age, max_count=args.max_count)

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        if not results:
            print("No pipelines found.")
        for r in results:
            print(f"{r.pipeline}: removed={r.removed}, retained={r.retained}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
