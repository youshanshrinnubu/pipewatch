"""CLI entry point for aggregated pipeline stats."""
import argparse
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.aggregator import aggregate_all, format_aggregated


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-aggregate",
        description="Show aggregated stats from stored pipeline snapshots.",
    )
    parser.add_argument(
        "--snapshot",
        default="pipewatch_snapshots.json",
        help="Path to snapshot file (default: pipewatch_snapshots.json)",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a specific pipeline name",
    )
    return parser


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(path=args.snapshot)
    metrics = store.load_all()

    if not metrics:
        print("No snapshots found.", file=sys.stderr)
        return 1

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline == args.pipeline]
        if not metrics:
            print(f"No snapshots for pipeline: {args.pipeline}", file=sys.stderr)
            return 1

    aggregated = aggregate_all(metrics)

    for i, (name, stats) in enumerate(sorted(aggregated.items())):
        if i > 0:
            print()
        print(format_aggregated(stats))

    return 0


if __name__ == "__main__":
    sys.exit(main())
