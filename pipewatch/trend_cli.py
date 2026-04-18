"""CLI for pipeline trend analysis."""
import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.trend import analyze_all_trends


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Analyze metric trends from snapshot store")
    p.add_argument("snapshot", help="Path to snapshot file")
    p.add_argument("--pipeline", help="Filter to a specific pipeline")
    p.add_argument("--threshold", type=float, default=0.05, help="Delta threshold for direction (default: 0.05)")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    p.add_argument("--degrading-only", action="store_true", help="Only show degrading pipelines")
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if not metrics:
        print("No metrics found in snapshot.", file=sys.stderr)
        return 1

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline_name == args.pipeline]

    results = analyze_all_trends(metrics, threshold=args.threshold)

    if args.degrading_only:
        results = [r for r in results if r.direction == "degrading"]

    if not results:
        print("No trend data to display.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            print(f"[{r.direction.upper()}] {r.pipeline} — avg={r.avg_failure_rate:.2%} "
                  f"min={r.min_failure_rate:.2%} max={r.max_failure_rate:.2%} samples={r.sample_count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
