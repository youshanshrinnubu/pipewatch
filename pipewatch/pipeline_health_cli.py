"""CLI for pipeline health evaluation."""
import argparse
import json
import sys
from pipewatch.pipeline_health import evaluate_all, format_health_report
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evaluate pipeline health from snapshot")
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument("--pipeline", help="Filter to a specific pipeline")
    p.add_argument("--status", choices=["healthy", "warning", "critical"], help="Filter by status")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if not metrics:
        print("No metrics found in snapshot.", file=sys.stderr)
        sys.exit(1)

    reports = evaluate_all(metrics)

    if args.pipeline:
        reports = [r for r in reports if r.pipeline == args.pipeline]
    if args.status:
        reports = [r for r in reports if r.status == args.status]

    if not reports:
        print("No matching pipelines.")
        return

    if args.as_json:
        print(json.dumps([r.to_dict() for r in reports], indent=2))
    else:
        for r in reports:
            print(format_health_report(r))
            print()


if __name__ == "__main__":
    main()
