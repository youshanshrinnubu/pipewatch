"""CLI entry point for the pipeline profiler."""
from __future__ import annotations
import argparse
import json
from pipewatch.profiler import profile_all, format_profiled
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Profile pipeline throughput from snapshot")
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--grade",
        choices=["fast", "moderate", "slow"],
        default=None,
        help="Filter by grade",
    )
    return p


def main(argv=None) -> None:
    args = _build_parser().parse_args(argv)
    try:
        with open(args.snapshot) as f:
            raw = json.load(f)
    except FileNotFoundError:
        print(f"Error: file not found: {args.snapshot}")
        return

    metrics = [PipelineMetric(**entry) for entry in raw]
    results = profile_all(metrics)

    if args.grade:
        results = [r for r in results if r.grade == args.grade]

    if not results:
        print("No profiling results to display.")
        return

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_profiled(results))


if __name__ == "__main__":
    main()
