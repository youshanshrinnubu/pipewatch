"""CLI entry point for the classifier module."""
from __future__ import annotations
import argparse
import json
from pipewatch.metrics import PipelineMetric
from pipewatch.classifier import classify_all, format_classified


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Classify pipeline metrics into health tiers")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument(
        "--tier",
        choices=["healthy", "degraded", "critical", "unknown"],
        help="Filter output to a specific tier",
    )
    return p


def _demo_metrics():
    return [
        PipelineMetric("sales", 1000, 5, "ok"),
        PipelineMetric("inventory", 200, 40, "ok"),
        PipelineMetric("orders", 0, 0, "ok"),
        PipelineMetric("returns", 100, 60, "error"),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = _demo_metrics()
    results = classify_all(metrics)

    if args.tier:
        results = [r for r in results if r.tier == args.tier]

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_classified(results))


if __name__ == "__main__":
    main()
