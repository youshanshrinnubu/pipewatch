"""CLI entry-point for the pipeline evaluator."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.evaluator import EvaluatorConfig, evaluate_all, format_evaluation
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-evaluate",
        description="Evaluate pipeline metrics against configurable thresholds.",
    )
    p.add_argument("--store", default="snapshot.json", help="Path to snapshot store file.")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline name.")
    p.add_argument("--tier", default=None, choices=["healthy", "warning", "critical"],
                   help="Only show results matching this tier.")
    p.add_argument("--warning-fr", type=float, default=0.05,
                   help="Failure rate threshold for warning tier (default 0.05).")
    p.add_argument("--critical-fr", type=float, default=0.20,
                   help="Failure rate threshold for critical tier (default 0.20).")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON.")
    return p


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.store)
    metrics: List[PipelineMetric] = store.load()

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline == args.pipeline]

    cfg = EvaluatorConfig(
        warning_failure_rate=args.warning_fr,
        critical_failure_rate=args.critical_fr,
    )

    results = evaluate_all(metrics, cfg)

    if args.tier:
        results = [r for r in results if r.tier == args.tier]

    if not results:
        print("No evaluation results.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_evaluation(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
