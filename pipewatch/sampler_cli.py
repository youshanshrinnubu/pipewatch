"""CLI entry point for the metric sampler."""
from __future__ import annotations
import argparse
import json
from pipewatch.metrics import PipelineMetric
from pipewatch.sampler import sample_all, format_sampled
import time


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Sample pipeline metrics")
    p.add_argument("--n", type=int, default=3, help="Max samples per pipeline")
    p.add_argument("--seed", type=int, default=None, help="Random seed")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    return p


def _demo_metrics() -> list:
    now = time.time()
    rows = []
    for i in range(8):
        rows.append(PipelineMetric(
            pipeline="sales" if i % 2 == 0 else "inventory",
            timestamp=now - i * 60,
            status="ok" if i < 6 else "error",
            total_records=100,
            failed_records=i,
        ))
    return rows


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = _demo_metrics()
    results = sample_all(metrics, n=args.n, seed=args.seed)
    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_sampled(results))


if __name__ == "__main__":
    main()
