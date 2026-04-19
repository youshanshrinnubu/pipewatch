"""CLI for patcher: apply patch rules to demo metrics."""
import argparse
import json
from pipewatch.patcher import PatchRule, patch_all, format_patch_results
from pipewatch.metrics import PipelineMetric
import time


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Patch pipeline metrics")
    p.add_argument("--pipeline", default=None, help="Target pipeline (default: all)")
    p.add_argument("--set-status", default=None, dest="set_status")
    p.add_argument("--set-failure-rate", type=float, default=None, dest="set_failure_rate")
    p.add_argument("--json", action="store_true")
    return p


def _demo_metrics():
    now = time.time()
    return [
        PipelineMetric("sales", "ok", 1000, 10, now),
        PipelineMetric("inventory", "ok", 500, 200, now),
        PipelineMetric("orders", "error", 300, 300, now),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = _demo_metrics()
    rules = [PatchRule(
        pipeline=args.pipeline,
        set_status=args.set_status,
        set_failure_rate=args.set_failure_rate,
    )]
    results = patch_all(metrics, rules)
    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_patch_results(results))


if __name__ == "__main__":
    main()
