"""CLI for grouper — group metrics by status or pipeline."""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pipewatch.metrics import PipelineMetric
from pipewatch.grouper import (
    group_metrics_by_status,
    group_metrics_by_pipeline,
    format_groups,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Group pipeline metrics")
    p.add_argument("--by", choices=["status", "pipeline"], default="status")
    p.add_argument("--json", action="store_true", dest="as_json")
    return p


def _demo_metrics():
    now = datetime.now(timezone.utc)
    return [
        PipelineMetric("sales", 1000, 10, "ok", now),
        PipelineMetric("inventory", 500, 60, "ok", now),
        PipelineMetric("returns", 200, 180, "error", now),
        PipelineMetric("shipping", 800, 5, "ok", now),
        PipelineMetric("billing", 300, 90, "warn", now),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = _demo_metrics()

    if args.by == "status":
        groups = group_metrics_by_status(metrics)
    else:
        groups = group_metrics_by_pipeline(metrics)

    if args.as_json:
        print(json.dumps({k: v.to_dict() for k, v in groups.items()}, indent=2))
    else:
        print(format_groups(groups))


if __name__ == "__main__":
    main()
