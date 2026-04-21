"""mapper_cli.py — CLI entry point for the metric mapper."""
from __future__ import annotations

import argparse
import dataclasses
from datetime import datetime, timezone

from pipewatch.mapper import MapRule, Mapper, format_mapped
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-mapper",
        description="Map pipeline metrics through transformation rules.",
    )
    p.add_argument("--fmt", choices=["text", "json"], default="text")
    p.add_argument(
        "--cap-failed",
        type=int,
        default=None,
        metavar="N",
        help="Cap failed_records to at most N.",
    )
    p.add_argument(
        "--prefix",
        default=None,
        metavar="PREFIX",
        help="Only apply cap rule to pipelines with this prefix.",
    )
    return p


def _demo_metrics() -> list:
    now = datetime.now(timezone.utc)
    return [
        PipelineMetric("sales_daily", "ok", 1000, 5, now),
        PipelineMetric("inventory_sync", "warning", 500, 120, now),
        PipelineMetric("orders_etl", "error", 200, 200, now),
    ]


def main(argv=None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    metrics = _demo_metrics()
    mapper = Mapper()

    if args.cap_failed is not None:
        cap = args.cap_failed
        prefix = args.prefix

        def _cap_transform(m: PipelineMetric) -> PipelineMetric:
            if m.failed_records <= cap:
                return m
            d = dataclasses.asdict(m)
            d["failed_records"] = cap
            return PipelineMetric(**d)

        mapper.add_rule(MapRule(
            pipeline_prefix=prefix,
            transform=_cap_transform,
            label=f"cap_failed<={cap}",
        ))

    results = mapper.apply_all(metrics)
    print(format_mapped(results, fmt=args.fmt))


if __name__ == "__main__":  # pragma: no cover
    main()
