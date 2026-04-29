"""clamper_cli.py — CLI entry point for the clamper module."""
from __future__ import annotations

import argparse
import json

from pipewatch.clamper import ClampConfig, clamp_all
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-clamper",
        description="Clamp pipeline metric fields to min/max bounds.",
    )
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--min-total", type=int, default=0, metavar="N")
    p.add_argument("--max-total", type=int, default=10_000_000, metavar="N")
    p.add_argument("--min-failed", type=int, default=0, metavar="N")
    p.add_argument("--max-failed", type=int, default=10_000_000, metavar="N")
    p.add_argument("--max-duration", type=float, default=None, metavar="SEC")
    p.add_argument("--changed-only", action="store_true", help="Only show metrics that were clamped.")
    return p


def _demo_metrics() -> list[PipelineMetric]:
    import datetime
    now = datetime.datetime.utcnow()
    return [
        PipelineMetric("orders", "ok", total_records=500, failed_records=5, timestamp=now),
        PipelineMetric("inventory", "ok", total_records=20_000_000, failed_records=0, timestamp=now),
        PipelineMetric("payments", "error", total_records=100, failed_records=150, timestamp=now),
        PipelineMetric("shipping", "ok", total_records=300, failed_records=2, duration_seconds=9999.0, timestamp=now),
    ]


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    cfg = ClampConfig(
        min_total_records=args.min_total,
        max_total_records=args.max_total,
        min_failed_records=args.min_failed,
        max_failed_records=args.max_failed,
        max_duration_seconds=args.max_duration,
    )
    metrics = _demo_metrics()
    results = clamp_all(metrics, cfg)

    if args.changed_only:
        results = [r for r in results if r.changed]

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2, default=str))
        return

    if not results:
        print("No metrics to display.")
        return

    for r in results:
        tag = "[CLAMPED]" if r.changed else "[OK]"
        fields = ", ".join(r.fields_changed) if r.changed else "—"
        print(f"{tag} {r.original.pipeline_name}: changed={fields}")
        if r.changed:
            o, c = r.original, r.clamped
            print(f"       total_records  : {o.total_records} -> {c.total_records}")
            print(f"       failed_records : {o.failed_records} -> {c.failed_records}")
            if "duration_seconds" in r.fields_changed:
                print(f"       duration_seconds: {o.duration_seconds} -> {c.duration_seconds}")


if __name__ == "__main__":
    main()
