"""CLI entry point for exporting pipewatch data."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.exporter import (
    export_metrics_csv,
    export_metrics_json,
    export_summary_json,
    write_export,
)
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-export",
        description="Export pipeline metrics from snapshot store.",
    )
    p.add_argument("--config", required=True, help="Path to config JSON file")
    p.add_argument(
        "--format",
        choices=["json", "csv", "summary"],
        default="json",
        help="Output format (default: json)",
    )
    p.add_argument("--output", help="Write output to file instead of stdout")
    p.add_argument("--pipeline", help="Filter to a specific pipeline name")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        watcher_cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 1

    store = SnapshotStore(watcher_cfg.snapshot_path)
    metrics = store.load_all()

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline_name == args.pipeline]

    fmt = args.format
    if fmt == "json":
        content = export_metrics_json(metrics)
    elif fmt == "csv":
        content = export_metrics_csv(metrics)
    else:
        content = export_summary_json(metrics)

    if args.output:
        write_export(content, args.output)
        print(f"Exported to {args.output}")
    else:
        print(content)

    return 0


if __name__ == "__main__":
    sys.exit(main())
