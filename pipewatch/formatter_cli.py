"""CLI demo for the formatter module."""
from __future__ import annotations
import argparse
import sys
from pipewatch.formatter import format_records, FormatMode

_DEMO = [
    {"pipeline": "sales", "status": "ok", "failure_rate": 0.01, "total_records": 1000},
    {"pipeline": "inventory", "status": "warn", "failure_rate": 0.12, "total_records": 500},
    {"pipeline": "orders", "status": "error", "failure_rate": 0.45, "total_records": 200},
]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Format pipeline records in text, JSON, or CSV."
    )
    p.add_argument(
        "--format", choices=["text", "json", "csv"], default="text",
        dest="fmt", help="Output format (default: text)"
    )
    p.add_argument("--title", default="Pipeline Records",
                   help="Title shown in text mode")
    p.add_argument("--pipeline", default=None,
                   help="Filter to a single pipeline name")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    records = _DEMO
    if args.pipeline:
        records = [r for r in records if r["pipeline"] == args.pipeline]
    result = format_records(records, mode=args.fmt, title=args.title)
    print(result.output)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
