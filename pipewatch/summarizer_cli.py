"""CLI entry point for the summarizer module."""
from __future__ import annotations
import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.summarizer import summarize_metrics, format_summarizer_result


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-summarizer",
        description="Summarize pipeline health from a snapshot store.",
    )
    p.add_argument("--store", default="snapshot.json", help="Path to snapshot JSON file")
    p.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt",
        help="Output format (default: text)"
    )
    p.add_argument(
        "--tier", choices=["healthy", "warning", "critical"],
        default=None, help="Filter output lines by tier"
    )
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.store)
    metrics = store.load()

    if not metrics:
        print("No metrics found.", file=sys.stderr)
        return 0

    result = summarize_metrics(metrics)

    if args.tier:
        from pipewatch.summarizer import _tier, failure_rate
        result.lines = [
            ln for ln in result.lines
            if _tier(ln.failure_rate, ln.status) == args.tier
        ]

    if args.fmt == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(format_summarizer_result(result))

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
