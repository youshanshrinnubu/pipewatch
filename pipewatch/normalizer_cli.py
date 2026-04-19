"""CLI entry point for the normalizer module."""
from __future__ import annotations
import argparse
import json
from pipewatch.snapshot import SnapshotStore
from pipewatch.normalizer import normalize_all, format_normalized


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-normalizer",
        description="Normalize pipeline metrics to a [0,1] score.",
    )
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument("--pipeline", help="Filter to a single pipeline")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument(
        "--min-score",
        type=float,
        default=0.0,
        metavar="SCORE",
        help="Only show results with normalized_score <= SCORE (highlight poor pipelines)",
    )
    return p


def main(argv=None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline == args.pipeline]

    results = normalize_all(metrics)

    if args.min_score < 1.0:
        results = [r for r in results if r.normalized_score <= args.min_score or args.min_score == 0.0]

    if not results:
        print("No normalized results to display.")
        return

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_normalized(results))


if __name__ == "__main__":
    main()
