"""CLI entry-point for the pipeline health scorer."""
import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.scorer import score_all, best, worst


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Score pipeline health from snapshot store")
    p.add_argument("--store", default="pipewatch_snapshots.json", help="Snapshot file path")
    p.add_argument("--pipeline", help="Filter to a single pipeline")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    p.add_argument("--worst", dest="show_worst", action="store_true", help="Show only worst pipeline")
    p.add_argument("--best", dest="show_best", action="store_true", help="Show only best pipeline")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    store = SnapshotStore(args.store)
    metrics = store.load_all()

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline == args.pipeline]

    if not metrics:
        print("No metrics found.")
        return

    # Use only the latest snapshot per pipeline
    latest: dict = {}
    for m in metrics:
        if m.pipeline not in latest or m.timestamp > latest[m.pipeline].timestamp:
            latest[m.pipeline] = m

    scored = score_all(list(latest.values()))

    if args.show_worst:
        scored = [w] if (w := worst(scored)) else []
    elif args.show_best:
        scored = [b] if (b := best(scored)) else []

    if args.as_json:
        print(json.dumps([s.to_dict() for s in scored], indent=2))
    else:
        for s in scored:
            print(f"{s.pipeline:<30} score={s.score:6.2f}  grade={s.grade}  ({s.reason})")


if __name__ == "__main__":
    main()
