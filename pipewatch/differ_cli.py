"""CLI for diffing two snapshot files."""
import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.differ import diff_all


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-diff",
        description="Diff two pipewatch snapshot files",
    )
    p.add_argument("prev", help="Path to previous snapshot file")
    p.add_argument("curr", help="Path to current snapshot file")
    p.add_argument("--pipeline", help="Filter by pipeline name")
    p.add_argument("--degraded-only", action="store_true",
                   help="Only show degraded pipelines")
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="Output as JSON")
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    prev_store = SnapshotStore(args.prev)
    curr_store = SnapshotStore(args.curr)

    prev_metrics = prev_store.load_latest()
    curr_metrics = curr_store.load_latest()

    diffs = diff_all(prev_metrics, curr_metrics)

    if args.pipeline:
        diffs = [d for d in diffs if d.pipeline == args.pipeline]
    if args.degraded_only:
        diffs = [d for d in diffs if d.is_degraded()]

    if not diffs:
        print("No differences found.")
        return 0

    if args.as_json:
        print(json.dumps([d.to_dict() for d in diffs], indent=2))
    else:
        for d in diffs:
            flag = " [DEGRADED]" if d.is_degraded() else ""
            print(f"{d.pipeline}{flag}")
            print(f"  status : {d.prev_status} -> {d.curr_status}")
            print(f"  fail % : {d.prev_failure_rate:.2%} -> {d.curr_failure_rate:.2%} "
                  f"(delta {d.failure_rate_delta:+.2%})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
