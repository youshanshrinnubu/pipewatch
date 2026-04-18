"""CLI for baseline management: set and check baselines."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.baseline import BaselineStore
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Manage pipewatch baselines")
    p.add_argument("--snapshot", default=".pipewatch/snapshots.json", help="Snapshot file")
    p.add_argument("--baseline", default=".pipewatch/baseline.json", help="Baseline file")
    sub = p.add_subparsers(dest="command")

    sub.add_parser("set", help="Save current latest snapshot as baseline")

    chk = sub.add_parser("check", help="Check current snapshots against baseline")
    chk.add_argument("--json", dest="as_json", action="store_true")
    chk.add_argument("--regressions-only", action="store_true")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    snap_store = SnapshotStore(args.snapshot)
    base_store = BaselineStore(args.baseline)

    if args.command == "set":
        all_metrics = snap_store.load_all()
        if not all_metrics:
            print("No snapshots found.", file=sys.stderr)
            return 1
        latest: dict = {}
        for m in all_metrics:
            latest[m.pipeline_name] = m
        base_store.save(list(latest.values()))
        print(f"Baseline saved with {len(latest)} pipeline(s).")
        return 0

    # check
    all_metrics = snap_store.load_all()
    latest: dict = {}
    for m in all_metrics:
        latest[m.pipeline_name] = m

    violations = base_store.check(list(latest.values()))
    if args.regressions_only:
        violations = [v for v in violations if v.is_regression]

    if not violations:
        print("All pipelines within baseline.")
        return 0

    if args.as_json:
        print(json.dumps([v.to_dict() for v in violations], indent=2))
    else:
        for v in violations:
            flag = "[REGRESSION]" if v.is_regression else "[ok]"
            print(f"{flag} {v.pipeline}: baseline={v.baseline_failure_rate:.2%} current={v.current_failure_rate:.2%} status={v.current_status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
