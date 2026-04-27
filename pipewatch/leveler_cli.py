"""CLI entry point for the leveler module."""
from __future__ import annotations
import argparse
import json
import sys
from pipewatch.leveler import LevelConfig, level_all, format_leveled
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-leveler",
        description="Assign severity levels to pipeline metrics.",
    )
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument("--warning-rate", type=float, default=0.05, metavar="RATE",
                   help="Failure rate threshold for warning (default: 0.05)")
    p.add_argument("--critical-rate", type=float, default=0.20, metavar="RATE",
                   help="Failure rate threshold for critical (default: 0.20)")
    p.add_argument("--level", choices=["ok", "warning", "critical"],
                   help="Filter output to a specific level")
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="Output as JSON")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if not metrics:
        print("No metrics found.")
        return 0

    cfg = LevelConfig(
        warning_failure_rate=args.warning_rate,
        critical_failure_rate=args.critical_rate,
    )
    results = level_all(metrics, cfg)

    if args.level:
        results = [r for r in results if r.level == args.level]

    if not results:
        print(f"No metrics at level '{args.level}'." if args.level else "No results.")
        return 0

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_leveled(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
