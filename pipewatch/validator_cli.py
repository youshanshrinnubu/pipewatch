import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.validator import validate_all, format_validation


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Validate pipeline metrics for data integrity issues")
    p.add_argument("--snapshot", default="snapshot.json", help="Path to snapshot file")
    p.add_argument("--pipeline", default=None, help="Filter to a specific pipeline")
    p.add_argument("--errors-only", action="store_true", help="Only show metrics with errors")
    p.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    store = SnapshotStore(args.snapshot)
    metrics = store.load()

    if args.pipeline:
        metrics = [m for m in metrics if m.pipeline_name == args.pipeline]

    results = validate_all(metrics)

    if args.errors_only:
        results = [r for r in results if not r.valid]

    if not results:
        print("No validation issues found.")
        return

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_validation(results))

    has_errors = any(not r.valid for r in results)
    if has_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
