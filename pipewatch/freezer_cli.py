"""freezer_cli.py — CLI interface for freeze/thaw pipeline snapshot labels."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.freezer import FreezeStore, format_freeze_record
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-freezer",
        description="Freeze and thaw named pipeline metric snapshots.",
    )
    p.add_argument("--store", default="snapshot_store.json", help="Snapshot store path")
    p.add_argument("--freeze-store", default="freeze_store.json", help="Freeze store path")
    sub = p.add_subparsers(dest="command")

    freeze_p = sub.add_parser("freeze", help="Freeze current metrics under a label")
    freeze_p.add_argument("label", help="Label for this freeze point")
    freeze_p.add_argument("--json", dest="as_json", action="store_true")

    thaw_p = sub.add_parser("thaw", help="Retrieve a frozen snapshot by label")
    thaw_p.add_argument("label", help="Label to thaw")
    thaw_p.add_argument("--json", dest="as_json", action="store_true")

    list_p = sub.add_parser("list", help="List all freeze labels")
    list_p.add_argument("--json", dest="as_json", action="store_true")

    delete_p = sub.add_parser("delete", help="Delete a freeze label")
    delete_p.add_argument("label", help="Label to delete")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    snap = SnapshotStore(args.store)
    fstore = FreezeStore(args.freeze_store)

    if args.command == "freeze":
        all_metrics = snap.load_all()
        record = fstore.freeze(args.label, all_metrics)
        if getattr(args, "as_json", False):
            print(json.dumps(record.to_dict(), indent=2))
        else:
            print(format_freeze_record(record))
        return 0

    if args.command == "thaw":
        record = fstore.thaw(args.label)
        if record is None:
            print(f"No freeze found for label: {args.label}", file=sys.stderr)
            return 1
        if getattr(args, "as_json", False):
            print(json.dumps(record.to_dict(), indent=2))
        else:
            print(format_freeze_record(record))
        return 0

    if args.command == "list":
        labels = fstore.list_labels()
        if getattr(args, "as_json", False):
            print(json.dumps(labels))
        else:
            if not labels:
                print("No freeze points recorded.")
            else:
                for lbl in labels:
                    print(f"  - {lbl}")
        return 0

    if args.command == "delete":
        removed = fstore.delete(args.label)
        if removed:
            print(f"Deleted freeze: {args.label}")
        else:
            print(f"Label not found: {args.label}", file=sys.stderr)
            return 1
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
