"""CLI for archiving old pipeline snapshot data."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone

from pipewatch.archiver import archive_all
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-archiver",
        description="Archive old snapshot metrics to compressed files.",
    )
    p.add_argument("--max-keep", type=int, default=10, help="Metrics to keep per pipeline")
    p.add_argument("--archive-dir", default="/tmp/pipewatch_archives", help="Directory for archives")
    p.add_argument("--json", action="store_true", help="Output JSON")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(path=tempfile.mktemp(suffix=".json"))

    now = datetime.now(timezone.utc).timestamp()
    for pipeline, count in [("sales", 15), ("inventory", 8)]:
        for i in range(count):
            m = PipelineMetric(
                pipeline=pipeline,
                status="ok",
                records_processed=100 + i,
                records_failed=i,
                duration_seconds=1.0,
                timestamp=now - (count - i) * 60,
            )
            store.save(pipeline, m)

    results = archive_all(store, max_keep=args.max_keep, archive_dir=args.archive_dir)

    if not results:
        print("Nothing to archive.")
        return 0

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            print(
                f"[archived] {r.path} | archived={r.metrics_archived} kept={r.metrics_remaining}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
