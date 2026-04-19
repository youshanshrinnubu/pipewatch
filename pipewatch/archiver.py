"""Archive old snapshot data to compressed JSON files."""

from __future__ import annotations

import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from pipewatch.snapshot import SnapshotStore


@dataclass
class ArchiveResult:
    path: str
    metrics_archived: int
    metrics_remaining: int

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "metrics_archived": self.metrics_archived,
            "metrics_remaining": self.metrics_remaining,
        }


def _archive_path(directory: str, label: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"archive_{label}_{ts}.json.gz"
    return os.path.join(directory, filename)


def archive_store(
    store: SnapshotStore,
    pipeline: str,
    max_keep: int,
    archive_dir: str,
) -> ArchiveResult | None:
    """Archive metrics beyond max_keep for a pipeline to a gzipped JSON file."""
    metrics = store.load(pipeline)
    if len(metrics) <= max_keep:
        return None

    to_archive = metrics[:-max_keep]
    to_keep = metrics[-max_keep:]

    os.makedirs(archive_dir, exist_ok=True)
    path = _archive_path(archive_dir, pipeline)

    records = []
    for m in to_archive:
        d = m.__dict__.copy() if hasattr(m, "__dict__") else {}
        records.append(d)

    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(records, f, default=str)

    # Rewrite store with only kept metrics
    store._data[pipeline] = to_keep  # type: ignore[attr-defined]
    store.save(pipeline, to_keep[0])  # trigger persistence via first save
    # Overwrite properly
    store._data[pipeline] = to_keep  # type: ignore[attr-defined]

    return ArchiveResult(
        path=path,
        metrics_archived=len(to_archive),
        metrics_remaining=len(to_keep),
    )


def archive_all(
    store: SnapshotStore,
    max_keep: int,
    archive_dir: str,
) -> List[ArchiveResult]:
    results = []
    for pipeline in list(store._data.keys()):  # type: ignore[attr-defined]
        result = archive_store(store, pipeline, max_keep, archive_dir)
        if result is not None:
            results.append(result)
    return results
