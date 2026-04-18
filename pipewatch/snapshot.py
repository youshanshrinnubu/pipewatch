"""Snapshot persistence for pipeline metrics — save/load metric history to disk."""

import json
import os
from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetric

DEFAULT_SNAPSHOT_PATH = ".pipewatch_snapshots.json"


def _metric_to_dict(metric: PipelineMetric) -> dict:
    return {
        "pipeline_name": metric.pipeline_name,
        "status": metric.status,
        "records_processed": metric.records_processed,
        "records_failed": metric.records_failed,
        "duration_seconds": metric.duration_seconds,
        "timestamp": metric.timestamp.isoformat() if metric.timestamp else None,
    }


def _dict_to_metric(data: dict) -> PipelineMetric:
    ts = data.get("timestamp")
    return PipelineMetric(
        pipeline_name=data["pipeline_name"],
        status=data["status"],
        records_processed=data["records_processed"],
        records_failed=data["records_failed"],
        duration_seconds=data.get("duration_seconds", 0.0),
        timestamp=datetime.fromisoformat(ts) if ts else datetime.utcnow(),
    )


class SnapshotStore:
    def __init__(self, path: str = DEFAULT_SNAPSHOT_PATH, max_entries: int = 500):
        self.path = path
        self.max_entries = max_entries

    def save(self, metrics: List[PipelineMetric]) -> None:
        existing = self.load()
        combined = existing + metrics
        if len(combined) > self.max_entries:
            combined = combined[-self.max_entries:]
        with open(self.path, "w") as f:
            json.dump([_metric_to_dict(m) for m in combined], f, indent=2)

    def load(self) -> List[PipelineMetric]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return []
        return [_dict_to_metric(d) for d in data]

    def load_for_pipeline(self, pipeline_name: str) -> List[PipelineMetric]:
        return [m for m in self.load() if m.pipeline_name == pipeline_name]

    def clear(self) -> None:
        if os.path.exists(self.path):
            os.remove(self.path)
