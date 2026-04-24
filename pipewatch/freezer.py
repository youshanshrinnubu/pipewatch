"""freezer.py — Snapshot freeze/unfreeze utilities for pipeline metrics.

Allows capturing a named 'frozen' snapshot of current metrics for later
comparison or rollback reference points.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import _metric_to_dict, _dict_to_metric


@dataclass
class FreezeRecord:
    label: str
    frozen_at: str
    metrics: List[PipelineMetric]

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "frozen_at": self.frozen_at,
            "metrics": [_metric_to_dict(m) for m in self.metrics],
        }

    @staticmethod
    def from_dict(data: dict) -> "FreezeRecord":
        return FreezeRecord(
            label=data["label"],
            frozen_at=data["frozen_at"],
            metrics=[_dict_to_metric(m) for m in data.get("metrics", [])],
        )


class FreezeStore:
    def __init__(self, path: str = "freeze_store.json") -> None:
        self._path = path
        self._records: Dict[str, FreezeRecord] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "r") as f:
            raw = json.load(f)
        for entry in raw:
            rec = FreezeRecord.from_dict(entry)
            self._records[rec.label] = rec

    def _save(self) -> None:
        with open(self._path, "w") as f:
            json.dump([r.to_dict() for r in self._records.values()], f, indent=2)

    def freeze(self, label: str, metrics: List[PipelineMetric]) -> FreezeRecord:
        now = datetime.now(timezone.utc).isoformat()
        record = FreezeRecord(label=label, frozen_at=now, metrics=list(metrics))
        self._records[label] = record
        self._save()
        return record

    def thaw(self, label: str) -> Optional[FreezeRecord]:
        return self._records.get(label)

    def list_labels(self) -> List[str]:
        return list(self._records.keys())

    def delete(self, label: str) -> bool:
        if label in self._records:
            del self._records[label]
            self._save()
            return True
        return False


def format_freeze_record(record: FreezeRecord) -> str:
    lines = [
        f"Freeze: {record.label}",
        f"  Frozen at : {record.frozen_at}",
        f"  Pipelines : {len(record.metrics)}",
    ]
    for m in record.metrics:
        lines.append(f"    - {m.pipeline_name} | status={m.status} | records={m.total_records}")
    return "\n".join(lines)
