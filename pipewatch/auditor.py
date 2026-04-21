"""Audit log for pipeline metric events and alert firings."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


@dataclass
class AuditEntry:
    event_type: str          # "metric" | "alert"
    pipeline: str
    timestamp: float
    detail: dict
    id: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "event_type": self.event_type,
            "pipeline": self.pipeline,
            "timestamp": self.timestamp,
            "detail": self.detail,
        }


def _next_id(entries: List[AuditEntry]) -> int:
    return max((e.id for e in entries), default=0) + 1


class AuditLog:
    """In-memory append-only audit log with optional file persistence."""

    def __init__(self, path: Optional[str] = None) -> None:
        self._path = path
        self._entries: List[AuditEntry] = []
        if path:
            self._load()

    def record_metric(self, metric: PipelineMetric) -> AuditEntry:
        entry = AuditEntry(
            event_type="metric",
            pipeline=metric.pipeline_name,
            timestamp=metric.timestamp,
            detail={
                "status": metric.status,
                "total_records": metric.total_records,
                "failed_records": metric.failed_records,
            },
            id=_next_id(self._entries),
        )
        self._append(entry)
        return entry

    def record_alert(self, alert: Alert) -> AuditEntry:
        entry = AuditEntry(
            event_type="alert",
            pipeline=alert.pipeline_name,
            timestamp=time.time(),
            detail={"severity": alert.severity, "message": alert.message},
            id=_next_id(self._entries),
        )
        self._append(entry)
        return entry

    def entries(self, pipeline: Optional[str] = None, event_type: Optional[str] = None) -> List[AuditEntry]:
        result = self._entries
        if pipeline:
            result = [e for e in result if e.pipeline == pipeline]
        if event_type:
            result = [e for e in result if e.event_type == event_type]
        return list(result)

    def _append(self, entry: AuditEntry) -> None:
        self._entries.append(entry)
        if self._path:
            with open(self._path, "a") as fh:
                fh.write(json.dumps(entry.to_dict()) + "\n")

    def _load(self) -> None:
        try:
            with open(self._path) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)
                    self._entries.append(
                        AuditEntry(
                            id=d["id"],
                            event_type=d["event_type"],
                            pipeline=d["pipeline"],
                            timestamp=d["timestamp"],
                            detail=d["detail"],
                        )
                    )
        except FileNotFoundError:
            pass
