from __future__ import annotations
import json
from pathlib import Path
from typing import List
from pipewatch.anomaly import AnomalyResult
from pipewatch.metrics import PipelineMetric


class AnomalyStore:
    def __init__(self, path: str = ".pipewatch_anomalies.json"):
        self._path = Path(path)

    def save(self, results: List[AnomalyResult]) -> None:
        existing = self._load_raw()
        existing.extend([r.to_dict() for r in results])
        self._path.write_text(json.dumps(existing, indent=2))

    def load(self) -> List[dict]:
        return self._load_raw()

    def _load_raw(self) -> list:
        if not self._path.exists():
            return []
        try:
            return json.loads(self._path.read_text())
        except (json.JSONDecodeError, OSError):
            return []

    def clear(self) -> None:
        if self._path.exists():
            self._path.unlink()

    def filter_by_severity(self, severity: str) -> List[dict]:
        return [r for r in self.load() if r.get("severity") == severity]

    def filter_by_pipeline(self, pipeline: str) -> List[dict]:
        return [r for r in self.load() if r.get("pipeline") == pipeline]

    def summary(self) -> dict:
        records = self.load()
        total = len(records)
        warnings = sum(1 for r in records if r.get("severity") == "warning")
        criticals = sum(1 for r in records if r.get("severity") == "critical")
        return {"total": total, "warning": warnings, "critical": criticals}
