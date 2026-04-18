"""Pipeline metrics collection and storage."""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineMetric:
    """Represents a single pipeline health metric snapshot."""

    pipeline_name: str
    status: str  # 'ok', 'warning', 'error'
    records_processed: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    @property
    def failure_rate(self) -> float:
        """Return the ratio of failed to total records."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return self.records_failed / total

    def is_healthy(self, max_failure_rate: float = 0.05) -> bool:
        """Return True if the pipeline is within acceptable thresholds."""
        return self.status != "error" and self.failure_rate <= max_failure_rate

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "duration_seconds": self.duration_seconds,
            "failure_rate": round(self.failure_rate, 4),
            "error_message": self.error_message,
            "timestamp": self.timestamp,
        }


class MetricsCollector:
    """Collects and stores pipeline metrics in memory."""

    def __init__(self, max_history: int = 100):
        self._history: list[PipelineMetric] = []
        self.max_history = max_history

    def record(self, metric: PipelineMetric) -> None:
        """Add a metric snapshot, evicting oldest if over capacity."""
        self._history.append(metric)
        if len(self._history) > self.max_history:
            self._history.pop(0)

    def latest(self, pipeline_name: str) -> Optional[PipelineMetric]:
        """Return the most recent metric for a given pipeline."""
        for metric in reversed(self._history):
            if metric.pipeline_name == pipeline_name:
                return metric
        return None

    def all_pipelines(self) -> list[str]:
        """Return unique pipeline names seen so far."""
        seen = []
        for m in self._history:
            if m.pipeline_name not in seen:
                seen.append(m.pipeline_name)
        return seen

    def history(self, pipeline_name: str) -> list[PipelineMetric]:
        """Return all recorded metrics for a pipeline."""
        return [m for m in self._history if m.pipeline_name == pipeline_name]
