"""cycler.py — Round-robin cycling of pipeline metric snapshots for rotation-based monitoring."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CycleState:
    pipelines: List[str] = field(default_factory=list)
    index: int = 0

    def advance(self) -> None:
        if self.pipelines:
            self.index = (self.index + 1) % len(self.pipelines)

    def current(self) -> Optional[str]:
        if not self.pipelines:
            return None
        return self.pipelines[self.index]


@dataclass
class CycleResult:
    pipeline: str
    metric: PipelineMetric
    position: int
    total: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "position": self.position,
            "total": self.total,
            "status": self.metric.status,
            "total_records": self.metric.total_records,
            "failed_records": self.metric.failed_records,
        }


class Cycler:
    """Cycles through pipelines in round-robin order, yielding the latest metric for each."""

    def __init__(self) -> None:
        self._state: CycleState = CycleState()
        self._metrics: Dict[str, List[PipelineMetric]] = {}

    def load(self, metrics: List[PipelineMetric]) -> None:
        """Load metrics and rebuild pipeline order."""
        self._metrics = {}
        for m in metrics:
            self._metrics.setdefault(m.pipeline_name, []).append(m)
        self._state.pipelines = sorted(self._metrics.keys())
        self._state.index = 0

    def current(self) -> Optional[CycleResult]:
        name = self._state.current()
        if name is None:
            return None
        bucket = self._metrics.get(name, [])
        if not bucket:
            return None
        latest = max(bucket, key=lambda m: m.timestamp)
        return CycleResult(
            pipeline=name,
            metric=latest,
            position=self._state.index + 1,
            total=len(self._state.pipelines),
        )

    def next(self) -> Optional[CycleResult]:
        self._state.advance()
        return self.current()

    def peek_all(self) -> List[CycleResult]:
        results = []
        for i, name in enumerate(self._state.pipelines):
            bucket = self._metrics.get(name, [])
            if bucket:
                latest = max(bucket, key=lambda m: m.timestamp)
                results.append(CycleResult(pipeline=name, metric=latest, position=i + 1, total=len(self._state.pipelines)))
        return results
