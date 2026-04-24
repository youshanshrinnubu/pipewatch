"""rechecker.py — Re-evaluate pipelines that previously failed and detect recovery."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict

from pipewatch.metrics import PipelineMetric, failure_rate, is_healthy


@dataclass
class RecoveryResult:
    pipeline: str
    previous_status: str
    current_status: str
    previous_failure_rate: float
    current_failure_rate: float
    recovered: bool
    still_failing: bool
    note: str = ""

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "previous_status": self.previous_status,
            "current_status": self.current_status,
            "previous_failure_rate": round(self.previous_failure_rate, 4),
            "current_failure_rate": round(self.current_failure_rate, 4),
            "recovered": self.recovered,
            "still_failing": self.still_failing,
            "note": self.note,
        }


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records == 0:
        return 0.0
    return m.failed_records / m.total_records


def recheck_pipeline(
    pipeline: str,
    before: List[PipelineMetric],
    after: List[PipelineMetric],
    failure_threshold: float = 0.1,
) -> Optional[RecoveryResult]:
    """Compare the most recent before/after metric for a pipeline."""
    prev_metrics = [m for m in before if m.pipeline == pipeline]
    curr_metrics = [m for m in after if m.pipeline == pipeline]
    if not prev_metrics or not curr_metrics:
        return None

    prev = max(prev_metrics, key=lambda m: m.timestamp)
    curr = max(curr_metrics, key=lambda m: m.timestamp)

    prev_fr = _failure_rate(prev)
    curr_fr = _failure_rate(curr)
    prev_healthy = is_healthy(prev, failure_threshold)
    curr_healthy = is_healthy(curr, failure_threshold)

    recovered = (not prev_healthy) and curr_healthy
    still_failing = (not prev_healthy) and (not curr_healthy)

    if prev_healthy and curr_healthy:
        note = "stable"
    elif recovered:
        note = "pipeline recovered"
    elif still_failing:
        note = "pipeline still failing"
    else:
        note = "pipeline newly degraded"

    return RecoveryResult(
        pipeline=pipeline,
        previous_status=prev.status,
        current_status=curr.status,
        previous_failure_rate=prev_fr,
        current_failure_rate=curr_fr,
        recovered=recovered,
        still_failing=still_failing,
        note=note,
    )


def recheck_all(
    before: List[PipelineMetric],
    after: List[PipelineMetric],
    failure_threshold: float = 0.1,
) -> List[RecoveryResult]:
    """Recheck all pipelines that appear in the before snapshot."""
    pipelines = {m.pipeline for m in before}
    results = []
    for pipeline in sorted(pipelines):
        result = recheck_pipeline(pipeline, before, after, failure_threshold)
        if result is not None:
            results.append(result)
    return results
