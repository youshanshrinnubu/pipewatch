"""Pipeline health scorer — assigns a numeric health score to a pipeline based on its metrics."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, is_healthy


@dataclass
class ScoredMetric:
    pipeline: str
    score: float  # 0.0 (worst) to 100.0 (best)
    grade: str    # A, B, C, D, F
    reason: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": self.score,
            "grade": self.grade,
            "reason": self.reason,
        }


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def score_metric(metric: PipelineMetric) -> ScoredMetric:
    """Compute a health score for a single metric snapshot."""
    if metric.status == "error":
        return ScoredMetric(metric.pipeline, 0.0, "F", "status=error")

    fr = metric.failure_rate if metric.total_records > 0 else 0.0
    base = 100.0 - (fr * 100.0)

    # Penalise for non-ok status even if failure_rate is low
    if metric.status != "ok":
        base = max(0.0, base - 20.0)
        reason = f"status={metric.status}, failure_rate={fr:.2%}"
    else:
        reason = f"failure_rate={fr:.2%}"

    score = round(max(0.0, min(100.0, base)), 2)
    return ScoredMetric(metric.pipeline, score, _grade(score), reason)


def score_all(metrics: List[PipelineMetric]) -> List[ScoredMetric]:
    """Score every metric in the list."""
    return [score_metric(m) for m in metrics]


def best(scored: List[ScoredMetric]) -> Optional[ScoredMetric]:
    return max(scored, key=lambda s: s.score) if scored else None


def worst(scored: List[ScoredMetric]) -> Optional[ScoredMetric]:
    return min(scored, key=lambda s: s.score) if scored else None
