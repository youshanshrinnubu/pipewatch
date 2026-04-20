"""Evaluator: score and classify pipeline metrics against configurable thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, failure_rate


@dataclass
class EvaluationResult:
    pipeline: str
    score: float          # 0.0 – 1.0
    tier: str             # "healthy" | "warning" | "critical"
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 4),
            "tier": self.tier,
            "reasons": self.reasons,
        }


@dataclass
class EvaluatorConfig:
    warning_failure_rate: float = 0.05
    critical_failure_rate: float = 0.20
    warning_score_floor: float = 0.60
    critical_score_floor: float = 0.30


def _tier(score: float, cfg: EvaluatorConfig) -> str:
    if score >= cfg.warning_score_floor:
        return "healthy"
    if score >= cfg.critical_score_floor:
        return "warning"
    return "critical"


def evaluate(metric: PipelineMetric, cfg: Optional[EvaluatorConfig] = None) -> EvaluationResult:
    """Evaluate a single metric and return an EvaluationResult."""
    if cfg is None:
        cfg = EvaluatorConfig()

    reasons: List[str] = []
    score = 1.0

    if metric.status == "error":
        score = 0.0
        reasons.append("status is error")
    else:
        fr = failure_rate(metric)
        if fr >= cfg.critical_failure_rate:
            penalty = 0.70
            score -= penalty
            reasons.append(f"failure rate {fr:.1%} >= critical threshold {cfg.critical_failure_rate:.1%}")
        elif fr >= cfg.warning_failure_rate:
            penalty = 0.35
            score -= penalty
            reasons.append(f"failure rate {fr:.1%} >= warning threshold {cfg.warning_failure_rate:.1%}")

        if metric.status == "warning":
            score -= 0.10
            reasons.append("status is warning")

    score = max(0.0, min(1.0, score))
    tier = _tier(score, cfg)
    return EvaluationResult(pipeline=metric.pipeline, score=score, tier=tier, reasons=reasons)


def evaluate_all(
    metrics: List[PipelineMetric],
    cfg: Optional[EvaluatorConfig] = None,
) -> List[EvaluationResult]:
    """Evaluate a list of metrics, one result per metric."""
    return [evaluate(m, cfg) for m in metrics]


def format_evaluation(results: List[EvaluationResult]) -> str:
    if not results:
        return "No evaluation results."
    lines = []
    for r in results:
        tag = f"[{r.tier.upper()}]"
        lines.append(f"{tag:<12} {r.pipeline}  score={r.score:.2f}")
        for reason in r.reasons:
            lines.append(f"             - {reason}")
    return "\n".join(lines)
