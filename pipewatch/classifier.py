"""Classify pipeline metrics into named health tiers."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class ClassifiedMetric:
    metric: PipelineMetric
    tier: str  # "healthy", "degraded", "critical", "unknown"
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline_name,
            "tier": self.tier,
            "reasons": self.reasons,
        }


def classify(metric: PipelineMetric) -> ClassifiedMetric:
    reasons: List[str] = []

    if metric.status == "error":
        reasons.append("status is error")
        return ClassifiedMetric(metric=metric, tier="critical", reasons=reasons)

    if metric.total_records == 0:
        reasons.append("no records processed")
        return ClassifiedMetric(metric=metric, tier="unknown", reasons=reasons)

    rate = metric.failed_records / metric.total_records

    if rate >= 0.5:
        reasons.append(f"failure rate {rate:.1%} >= 50%")
        return ClassifiedMetric(metric=metric, tier="critical", reasons=reasons)

    if rate >= 0.1:
        reasons.append(f"failure rate {rate:.1%} >= 10%")
        return ClassifiedMetric(metric=metric, tier="degraded", reasons=reasons)

    if metric.status != "ok":
        reasons.append(f"non-ok status: {metric.status}")
        return ClassifiedMetric(metric=metric, tier="degraded", reasons=reasons)

    return ClassifiedMetric(metric=metric, tier="healthy", reasons=reasons)


def classify_all(metrics: List[PipelineMetric]) -> List[ClassifiedMetric]:
    return [classify(m) for m in metrics]


def format_classified(results: List[ClassifiedMetric]) -> str:
    if not results:
        return "No metrics to classify."
    lines = []
    for r in results:
        reason_str = "; ".join(r.reasons) if r.reasons else "all clear"
        lines.append(f"[{r.tier.upper():8s}] {r.metric.pipeline_name} — {reason_str}")
    return "\n".join(lines)
