"""Digester: produce a compact health digest across all pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipewatch.metrics import PipelineMetric, failure_rate, is_healthy


@dataclass
class DigestEntry:
    pipeline: str
    total_metrics: int
    healthy_count: int
    warning_count: int
    critical_count: int
    avg_failure_rate: float
    overall_status: str

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "total_metrics": self.total_metrics,
            "healthy_count": self.healthy_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "avg_failure_rate": round(self.avg_failure_rate, 4),
            "overall_status": self.overall_status,
        }


def _classify(metric: PipelineMetric) -> str:
    fr = failure_rate(metric)
    if metric.status == "error" or fr >= 0.5:
        return "critical"
    if fr >= 0.2:
        return "warning"
    return "healthy"


def digest_pipeline(pipeline: str, metrics: List[PipelineMetric]) -> DigestEntry:
    if not metrics:
        return DigestEntry(pipeline, 0, 0, 0, 0, 0.0, "unknown")

    counts: Dict[str, int] = {"healthy": 0, "warning": 0, "critical": 0}
    total_fr = 0.0
    for m in metrics:
        label = _classify(m)
        counts[label] += 1
        total_fr += failure_rate(m)

    avg_fr = total_fr / len(metrics)
    if counts["critical"] > 0:
        overall = "critical"
    elif counts["warning"] > 0:
        overall = "warning"
    else:
        overall = "healthy"

    return DigestEntry(
        pipeline=pipeline,
        total_metrics=len(metrics),
        healthy_count=counts["healthy"],
        warning_count=counts["warning"],
        critical_count=counts["critical"],
        avg_failure_rate=avg_fr,
        overall_status=overall,
    )


def digest_all(metrics: List[PipelineMetric]) -> List[DigestEntry]:
    grouped: Dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        grouped.setdefault(m.pipeline, []).append(m)
    return [digest_pipeline(p, ms) for p, ms in sorted(grouped.items())]


def format_digest(entries: List[DigestEntry]) -> str:
    if not entries:
        return "No pipelines to digest."
    lines = ["Pipeline Digest", "=" * 40]
    for e in entries:
        lines.append(
            f"{e.pipeline}: {e.overall_status.upper()} "
            f"(h={e.healthy_count} w={e.warning_count} c={e.critical_count} "
            f"avg_fr={e.avg_failure_rate:.2%})"
        )
    return "\n".join(lines)
