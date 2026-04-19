"""Pipeline health scoring and status rollup across multiple metrics."""
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from pipewatch.metrics import PipelineMetric, failure_rate, is_healthy


@dataclass
class HealthReport:
    pipeline: str
    status: str
    score: float  # 0.0 - 1.0
    total_metrics: int
    healthy_count: int
    warning_count: int
    critical_count: int
    avg_failure_rate: float
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "score": round(self.score, 4),
            "total_metrics": self.total_metrics,
            "healthy_count": self.healthy_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "avg_failure_rate": round(self.avg_failure_rate, 4),
            "notes": self.notes,
        }


def _classify(metric: PipelineMetric) -> str:
    fr = failure_rate(metric)
    if metric.status == "error" or fr >= 0.5:
        return "critical"
    if fr >= 0.2:
        return "warning"
    return "healthy"


def evaluate_pipeline(pipeline: str, metrics: List[PipelineMetric]) -> Optional[HealthReport]:
    relevant = [m for m in metrics if m.pipeline == pipeline]
    if not relevant:
        return None

    counts = {"healthy": 0, "warning": 0, "critical": 0}
    for m in relevant:
        counts[_classify(m)] += 1

    total = len(relevant)
    avg_fr = sum(failure_rate(m) for m in relevant) / total

    score = counts["healthy"] / total - (counts["critical"] / total) * 0.5
    score = max(0.0, min(1.0, score))

    if counts["critical"] > 0:
        status = "critical"
    elif counts["warning"] > 0:
        status = "warning"
    else:
        status = "healthy"

    notes = []
    if avg_fr > 0.3:
        notes.append(f"High average failure rate: {avg_fr:.1%}")
    if counts["critical"] > 0:
        notes.append(f"{counts['critical']} critical metric(s) detected")

    return HealthReport(
        pipeline=pipeline,
        status=status,
        score=score,
        total_metrics=total,
        healthy_count=counts["healthy"],
        warning_count=counts["warning"],
        critical_count=counts["critical"],
        avg_failure_rate=avg_fr,
        notes=notes,
    )


def evaluate_all(metrics: List[PipelineMetric]) -> List[HealthReport]:
    pipelines = sorted({m.pipeline for m in metrics})
    return [r for p in pipelines if (r := evaluate_pipeline(p, metrics)) is not None]


def format_health_report(report: HealthReport) -> str:
    lines = [
        f"Pipeline : {report.pipeline}",
        f"Status   : {report.status.upper()}",
        f"Score    : {report.score:.2f}",
        f"Metrics  : {report.total_metrics} total | {report.healthy_count} healthy | {report.warning_count} warning | {report.critical_count} critical",
        f"Avg FR   : {report.avg_failure_rate:.1%}",
    ]
    for note in report.notes:
        lines.append(f"  ! {note}")
    return "\n".join(lines)
