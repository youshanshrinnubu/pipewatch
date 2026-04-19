"""Inspector: deep-dive diagnostics for a single pipeline's metric history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric
from pipewatch.metrics import failure_rate


@dataclass
class InspectionReport:
    pipeline: str
    total_runs: int
    avg_failure_rate: float
    max_failure_rate: float
    min_failure_rate: float
    error_count: int
    ok_count: int
    last_status: Optional[str]
    verdict: str  # "healthy" | "degraded" | "critical"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_runs": self.total_runs,
            "avg_failure_rate": round(self.avg_failure_rate, 4),
            "max_failure_rate": round(self.max_failure_rate, 4),
            "min_failure_rate": round(self.min_failure_rate, 4),
            "error_count": self.error_count,
            "ok_count": self.ok_count,
            "last_status": self.last_status,
            "verdict": self.verdict,
        }


def _verdict(avg_fr: float, error_count: int, total: int) -> str:
    if error_count > 0 or avg_fr >= 0.5:
        return "critical"
    if avg_fr >= 0.2:
        return "degraded"
    return "healthy"


def inspect_pipeline(pipeline: str, metrics: List[PipelineMetric]) -> Optional[InspectionReport]:
    """Return an InspectionReport for *pipeline* using the provided metric history."""
    relevant = [m for m in metrics if m.pipeline_name == pipeline]
    if not relevant:
        return None

    rates = [failure_rate(m) for m in relevant]
    avg_fr = sum(rates) / len(rates)
    error_count = sum(1 for m in relevant if m.status == "error")
    ok_count = sum(1 for m in relevant if m.status == "ok")
    last_status = relevant[-1].status

    return InspectionReport(
        pipeline=pipeline,
        total_runs=len(relevant),
        avg_failure_rate=avg_fr,
        max_failure_rate=max(rates),
        min_failure_rate=min(rates),
        error_count=error_count,
        ok_count=ok_count,
        last_status=last_status,
        verdict=_verdict(avg_fr, error_count, len(relevant)),
    )


def inspect_all(metrics: List[PipelineMetric]) -> List[InspectionReport]:
    """Return InspectionReports for every distinct pipeline found in *metrics*."""
    pipelines = list(dict.fromkeys(m.pipeline_name for m in metrics))
    reports = [inspect_pipeline(p, metrics) for p in pipelines]
    return [r for r in reports if r is not None]


def format_inspection(report: InspectionReport) -> str:
    lines = [
        f"Pipeline : {report.pipeline}",
        f"Verdict  : {report.verdict.upper()}",
        f"Runs     : {report.total_runs}  (ok={report.ok_count}, error={report.error_count})",
        f"Failure  : avg={report.avg_failure_rate:.2%}  min={report.min_failure_rate:.2%}  max={report.max_failure_rate:.2%}",
        f"Last     : {report.last_status}",
    ]
    return "\n".join(lines)
