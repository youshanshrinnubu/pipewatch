"""Summarizer: produce human-readable digest summaries of pipeline metric collections."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, failure_rate


@dataclass
class SummaryLine:
    pipeline: str
    status: str
    total_records: int
    failed_records: int
    failure_rate: float
    note: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 4),
            "note": self.note,
        }


@dataclass
class SummarizerResult:
    lines: List[SummaryLine] = field(default_factory=list)
    total_pipelines: int = 0
    healthy_count: int = 0
    warning_count: int = 0
    critical_count: int = 0

    def to_dict(self) -> dict:
        return {
            "total_pipelines": self.total_pipelines,
            "healthy_count": self.healthy_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "lines": [ln.to_dict() for ln in self.lines],
        }


def _note(fr: float, status: str) -> str:
    if status == "error":
        return "pipeline in error state"
    if fr >= 0.5:
        return "critical failure rate"
    if fr >= 0.2:
        return "elevated failure rate"
    if fr > 0.0:
        return "minor failures detected"
    return "all records processed successfully"


def _tier(fr: float, status: str) -> str:
    if status == "error" or fr >= 0.5:
        return "critical"
    if status == "warning" or fr >= 0.2:
        return "warning"
    return "healthy"


def summarize_metrics(metrics: List[PipelineMetric]) -> SummarizerResult:
    """Summarize a flat list of metrics (latest per pipeline used)."""
    latest: dict[str, PipelineMetric] = {}
    for m in metrics:
        if m.pipeline not in latest or m.timestamp > latest[m.pipeline].timestamp:
            latest[m.pipeline] = m

    result = SummarizerResult(total_pipelines=len(latest))
    for pipeline, m in sorted(latest.items()):
        fr = failure_rate(m)
        tier = _tier(fr, m.status)
        line = SummaryLine(
            pipeline=pipeline,
            status=m.status,
            total_records=m.total_records,
            failed_records=m.failed_records,
            failure_rate=fr,
            note=_note(fr, m.status),
        )
        result.lines.append(line)
        if tier == "healthy":
            result.healthy_count += 1
        elif tier == "warning":
            result.warning_count += 1
        else:
            result.critical_count += 1
    return result


def format_summarizer_result(result: SummarizerResult) -> str:
    lines = [
        f"Pipelines: {result.total_pipelines}  "
        f"Healthy: {result.healthy_count}  "
        f"Warning: {result.warning_count}  "
        f"Critical: {result.critical_count}",
        "-" * 60,
    ]
    for ln in result.lines:
        pct = f"{ln.failure_rate * 100:.1f}%"
        lines.append(
            f"  {ln.pipeline:<25} [{ln.status:<8}] fail={pct:<7} {ln.note}"
        )
    return "\n".join(lines)
