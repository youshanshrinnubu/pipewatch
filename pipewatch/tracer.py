"""Trace pipeline metric history and highlight state transitions."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TraceEvent:
    pipeline: str
    timestamp: float
    status: str
    failure_rate: float
    transition: Optional[str] = None  # e.g. "ok->warning"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "timestamp": self.timestamp,
            "status": self.status,
            "failure_rate": round(self.failure_rate, 4),
            "transition": self.transition,
        }


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records == 0:
        return 0.0
    return m.failed_records / m.total_records


def trace_pipeline(metrics: List[PipelineMetric]) -> List[TraceEvent]:
    """Build a trace of events for a single pipeline, marking transitions."""
    sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)
    events: List[TraceEvent] = []
    prev_status: Optional[str] = None

    for m in sorted_metrics:
        rate = _failure_rate(m)
        transition = None
        if prev_status is not None and prev_status != m.status:
            transition = f"{prev_status}->{m.status}"
        events.append(TraceEvent(
            pipeline=m.pipeline_name,
            timestamp=m.timestamp,
            status=m.status,
            failure_rate=rate,
            transition=transition,
        ))
        prev_status = m.status

    return events


def trace_all(metrics: List[PipelineMetric]) -> dict:
    """Group and trace all pipelines."""
    grouped: dict = {}
    for m in metrics:
        grouped.setdefault(m.pipeline_name, []).append(m)
    return {name: trace_pipeline(ms) for name, ms in grouped.items()}


def format_trace(events: List[TraceEvent]) -> str:
    if not events:
        return "  (no events)"
    lines = []
    for e in events:
        tag = f" [{e.transition}]" if e.transition else ""
        lines.append(f"  {e.timestamp:.1f}  {e.status:<10}  fr={e.failure_rate:.2%}{tag}")
    return "\n".join(lines)
