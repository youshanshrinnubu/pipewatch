"""Summary statistics computed from historical snapshots."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, failure_rate


@dataclass
class PipelineSummary:
    pipeline_name: str
    total_runs: int
    avg_failure_rate: float
    avg_duration_seconds: float
    last_status: Optional[str]
    error_run_count: int


def summarize(metrics: List[PipelineMetric]) -> Optional[PipelineSummary]:
    if not metrics:
        return None

    name = metrics[0].pipeline_name
    total = len(metrics)
    avg_fr = sum(failure_rate(m) for m in metrics) / total
    avg_dur = sum(m.duration_seconds for m in metrics) / total
    last_status = metrics[-1].status
    error_count = sum(1 for m in metrics if m.status == "error")

    return PipelineSummary(
        pipeline_name=name,
        total_runs=total,
        avg_failure_rate=round(avg_fr, 4),
        avg_duration_seconds=round(avg_dur, 2),
        last_status=last_status,
        error_run_count=error_count,
    )


def summarize_all(metrics: List[PipelineMetric]) -> List[PipelineSummary]:
    by_pipeline: dict = {}
    for m in metrics:
        by_pipeline.setdefault(m.pipeline_name, []).append(m)
    results = []
    for name, ms in by_pipeline.items():
        s = summarize(ms)
        if s:
            results.append(s)
    return results


def format_summary(s: PipelineSummary) -> str:
    return (
        f"[{s.pipeline_name}] runs={s.total_runs} "
        f"avg_failure_rate={s.avg_failure_rate:.2%} "
        f"avg_duration={s.avg_duration_seconds}s "
        f"last_status={s.last_status} "
        f"error_runs={s.error_run_count}"
    )
