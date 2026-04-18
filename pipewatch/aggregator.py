"""Aggregate metrics across multiple pipeline runs."""
from typing import List, Dict, Optional
from dataclasses import dataclass
from pipewatch.metrics import PipelineMetric, failure_rate


@dataclass
class AggregatedStats:
    pipeline: str
    total_runs: int
    avg_failure_rate: float
    max_failure_rate: float
    error_count: int
    ok_count: int
    avg_records_processed: float


def aggregate(metrics: List[PipelineMetric]) -> Optional[AggregatedStats]:
    """Aggregate a list of metrics for a single pipeline."""
    if not metrics:
        return None

    pipeline = metrics[0].pipeline
    rates = [failure_rate(m) for m in metrics]
    statuses = [m.status for m in metrics]

    return AggregatedStats(
        pipeline=pipeline,
        total_runs=len(metrics),
        avg_failure_rate=sum(rates) / len(rates),
        max_failure_rate=max(rates),
        error_count=statuses.count("error"),
        ok_count=statuses.count("ok"),
        avg_records_processed=sum(m.records_processed for m in metrics) / len(metrics),
    )


def aggregate_all(metrics: List[PipelineMetric]) -> Dict[str, AggregatedStats]:
    """Group metrics by pipeline and aggregate each group."""
    grouped: Dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        grouped.setdefault(m.pipeline, []).append(m)

    result = {}
    for pipeline, group in grouped.items():
        stats = aggregate(group)
        if stats:
            result[pipeline] = stats
    return result


def format_aggregated(stats: AggregatedStats) -> str:
    lines = [
        f"Pipeline : {stats.pipeline}",
        f"Runs     : {stats.total_runs}",
        f"Avg Fail : {stats.avg_failure_rate:.2%}",
        f"Max Fail : {stats.max_failure_rate:.2%}",
        f"OK/Error : {stats.ok_count}/{stats.error_count}",
        f"Avg Recs : {stats.avg_records_processed:.1f}",
    ]
    return "\n".join(lines)
