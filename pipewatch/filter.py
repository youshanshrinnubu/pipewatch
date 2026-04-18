"""Filtering utilities for pipeline metrics and alerts."""

from typing import List, Optional, Callable
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


def filter_metrics_by_status(
    metrics: List[PipelineMetric], statuses: List[str]
) -> List[PipelineMetric]:
    """Return only metrics whose status is in the given list."""
    return [m for m in metrics if m.status in statuses]


def filter_metrics_by_pipeline(
    metrics: List[PipelineMetric], pipeline_names: List[str]
) -> List[PipelineMetric]:
    """Return only metrics for the specified pipeline names."""
    return [m for m in metrics if m.pipeline_name in pipeline_names]


def filter_metrics_by_failure_rate(
    metrics: List[PipelineMetric],
    min_rate: float = 0.0,
    max_rate: float = 1.0,
) -> List[PipelineMetric]:
    """Return metrics whose failure rate falls within [min_rate, max_rate]."""
    from pipewatch.metrics import failure_rate

    return [
        m
        for m in metrics
        if min_rate <= failure_rate(m) <= max_rate
    ]


def filter_alerts_by_severity(
    alerts: List[Alert], severities: List[str]
) -> List[Alert]:
    """Return only alerts matching the given severity levels."""
    return [a for a in alerts if a.severity in severities]


def filter_alerts_by_pipeline(
    alerts: List[Alert], pipeline_names: List[str]
) -> List[Alert]:
    """Return only alerts for the specified pipeline names."""
    return [a for a in alerts if a.pipeline_name in pipeline_names]


def compose_filters(filters: List[Callable[[List], List]]) -> Callable[[List], List]:
    """Compose multiple filter functions into a single filter."""
    def combined(items):
        for f in filters:
            items = f(items)
        return items
    return combined
