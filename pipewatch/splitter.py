"""Split a list of metrics or alerts into partitions based on a key function."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Dict, Generic, List, TypeVar

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert

T = TypeVar("T")


@dataclass
class SplitResult(Generic[T]):
    key: str
    items: List[T] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"key": self.key, "count": len(self.items)}


def split(items: List[T], key_fn: Callable[[T], str]) -> Dict[str, SplitResult[T]]:
    """Partition *items* by the string returned by *key_fn*."""
    result: Dict[str, SplitResult[T]] = {}
    for item in items:
        k = key_fn(item)
        if k not in result:
            result[k] = SplitResult(key=k)
        result[k].items.append(item)
    return result


def split_metrics_by_status(metrics: List[PipelineMetric]) -> Dict[str, SplitResult[PipelineMetric]]:
    return split(metrics, lambda m: m.status)


def split_metrics_by_pipeline(metrics: List[PipelineMetric]) -> Dict[str, SplitResult[PipelineMetric]]:
    return split(metrics, lambda m: m.pipeline_name)


def split_alerts_by_severity(alerts: List[Alert]) -> Dict[str, SplitResult[Alert]]:
    return split(alerts, lambda a: a.severity)


def split_alerts_by_pipeline(alerts: List[Alert]) -> Dict[str, SplitResult[Alert]]:
    return split(alerts, lambda a: a.pipeline_name)


def format_split(results: Dict[str, SplitResult]) -> str:
    if not results:
        return "No items to split."
    lines = []
    for key, sr in sorted(results.items()):
        lines.append(f"  {key}: {len(sr.items)} item(s)")
    return "\n".join(lines)
