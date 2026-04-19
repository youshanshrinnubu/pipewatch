"""Group metrics or alerts by a shared attribute."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


@dataclass
class MetricGroup:
    key: str
    metrics: List[PipelineMetric] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "count": len(self.metrics),
            "pipelines": [m.pipeline_name for m in self.metrics],
        }


@dataclass
class AlertGroup:
    key: str
    alerts: List[Alert] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "count": len(self.alerts),
            "pipelines": [a.pipeline_name for a in self.alerts],
        }


def group_metrics_by_status(metrics: List[PipelineMetric]) -> Dict[str, MetricGroup]:
    groups: Dict[str, MetricGroup] = {}
    for m in metrics:
        key = m.status
        if key not in groups:
            groups[key] = MetricGroup(key=key)
        groups[key].metrics.append(m)
    return groups


def group_metrics_by_pipeline(metrics: List[PipelineMetric]) -> Dict[str, MetricGroup]:
    groups: Dict[str, MetricGroup] = {}
    for m in metrics:
        key = m.pipeline_name
        if key not in groups:
            groups[key] = MetricGroup(key=key)
        groups[key].metrics.append(m)
    return groups


def group_alerts_by_severity(alerts: List[Alert]) -> Dict[str, AlertGroup]:
    groups: Dict[str, AlertGroup] = {}
    for a in alerts:
        key = a.severity
        if key not in groups:
            groups[key] = AlertGroup(key=key)
        groups[key].alerts.append(a)
    return groups


def format_groups(groups: Dict[str, MetricGroup]) -> str:
    lines = []
    for key, grp in sorted(groups.items()):
        lines.append(f"[{key}] {grp.count} pipeline(s): {', '.join(grp.pipelines)}")
    return "\n".join(lines) if lines else "No groups."
