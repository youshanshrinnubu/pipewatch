"""Replay historical metrics through alert evaluation for retrospective analysis."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert, AlertManager


@dataclass
class ReplayEvent:
    index: int
    metric: PipelineMetric
    alerts: List[Alert] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "pipeline": self.metric.pipeline_name,
            "status": self.metric.status,
            "failure_rate": self.metric.failed_records / self.metric.total_records
            if self.metric.total_records else 0.0,
            "alerts": [a.to_dict() for a in self.alerts],
        }


def replay(
    metrics: List[PipelineMetric],
    warn_threshold: float = 0.05,
    critical_threshold: float = 0.20,
) -> List[ReplayEvent]:
    """Replay a list of metrics through alert evaluation, returning one event per metric."""
    manager = AlertManager(
        warn_threshold=warn_threshold,
        critical_threshold=critical_threshold,
    )
    events: List[ReplayEvent] = []
    for i, metric in enumerate(metrics):
        alerts = manager.evaluate(metric)
        events.append(ReplayEvent(index=i, metric=metric, alerts=alerts))
    return events


def replay_summary(events: List[ReplayEvent]) -> dict:
    total = len(events)
    alerted = sum(1 for e in events if e.alerts)
    critical = sum(
        1 for e in events if any(a.severity == "critical" for a in e.alerts)
    )
    return {
        "total_events": total,
        "events_with_alerts": alerted,
        "critical_events": critical,
    }
