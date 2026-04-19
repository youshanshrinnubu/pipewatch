"""Correlate alerts across pipelines to detect shared failure patterns."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.alerts import Alert


@dataclass
class CorrelationGroup:
    severity: str
    pipelines: List[str]
    alerts: List[Alert]
    message_sample: str

    def to_dict(self) -> dict:
        return {
            "severity": self.severity,
            "pipelines": self.pipelines,
            "alert_count": len(self.alerts),
            "message_sample": self.message_sample,
        }


def correlate_by_severity(alerts: List[Alert]) -> List[CorrelationGroup]:
    """Group alerts that share the same severity across multiple pipelines."""
    buckets: Dict[str, List[Alert]] = {}
    for alert in alerts:
        buckets.setdefault(alert.severity, []).append(alert)

    groups = []
    for severity, group_alerts in buckets.items():
        pipelines = list({a.pipeline for a in group_alerts})
        if len(pipelines) < 2:
            continue
        groups.append(CorrelationGroup(
            severity=severity,
            pipelines=sorted(pipelines),
            alerts=group_alerts,
            message_sample=group_alerts[0].message,
        ))
    return groups


def correlate_by_message(alerts: List[Alert], min_pipelines: int = 2) -> List[CorrelationGroup]:
    """Group alerts that share the same message across multiple pipelines."""
    buckets: Dict[str, List[Alert]] = {}
    for alert in alerts:
        buckets.setdefault(alert.message, []).append(alert)

    groups = []
    for message, group_alerts in buckets.items():
        pipelines = list({a.pipeline for a in group_alerts})
        if len(pipelines) < min_pipelines:
            continue
        severity = group_alerts[0].severity
        groups.append(CorrelationGroup(
            severity=severity,
            pipelines=sorted(pipelines),
            alerts=group_alerts,
            message_sample=message,
        ))
    return groups


def format_correlation(groups: List[CorrelationGroup]) -> str:
    if not groups:
        return "No correlated alert groups found."
    lines = []
    for g in groups:
        lines.append(f"[{g.severity.upper()}] {len(g.alerts)} alerts across {len(g.pipelines)} pipelines: {', '.join(g.pipelines)}")
        lines.append(f"  Sample: {g.message_sample}")
    return "\n".join(lines)
