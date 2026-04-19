"""linker.py — Link related alerts across pipelines based on shared timing or failure patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class LinkedGroup:
    """A group of alerts that are likely related."""

    reason: str  # 'time_window' or 'failure_pattern'
    alerts: List[Alert] = field(default_factory=list)

    @property
    def pipelines(self) -> List[str]:
        return list({a.pipeline for a in self.alerts})

    @property
    def severities(self) -> List[str]:
        return list({a.severity for a in self.alerts})

    def to_dict(self) -> dict:
        return {
            "reason": self.reason,
            "pipeline_count": len(self.pipelines),
            "pipelines": self.pipelines,
            "severities": self.severities,
            "alert_count": len(self.alerts),
            "alerts": [
                {"pipeline": a.pipeline, "severity": a.severity, "message": a.message}
                for a in self.alerts
            ],
        }


def link_by_time_window(
    alerts: List[Alert], window_seconds: float = 60.0
) -> List[LinkedGroup]:
    """Group alerts that fired within the same time window.

    Alerts without a timestamp are ignored.
    """
    import time

    timed = [(a, getattr(a, "timestamp", None)) for a in alerts]
    timed = [(a, ts) for a, ts in timed if ts is not None]
    if not timed:
        return []

    timed.sort(key=lambda x: x[1])
    groups: List[LinkedGroup] = []
    current: List[Alert] = [timed[0][0]]
    window_start = timed[0][1]

    for alert, ts in timed[1:]:
        if ts - window_start <= window_seconds:
            current.append(alert)
        else:
            if len(current) > 1:
                groups.append(LinkedGroup(reason="time_window", alerts=list(current)))
            current = [alert]
            window_start = ts

    if len(current) > 1:
        groups.append(LinkedGroup(reason="time_window", alerts=list(current)))

    return groups


def link_by_failure_pattern(
    alerts: List[Alert], min_group_size: int = 2
) -> List[LinkedGroup]:
    """Group alerts that share the same severity across multiple pipelines."""
    by_severity: Dict[str, List[Alert]] = {}
    for alert in alerts:
        by_severity.setdefault(alert.severity, []).append(alert)

    groups: List[LinkedGroup] = []
    for severity, members in by_severity.items():
        pipelines = {a.pipeline for a in members}
        if len(pipelines) >= min_group_size:
            groups.append(LinkedGroup(reason="failure_pattern", alerts=members))

    return groups


def link_all(
    alerts: List[Alert],
    window_seconds: float = 60.0,
    min_group_size: int = 2,
) -> List[LinkedGroup]:
    """Run all linking strategies and return combined results."""
    results: List[LinkedGroup] = []
    results.extend(link_by_time_window(alerts, window_seconds=window_seconds))
    results.extend(link_by_failure_pattern(alerts, min_group_size=min_group_size))
    return results


def format_linked(groups: List[LinkedGroup]) -> str:
    """Return a human-readable summary of linked alert groups."""
    if not groups:
        return "No linked alert groups found."
    lines = []
    for i, g in enumerate(groups, 1):
        lines.append(
            f"[Group {i}] reason={g.reason} pipelines={g.pipelines} "
            f"severities={g.severities} alerts={len(g.alerts)}"
        )
    return "\n".join(lines)
