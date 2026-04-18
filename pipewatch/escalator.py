"""Alert escalation: upgrade severity when an alert repeats beyond a threshold."""
from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Callable, Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class EscalationConfig:
    warn_to_critical_after: int = 3   # repeated warnings before escalating
    window_seconds: float = 300.0     # rolling window to count repeats


@dataclass
class _Record:
    timestamps: List[float] = field(default_factory=list)


def _alert_key(alert: Alert) -> str:
    return f"{alert.pipeline}:{alert.severity}"


class Escalator:
    """Tracks repeated alerts and escalates severity when threshold is exceeded."""

    def __init__(self, config: Optional[EscalationConfig] = None) -> None:
        self.config = config or EscalationConfig()
        self._records: Dict[str, _Record] = {}

    def _prune(self, record: _Record, now: float) -> None:
        cutoff = now - self.config.window_seconds
        record.timestamps = [t for t in record.timestamps if t >= cutoff]

    def process(self, alert: Alert) -> Alert:
        """Return the alert, possibly with upgraded severity."""
        if alert.severity == "critical":
            return alert

        now = time()
        key = _alert_key(alert)
        record = self._records.setdefault(key, _Record())
        self._prune(record, now)
        record.timestamps.append(now)

        if len(record.timestamps) >= self.config.warn_to_critical_after:
            return Alert(
                pipeline=alert.pipeline,
                severity="critical",
                message=f"[ESCALATED] {alert.message}",
                metric=alert.metric,
            )
        return alert

    def process_all(self, alerts: List[Alert]) -> List[Alert]:
        return [self.process(a) for a in alerts]

    def repeat_count(self, alert: Alert) -> int:
        """Return how many times this alert has been seen in the current window."""
        record = self._records.get(_alert_key(alert))
        if record is None:
            return 0
        now = time()
        self._prune(record, now)
        return len(record.timestamps)
