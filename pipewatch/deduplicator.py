"""Deduplicator: suppress duplicate alerts within a time window."""
from __future__ import annotations
import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.alerts import Alert


@dataclass
class DeduplicatorConfig:
    window_seconds: float = 60.0


@dataclass
class _Entry:
    first_seen: float
    last_seen: float
    count: int = 1


def _alert_key(alert: Alert) -> str:
    raw = f"{alert.pipeline}:{alert.severity}:{alert.message}"
    return hashlib.md5(raw.encode()).hexdigest()


class Deduplicator:
    def __init__(self, config: Optional[DeduplicatorConfig] = None) -> None:
        self._config = config or DeduplicatorConfig()
        self._seen: Dict[str, _Entry] = {}

    def _now(self) -> float:
        return time.time()

    def _evict(self) -> None:
        cutoff = self._now() - self._config.window_seconds
        self._seen = {k: v for k, v in self._seen.items() if v.last_seen >= cutoff}

    def is_duplicate(self, alert: Alert) -> bool:
        self._evict()
        key = _alert_key(alert)
        return key in self._seen

    def record(self, alert: Alert) -> None:
        self._evict()
        key = _alert_key(alert)
        now = self._now()
        if key in self._seen:
            e = self._seen[key]
            e.last_seen = now
            e.count += 1
        else:
            self._seen[key] = _Entry(first_seen=now, last_seen=now)

    def filter(self, alerts: List[Alert]) -> List[Alert]:
        result = []
        for alert in alerts:
            if not self.is_duplicate(alert):
                self.record(alert)
                result.append(alert)
        return result

    def stats(self) -> Dict[str, int]:
        self._evict()
        return {k: v.count for k, v in self._seen.items()}
