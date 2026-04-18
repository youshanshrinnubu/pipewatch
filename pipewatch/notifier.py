"""Notification rate-limiting and deduplication for alerts."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert


@dataclass
class NotifierConfig:
    cooldown_seconds: int = 300
    max_repeats: int = 3


@dataclass
class _State:
    last_sent: float = 0.0
    repeat_count: int = 0


class Notifier:
    """Wraps an AlertManager handler with rate-limiting."""

    def __init__(self, config: Optional[NotifierConfig] = None) -> None:
        self.config = config or NotifierConfig()
        self._states: Dict[str, _State] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.severity}"

    def should_send(self, alert: Alert) -> bool:
        key = self._key(alert)
        now = time.time()
        state = self._states.get(key)
        if state is None:
            return True
        elapsed = now - state.last_sent
        if elapsed >= self.config.cooldown_seconds:
            return True
        if state.repeat_count < self.config.max_repeats:
            return True
        return False

    def record_sent(self, alert: Alert) -> None:
        key = self._key(alert)
        now = time.time()
        state = self._states.get(key)
        if state is None or (now - state.last_sent) >= self.config.cooldown_seconds:
            self._states[key] = _State(last_sent=now, repeat_count=1)
        else:
            state.repeat_count += 1
            state.last_sent = now

    def notify(self, alert: Alert, handler) -> bool:
        """Send alert through handler if rate-limit allows. Returns True if sent."""
        if self.should_send(alert):
            handler(alert)
            self.record_sent(alert)
            return True
        return False

    def reset(self, pipeline: Optional[str] = None) -> None:
        if pipeline is None:
            self._states.clear()
        else:
            keys = [k for k in self._states if k.startswith(f"{pipeline}:")]
            for k in keys:
                del self._states[k]
