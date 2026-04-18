"""Rate-limiting / throttle layer for alert dispatch."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert


@dataclass
class ThrottleConfig:
    window_seconds: float = 60.0
    max_per_window: int = 3


@dataclass
class _Bucket:
    count: int = 0
    window_start: float = field(default_factory=time.monotonic)


class Throttle:
    """Decides whether an alert should be dispatched based on rate limits."""

    def __init__(self, config: Optional[ThrottleConfig] = None) -> None:
        self._cfg = config or ThrottleConfig()
        self._buckets: Dict[str, _Bucket] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.severity}"

    def _reset_if_expired(self, bucket: _Bucket, now: float) -> _Bucket:
        if now - bucket.window_start >= self._cfg.window_seconds:
            return _Bucket(window_start=now)
        return bucket

    def allow(self, alert: Alert) -> bool:
        """Return True if the alert is within rate limits."""
        now = time.monotonic()
        key = self._key(alert)
        bucket = self._buckets.get(key, _Bucket(window_start=now))
        bucket = self._reset_if_expired(bucket, now)
        if bucket.count >= self._cfg.max_per_window:
            self._buckets[key] = bucket
            return False
        bucket.count += 1
        self._buckets[key] = bucket
        return True

    def reset(self, alert: Alert) -> None:
        """Manually clear the bucket for a given alert key."""
        self._buckets.pop(self._key(alert), None)

    def status(self, alert: Alert) -> Dict[str, object]:
        key = self._key(alert)
        bucket = self._buckets.get(key)
        if bucket is None:
            return {"key": key, "count": 0, "remaining": self._cfg.max_per_window}
        remaining = max(0, self._cfg.max_per_window - bucket.count)
        return {"key": key, "count": bucket.count, "remaining": remaining}
