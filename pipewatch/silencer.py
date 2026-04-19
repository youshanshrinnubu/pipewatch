"""Silencer: suppress alerts for specific pipelines or conditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class SilenceRule:
    pipeline: Optional[str] = None   # None means match all
    severity: Optional[str] = None   # None means match all
    expires_at: Optional[datetime] = None  # None means never expires
    reason: str = ""

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def matches(self, alert: Alert) -> bool:
        if self.is_expired():
            return False
        if self.pipeline is not None and alert.pipeline != self.pipeline:
            return False
        if self.severity is not None and alert.severity != self.severity:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "severity": self.severity,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SilenceRule:
        """Deserialise a SilenceRule from a dictionary (e.g. loaded from JSON/YAML)."""
        expires_at = None
        if data.get("expires_at") is not None:
            expires_at = datetime.fromisoformat(data["expires_at"])
        return cls(
            pipeline=data.get("pipeline"),
            severity=data.get("severity"),
            expires_at=expires_at,
            reason=data.get("reason", ""),
        )


@dataclass
class Silencer:
    rules: List[SilenceRule] = field(default_factory=list)

    def add_rule(self, rule: SilenceRule) -> None:
        self.rules.append(rule)

    def is_silenced(self, alert: Alert) -> bool:
        return any(r.matches(alert) for r in self.rules)

    def filter(self, alerts: List[Alert]) -> List[Alert]:
        return [a for a in alerts if not self.is_silenced(a)]

    def purge_expired(self) -> int:
        before = len(self.rules)
        self.rules = [r for r in self.rules if not r.is_expired()]
        return before - len(self.rules)

    def active_rules(self) -> List[SilenceRule]:
        return [r for r in self.rules if not r.is_expired()]
