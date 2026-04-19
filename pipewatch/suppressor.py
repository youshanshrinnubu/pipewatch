"""Suppressor: skip alerts matching defined suppression patterns."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.alerts import Alert


@dataclass
class SuppressionRule:
    pipeline: Optional[str]  # None = match all
    severity: Optional[str]  # None = match all
    reason: str = ""

    def matches(self, alert: Alert) -> bool:
        if self.pipeline is not None and alert.pipeline != self.pipeline:
            return False
        if self.severity is not None and alert.severity != self.severity:
            return False
        return True


def to_dict(rule: SuppressionRule) -> dict:
    return {
        "pipeline": rule.pipeline,
        "severity": rule.severity,
        "reason": rule.reason,
    }


@dataclass
class SuppressResult:
    alert: Alert
    suppressed: bool
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "severity": self.alert.severity,
            "message": self.alert.message,
            "suppressed": self.suppressed,
            "reason": self.reason,
        }


class Suppressor:
    def __init__(self, rules: Optional[List[SuppressionRule]] = None):
        self._rules: List[SuppressionRule] = rules or []

    def add_rule(self, rule: SuppressionRule) -> None:
        self._rules.append(rule)

    def check(self, alert: Alert) -> SuppressResult:
        for rule in self._rules:
            if rule.matches(alert):
                return SuppressResult(alert=alert, suppressed=True, reason=rule.reason)
        return SuppressResult(alert=alert, suppressed=False)

    def filter(self, alerts: List[Alert]) -> List[SuppressResult]:
        return [self.check(a) for a in alerts]

    def allowed(self, alerts: List[Alert]) -> List[Alert]:
        return [r.alert for r in self.filter(alerts) if not r.suppressed]
