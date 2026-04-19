from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from pipewatch.alerts import Alert


@dataclass
class DispatchRule:
    severity: Optional[str]  # None = match all
    pipeline: Optional[str]  # None = match all
    handler: Callable[[Alert], None]
    label: str = ""

    def matches(self, alert: Alert) -> bool:
        if self.severity and alert.severity != self.severity:
            return False
        if self.pipeline and alert.pipeline != self.pipeline:
            return False
        return True


@dataclass
class DispatchResult:
    alert: Alert
    matched_labels: List[str]
    dispatched: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "severity": self.alert.severity,
            "matched_labels": self.matched_labels,
            "dispatched": self.dispatched,
        }


class Dispatcher:
    def __init__(self) -> None:
        self._rules: List[DispatchRule] = []
        self._fallback: Optional[Callable[[Alert], None]] = None

    def add_rule(self, rule: DispatchRule) -> None:
        self._rules.append(rule)

    def set_fallback(self, handler: Callable[[Alert], None]) -> None:
        self._fallback = handler

    def dispatch(self, alert: Alert) -> DispatchResult:
        matched: List[str] = []
        for rule in self._rules:
            if rule.matches(alert):
                rule.handler(alert)
                matched.append(rule.label or "(unlabeled)")
        if not matched and self._fallback:
            self._fallback(alert)
            return DispatchResult(alert=alert, matched_labels=["(fallback)"], dispatched=True)
        return DispatchResult(alert=alert, matched_labels=matched, dispatched=bool(matched))

    def dispatch_all(self, alerts: List[Alert]) -> List[DispatchResult]:
        return [self.dispatch(a) for a in alerts]


def format_dispatch_results(results: List[DispatchResult]) -> str:
    if not results:
        return "No alerts dispatched."
    lines = []
    for r in results:
        status = "OK" if r.dispatched else "UNHANDLED"
        labels = ", ".join(r.matched_labels) if r.matched_labels else "none"
        lines.append(f"[{status}] {r.alert.pipeline} ({r.alert.severity}) -> {labels}")
    return "\n".join(lines)
