"""Alert router: dispatch alerts to handlers based on severity or pipeline rules."""

from dataclasses import dataclass, field
from typing import Callable, List, Optional
from pipewatch.alerts import Alert


@dataclass
class RouteRule:
    handler: Callable[[Alert], None]
    severity: Optional[str] = None  # None = match all
    pipeline_prefix: Optional[str] = None  # None = match all

    def matches(self, alert: Alert) -> bool:
        if self.severity and alert.severity != self.severity:
            return False
        if self.pipeline_prefix and not alert.pipeline.startswith(self.pipeline_prefix):
            return False
        return True


@dataclass
class Router:
    rules: List[RouteRule] = field(default_factory=list)
    fallback: Optional[Callable[[Alert], None]] = None

    def add_rule(
        self,
        handler: Callable[[Alert], None],
        severity: Optional[str] = None,
        pipeline_prefix: Optional[str] = None,
    ) -> None:
        self.rules.append(RouteRule(handler=handler, severity=severity, pipeline_prefix=pipeline_prefix))

    def set_fallback(self, handler: Callable[[Alert], None]) -> None:
        self.fallback = handler

    def dispatch(self, alert: Alert) -> int:
        """Dispatch alert to all matching handlers. Returns count of handlers called."""
        matched = [r for r in self.rules if r.matches(alert)]
        if matched:
            for rule in matched:
                rule.handler(alert)
            return len(matched)
        if self.fallback:
            self.fallback(alert)
            return 1
        return 0

    def dispatch_all(self, alerts: List[Alert]) -> int:
        return sum(self.dispatch(a) for a in alerts)
