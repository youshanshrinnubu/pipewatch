"""Alerting hooks for pipeline health events."""
from dataclasses import dataclass, field
from typing import Callable, List, Optional
from datetime import datetime
from pipewatch.metrics import PipelineMetric, is_healthy


@dataclass
class Alert:
    pipeline_name: str
    reason: str
    severity: str  # 'warning' | 'critical'
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    metric_snapshot: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "reason": self.reason,
            "severity": self.severity,
            "triggered_at": self.triggered_at.isoformat(),
            "metric_snapshot": self.metric_snapshot,
        }


AlertHandler = Callable[[Alert], None]


class AlertManager:
    def __init__(self, failure_rate_threshold: float = 0.1):
        self.failure_rate_threshold = failure_rate_threshold
        self._handlers: List[AlertHandler] = []

    def register_handler(self, handler: AlertHandler) -> None:
        """Register a callable that receives Alert objects."""
        self._handlers.append(handler)

    def evaluate(self, metric: PipelineMetric) -> Optional[Alert]:
        """Evaluate a metric and fire an alert if unhealthy."""
        from pipewatch.metrics import failure_rate, to_dict

        rate = failure_rate(metric)
        healthy = is_healthy(metric, self.failure_rate_threshold)

        if healthy:
            return None

        severity = "critical" if rate >= 0.25 or metric.status == "error" else "warning"
        reason = (
            f"Pipeline '{metric.pipeline_name}' unhealthy: "
            f"status={metric.status}, failure_rate={rate:.2%}"
        )
        alert = Alert(
            pipeline_name=metric.pipeline_name,
            reason=reason,
            severity=severity,
            metric_snapshot=to_dict(metric),
        )
        self._fire(alert)
        return alert

    def _fire(self, alert: Alert) -> None:
        for handler in self._handlers:
            handler(alert)
