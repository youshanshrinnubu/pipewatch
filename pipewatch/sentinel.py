"""Sentinel: threshold-based guard that emits alerts when metrics cross defined limits."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


@dataclass
class SentinelRule:
    pipeline: Optional[str]  # None = match all
    max_failure_rate: Optional[float] = None  # 0.0 – 1.0
    max_failed_records: Optional[int] = None
    forbidden_statuses: List[str] = field(default_factory=list)

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline is not None and metric.pipeline != self.pipeline:
            return False
        return True

    def violations(self, metric: PipelineMetric) -> List[str]:
        msgs: List[str] = []
        total = metric.total_records or 0
        failed = metric.failed_records or 0
        rate = (failed / total) if total > 0 else 0.0

        if self.max_failure_rate is not None and rate > self.max_failure_rate:
            msgs.append(
                f"failure rate {rate:.2%} exceeds limit {self.max_failure_rate:.2%}"
            )
        if self.max_failed_records is not None and failed > self.max_failed_records:
            msgs.append(
                f"failed records {failed} exceeds limit {self.max_failed_records}"
            )
        if metric.status in self.forbidden_statuses:
            msgs.append(f"status '{metric.status}' is forbidden")
        return msgs


@dataclass
class SentinelResult:
    metric: PipelineMetric
    rule: SentinelRule
    violations: List[str]

    @property
    def triggered(self) -> bool:
        return len(self.violations) > 0

    def to_alert(self) -> Alert:
        severity = "critical" if metric_has_error_status(self.metric) else "warning"
        message = "; ".join(self.violations)
        return Alert(
            pipeline=self.metric.pipeline,
            severity=severity,
            message=f"Sentinel triggered: {message}",
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline,
            "triggered": self.triggered,
            "violations": self.violations,
        }


def metric_has_error_status(metric: PipelineMetric) -> bool:
    return metric.status in ("error", "critical")


def check_sentinel(
    metric: PipelineMetric, rules: List[SentinelRule]
) -> List[SentinelResult]:
    results: List[SentinelResult] = []
    for rule in rules:
        if rule.matches(metric):
            viols = rule.violations(metric)
            results.append(SentinelResult(metric=metric, rule=rule, violations=viols))
    return results


def check_all_sentinels(
    metrics: List[PipelineMetric], rules: List[SentinelRule]
) -> List[SentinelResult]:
    out: List[SentinelResult] = []
    for m in metrics:
        out.extend(check_sentinel(m, rules))
    return out
