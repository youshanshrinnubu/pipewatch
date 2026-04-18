"""Severity labeling for pipeline metrics based on configurable thresholds."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class LabelRule:
    label: str
    min_failure_rate: float = 0.0
    max_failure_rate: float = 1.0
    statuses: List[str] = field(default_factory=list)


@dataclass
class LabeledMetric:
    metric: PipelineMetric
    labels: List[str]

    def has_label(self, label: str) -> bool:
        return label in self.labels

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.metric.pipeline_name,
            "labels": self.labels,
            "failure_rate": self.metric.failure_rate,
            "status": self.metric.status,
        }


class Labeler:
    def __init__(self, rules: Optional[List[LabelRule]] = None):
        self.rules: List[LabelRule] = rules or _default_rules()

    def label(self, metric: PipelineMetric) -> LabeledMetric:
        matched = []
        for rule in self.rules:
            rate_ok = rule.min_failure_rate <= metric.failure_rate <= rule.max_failure_rate
            status_ok = not rule.statuses or metric.status in rule.statuses
            if rate_ok and status_ok:
                matched.append(rule.label)
        return LabeledMetric(metric=metric, labels=matched)

    def label_all(self, metrics: List[PipelineMetric]) -> List[LabeledMetric]:
        return [self.label(m) for m in metrics]


def _default_rules() -> List[LabelRule]:
    return [
        LabelRule(label="healthy", max_failure_rate=0.05, statuses=["ok"]),
        LabelRule(label="degraded", min_failure_rate=0.05, max_failure_rate=0.2),
        LabelRule(label="critical", min_failure_rate=0.2),
        LabelRule(label="error", statuses=["error"]),
    ]
