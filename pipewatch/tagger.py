"""Tag metrics with labels for grouping and filtering."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TaggedMetric:
    metric: PipelineMetric
    tags: Dict[str, str] = field(default_factory=dict)

    def has_tag(self, key: str, value: Optional[str] = None) -> bool:
        if key not in self.tags:
            return False
        if value is None:
            return True
        return self.tags[key] == value

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline_name,
            "tags": self.tags,
            "status": self.metric.status,
            "failure_rate": round(
                self.metric.failed_records / self.metric.total_records
                if self.metric.total_records else 0.0, 4
            ),
        }


class Tagger:
    def __init__(self) -> None:
        self._rules: List[Dict] = []

    def add_rule(self, key: str, value: str, pipeline_prefix: Optional[str] = None) -> None:
        self._rules.append({"key": key, "value": value, "prefix": pipeline_prefix})

    def tag(self, metric: PipelineMetric) -> TaggedMetric:
        tags: Dict[str, str] = {}
        for rule in self._rules:
            prefix = rule["prefix"]
            if prefix is None or metric.pipeline_name.startswith(prefix):
                tags[rule["key"]] = rule["value"]
        return TaggedMetric(metric=metric, tags=tags)

    def tag_all(self, metrics: List[PipelineMetric]) -> List[TaggedMetric]:
        return [self.tag(m) for m in metrics]


def filter_by_tag(tagged: List[TaggedMetric], key: str, value: Optional[str] = None) -> List[TaggedMetric]:
    return [t for t in tagged if t.has_tag(key, value)]
