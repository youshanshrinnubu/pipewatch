"""Enricher: attach contextual metadata to pipeline metrics."""
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class EnrichedMetric:
    metric: PipelineMetric
    meta: Dict[str, Any] = field(default_factory=dict)

    def has_meta(self, key: str) -> bool:
        return key in self.meta

    def to_dict(self) -> Dict[str, Any]:
        base = {
            "pipeline": self.metric.pipeline_name,
            "status": self.metric.status,
            "records_processed": self.metric.records_processed,
            "records_failed": self.metric.records_failed,
            "meta": self.meta,
        }
        return base


@dataclass
class EnrichmentRule:
    key: str
    value: Any
    pipeline_prefix: Optional[str] = None


class Enricher:
    def __init__(self) -> None:
        self._rules: List[EnrichmentRule] = []

    def add_rule(self, key: str, value: Any, pipeline_prefix: Optional[str] = None) -> None:
        self._rules.append(EnrichmentRule(key=key, value=value, pipeline_prefix=pipeline_prefix))

    def enrich(self, metric: PipelineMetric) -> EnrichedMetric:
        meta: Dict[str, Any] = {}
        for rule in self._rules:
            if rule.pipeline_prefix is None or metric.pipeline_name.startswith(rule.pipeline_prefix):
                meta[rule.key] = rule.value
        return EnrichedMetric(metric=metric, meta=meta)

    def enrich_all(self, metrics: List[PipelineMetric]) -> List[EnrichedMetric]:
        return [self.enrich(m) for m in metrics]
