"""mapper.py — Map pipeline metrics through transformation rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MapRule:
    """A single transformation rule applied to matching metrics."""
    pipeline_prefix: Optional[str]  # None = match all
    transform: Callable[[PipelineMetric], PipelineMetric]
    label: str = "unnamed"

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline_prefix is None:
            return True
        return metric.pipeline_name.startswith(self.pipeline_prefix)


@dataclass
class MapResult:
    original: PipelineMetric
    mapped: PipelineMetric
    rules_applied: List[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return self.original is not self.mapped

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.mapped.pipeline_name,
            "rules_applied": self.rules_applied,
            "changed": self.changed,
            "original_status": self.original.status,
            "mapped_status": self.mapped.status,
            "original_failed": self.original.failed_records,
            "mapped_failed": self.mapped.failed_records,
        }


class Mapper:
    """Applies a chain of MapRules to pipeline metrics."""

    def __init__(self) -> None:
        self._rules: List[MapRule] = []

    def add_rule(self, rule: MapRule) -> None:
        self._rules.append(rule)

    def apply(self, metric: PipelineMetric) -> MapResult:
        current = metric
        applied: List[str] = []
        for rule in self._rules:
            if rule.matches(current):
                current = rule.transform(current)
                applied.append(rule.label)
        return MapResult(original=metric, mapped=current, rules_applied=applied)

    def apply_all(self, metrics: List[PipelineMetric]) -> List[MapResult]:
        return [self.apply(m) for m in metrics]


def format_mapped(results: List[MapResult], fmt: str = "text") -> str:
    import json
    if fmt == "json":
        return json.dumps([r.to_dict() for r in results], indent=2)
    lines = []
    for r in results:
        tag = "(changed)" if r.changed else "(unchanged)"
        rules = ", ".join(r.rules_applied) if r.rules_applied else "none"
        lines.append(f"{r.mapped.pipeline_name}: {tag} rules=[{rules}] status={r.mapped.status}")
    return "\n".join(lines) if lines else "No metrics mapped."
