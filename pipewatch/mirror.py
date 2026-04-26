"""Mirror module: duplicate pipeline metrics to one or more secondary destinations."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MirrorRule:
    """A rule that selects which pipelines to mirror and where to send them."""
    destination: str
    pipeline_prefix: Optional[str] = None  # None means mirror all pipelines

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline_prefix is None:
            return True
        return metric.pipeline_name.startswith(self.pipeline_prefix)


@dataclass
class MirrorResult:
    metric: PipelineMetric
    destinations: List[str] = field(default_factory=list)
    skipped: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline_name,
            "destinations": self.destinations,
            "skipped": self.skipped,
        }


class Mirror:
    """Routes metrics to registered destination handlers based on mirror rules."""

    def __init__(self) -> None:
        self._rules: List[MirrorRule] = []
        self._handlers: Dict[str, Callable[[PipelineMetric], None]] = {}

    def add_rule(self, rule: MirrorRule) -> None:
        self._rules.append(rule)

    def register_destination(
        self, name: str, handler: Callable[[PipelineMetric], None]
    ) -> None:
        self._handlers[name] = handler

    def send(self, metric: PipelineMetric) -> MirrorResult:
        """Apply all matching rules and invoke destination handlers."""
        result = MirrorResult(metric=metric)
        for rule in self._rules:
            if rule.matches(metric) and rule.destination in self._handlers:
                self._handlers[rule.destination](metric)
                if rule.destination not in result.destinations:
                    result.destinations.append(rule.destination)
        result.skipped = len(result.destinations) == 0
        return result

    def send_all(self, metrics: List[PipelineMetric]) -> List[MirrorResult]:
        return [self.send(m) for m in metrics]
