"""pipewatch.stacker — accumulate metrics into a fixed-depth stack per pipeline."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class Stack:
    pipeline: str
    capacity: int
    _items: deque = field(default_factory=deque, repr=False)

    def push(self, metric: PipelineMetric) -> None:
        """Push a metric onto the stack, evicting the oldest if at capacity."""
        if len(self._items) >= self.capacity:
            self._items.popleft()
        self._items.append(metric)

    def peek(self) -> Optional[PipelineMetric]:
        """Return the most-recently pushed metric without removing it."""
        return self._items[-1] if self._items else None

    def all(self) -> List[PipelineMetric]:
        """Return all stacked metrics, oldest first."""
        return list(self._items)

    def size(self) -> int:
        return len(self._items)

    def to_dict(self) -> dict:
        from pipewatch.exporter import export_metrics_json
        import json
        return {
            "pipeline": self.pipeline,
            "capacity": self.capacity,
            "size": self.size(),
            "metrics": json.loads(export_metrics_json(self.all())),
        }


class Stacker:
    """Manages per-pipeline stacks with a shared capacity."""

    def __init__(self, capacity: int = 10) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._stacks: Dict[str, Stack] = {}

    def push(self, metric: PipelineMetric) -> None:
        name = metric.pipeline_name
        if name not in self._stacks:
            self._stacks[name] = Stack(pipeline=name, capacity=self.capacity)
        self._stacks[name].push(metric)

    def push_all(self, metrics: List[PipelineMetric]) -> None:
        for m in metrics:
            self.push(m)

    def get(self, pipeline: str) -> Optional[Stack]:
        return self._stacks.get(pipeline)

    def pipelines(self) -> List[str]:
        return sorted(self._stacks.keys())

    def all_stacks(self) -> List[Stack]:
        return [self._stacks[p] for p in self.pipelines()]

    def format_text(self) -> str:
        lines = []
        for stack in self.all_stacks():
            top = stack.peek()
            status = top.status if top else "empty"
            lines.append(
                f"{stack.pipeline}: {stack.size()}/{stack.capacity} entries, top status={status}"
            )
        return "\n".join(lines) if lines else "No stacks."
