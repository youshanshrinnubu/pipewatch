"""cursor.py — Tracks a named read position (cursor) across a snapshot store.

Allows incremental processing: remember the last-seen metric timestamp per
pipeline so only new metrics are processed on the next run.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CursorState:
    """Persisted cursor positions keyed by pipeline name."""
    positions: Dict[str, float] = field(default_factory=dict)

    def get(self, pipeline: str) -> Optional[float]:
        """Return the last-seen timestamp for *pipeline*, or None."""
        return self.positions.get(pipeline)

    def advance(self, pipeline: str, timestamp: float) -> None:
        """Move the cursor forward to *timestamp* for *pipeline*."""
        current = self.positions.get(pipeline)
        if current is None or timestamp > current:
            self.positions[pipeline] = timestamp

    def reset(self, pipeline: Optional[str] = None) -> None:
        """Reset cursor for one pipeline, or all if *pipeline* is None."""
        if pipeline is None:
            self.positions.clear()
        else:
            self.positions.pop(pipeline, None)


class CursorStore:
    """Persist and load a :class:`CursorState` to/from a JSON file."""

    def __init__(self, path: str) -> None:
        self._path = path

    def load(self) -> CursorState:
        if not os.path.exists(self._path):
            return CursorState()
        with open(self._path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return CursorState(positions={str(k): float(v) for k, v in raw.items()})

    def save(self, state: CursorState) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(state.positions, fh, indent=2)


def new_metrics(
    metrics: List[PipelineMetric],
    state: CursorState,
) -> List[PipelineMetric]:
    """Return only metrics whose timestamp is strictly after the cursor.

    Metrics with no cursor position (first run) are all considered new.
    """
    result: List[PipelineMetric] = []
    for m in metrics:
        ts = m.timestamp
        cursor = state.get(m.pipeline_name)
        if cursor is None or ts > cursor:
            result.append(m)
    return result


def advance_all(metrics: List[PipelineMetric], state: CursorState) -> None:
    """Advance the cursor for every metric in *metrics*."""
    for m in metrics:
        state.advance(m.pipeline_name, m.timestamp)
