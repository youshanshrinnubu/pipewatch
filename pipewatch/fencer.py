"""fencer.py — Circuit-breaker style fence for pipeline metrics.

A fence trips open when a pipeline's failure rate exceeds a threshold for
a configurable number of consecutive checks, and resets (closes) once the
pipeline stays healthy for a configurable cooldown count.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class FenceConfig:
    trip_threshold: float = 0.3    # failure rate to count as a bad check
    trip_count: int = 3            # consecutive bad checks before tripping
    reset_count: int = 2           # consecutive good checks to close again


@dataclass
class FenceState:
    pipeline: str
    open: bool = False
    consecutive_bad: int = 0
    consecutive_good: int = 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "open": self.open,
            "consecutive_bad": self.consecutive_bad,
            "consecutive_good": self.consecutive_good,
        }


@dataclass
class FenceResult:
    state: FenceState
    tripped: bool   # True if fence just opened this check
    reset: bool     # True if fence just closed this check

    def to_dict(self) -> dict:
        return {
            "state": self.state.to_dict(),
            "tripped": self.tripped,
            "reset": self.reset,
        }


class Fencer:
    def __init__(self, config: Optional[FenceConfig] = None) -> None:
        self.config = config or FenceConfig()
        self._states: Dict[str, FenceState] = {}

    def _get_state(self, pipeline: str) -> FenceState:
        if pipeline not in self._states:
            self._states[pipeline] = FenceState(pipeline=pipeline)
        return self._states[pipeline]

    def check(self, metric: PipelineMetric) -> FenceResult:
        state = self._get_state(metric.pipeline_name)
        cfg = self.config
        failure_rate = (
            metric.failed_records / metric.total_records
            if metric.total_records > 0
            else 0.0
        )
        is_bad = failure_rate >= cfg.trip_threshold or metric.status == "error"
        tripped = False
        reset = False

        if state.open:
            if not is_bad:
                state.consecutive_good += 1
                state.consecutive_bad = 0
                if state.consecutive_good >= cfg.reset_count:
                    state.open = False
                    state.consecutive_good = 0
                    reset = True
            else:
                state.consecutive_good = 0
        else:
            if is_bad:
                state.consecutive_bad += 1
                state.consecutive_good = 0
                if state.consecutive_bad >= cfg.trip_count:
                    state.open = True
                    state.consecutive_bad = 0
                    tripped = True
            else:
                state.consecutive_bad = 0

        return FenceResult(state=state, tripped=tripped, reset=reset)

    def check_all(self, metrics: List[PipelineMetric]) -> List[FenceResult]:
        return [self.check(m) for m in metrics]

    def states(self) -> List[FenceState]:
        return list(self._states.values())
