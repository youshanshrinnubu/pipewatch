"""Baseline management: store and compare against a known-good snapshot."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import _metric_to_dict, _dict_to_metric


@dataclass
class BaselineEntry:
    pipeline: str
    failure_rate: float
    status: str


@dataclass
class BaselineViolation:
    pipeline: str
    baseline_failure_rate: float
    current_failure_rate: float
    baseline_status: str
    current_status: str

    @property
    def is_regression(self) -> bool:
        return (
            self.current_failure_rate > self.baseline_failure_rate
            or (self.baseline_status == "ok" and self.current_status != "ok")
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "baseline_failure_rate": self.baseline_failure_rate,
            "current_failure_rate": self.current_failure_rate,
            "baseline_status": self.baseline_status,
            "current_status": self.current_status,
            "is_regression": self.is_regression,
        }


class BaselineStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def save(self, metrics: List[PipelineMetric]) -> None:
        data = [_metric_to_dict(m) for m in metrics]
        os.makedirs(os.path.dirname(self.path) if os.path.dirname(self.path) else ".", exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(data, f)

    def load(self) -> List[PipelineMetric]:
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [_dict_to_metric(d) for d in json.load(f)]

    def check(self, current: List[PipelineMetric]) -> List[BaselineViolation]:
        baseline = {m.pipeline_name: m for m in self.load()}
        violations: List[BaselineViolation] = []
        for metric in current:
            base = baseline.get(metric.pipeline_name)
            if base is None:
                continue
            from pipewatch.metrics import failure_rate
            v = BaselineViolation(
                pipeline=metric.pipeline_name,
                baseline_failure_rate=failure_rate(base),
                current_failure_rate=failure_rate(metric),
                baseline_status=base.status,
                current_status=metric.status,
            )
            violations.append(v)
        return violations
