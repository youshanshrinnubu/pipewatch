"""Leveler: assign severity levels to metrics based on configurable thresholds."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class LevelConfig:
    warning_failure_rate: float = 0.05
    critical_failure_rate: float = 0.20
    warning_statuses: List[str] = field(default_factory=lambda: ["warning"])
    critical_statuses: List[str] = field(default_factory=lambda: ["error", "critical"])


@dataclass
class LevelResult:
    pipeline: str
    level: str  # "ok", "warning", "critical"
    reason: str
    metric: PipelineMetric

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "level": self.level,
            "reason": self.reason,
        }


def _failure_rate(metric: PipelineMetric) -> float:
    if metric.total_records == 0:
        return 0.0
    return metric.failed_records / metric.total_records


def level_metric(metric: PipelineMetric, config: Optional[LevelConfig] = None) -> LevelResult:
    cfg = config or LevelConfig()
    fr = _failure_rate(metric)

    if metric.status in cfg.critical_statuses:
        return LevelResult(metric.pipeline, "critical", f"status={metric.status}", metric)
    if fr >= cfg.critical_failure_rate:
        return LevelResult(metric.pipeline, "critical", f"failure_rate={fr:.2%}", metric)
    if metric.status in cfg.warning_statuses:
        return LevelResult(metric.pipeline, "warning", f"status={metric.status}", metric)
    if fr >= cfg.warning_failure_rate:
        return LevelResult(metric.pipeline, "warning", f"failure_rate={fr:.2%}", metric)
    return LevelResult(metric.pipeline, "ok", "within thresholds", metric)


def level_all(
    metrics: List[PipelineMetric],
    config: Optional[LevelConfig] = None,
) -> List[LevelResult]:
    return [level_metric(m, config) for m in metrics]


def format_leveled(results: List[LevelResult]) -> str:
    if not results:
        return "No metrics to level."
    lines = []
    for r in results:
        lines.append(f"[{r.level.upper():8s}] {r.pipeline}: {r.reason}")
    return "\n".join(lines)
