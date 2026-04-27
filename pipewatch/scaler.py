"""scaler.py — Normalize pipeline metrics to a common scale for cross-pipeline comparison.

Provides min-max and z-score scaling of failure rates and record counts
across a collection of pipeline metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from .metrics import PipelineMetric


@dataclass
class ScaledMetric:
    """A pipeline metric with additional scaled fields."""

    pipeline: str
    original_failure_rate: float
    original_total_records: int
    scaled_failure_rate: float  # 0.0–1.0 (min-max) or z-score
    scaled_total_records: float
    method: str  # "minmax" or "zscore"

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "original_failure_rate": round(self.original_failure_rate, 6),
            "original_total_records": self.original_total_records,
            "scaled_failure_rate": round(self.scaled_failure_rate, 6),
            "scaled_total_records": round(self.scaled_total_records, 6),
            "method": self.method,
        }


@dataclass
class ScalerResult:
    """Collection of scaled metrics with metadata."""

    method: str
    scaled: List[ScaledMetric] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "method": self.method,
            "count": len(self.scaled),
            "scaled": [s.to_dict() for s in self.scaled],
        }


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records <= 0:
        return 0.0
    return m.failed_records / m.total_records


def _minmax(values: List[float]) -> List[float]:
    """Min-max scale a list of floats to [0, 1]."""
    lo, hi = min(values), max(values)
    span = hi - lo
    if span == 0.0:
        return [0.0] * len(values)
    return [(v - lo) / span for v in values]


def _zscore(values: List[float]) -> List[float]:
    """Z-score standardise a list of floats (mean=0, std=1)."""
    n = len(values)
    if n == 0:
        return []
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = variance ** 0.5
    if std == 0.0:
        return [0.0] * n
    return [(v - mean) / std for v in values]


def scale_metrics(
    metrics: Sequence[PipelineMetric],
    method: str = "minmax",
) -> Optional[ScalerResult]:
    """Scale a collection of pipeline metrics.

    Args:
        metrics: Sequence of PipelineMetric objects.
        method:  ``"minmax"`` (default) or ``"zscore"``.

    Returns:
        A :class:`ScalerResult`, or ``None`` if *metrics* is empty.
    """
    if not metrics:
        return None

    if method not in ("minmax", "zscore"):
        raise ValueError(f"Unknown scaling method: {method!r}. Use 'minmax' or 'zscore'.")

    scale_fn = _minmax if method == "minmax" else _zscore

    failure_rates = [_failure_rate(m) for m in metrics]
    total_records = [float(m.total_records) for m in metrics]

    scaled_fr = scale_fn(failure_rates)
    scaled_tr = scale_fn(total_records)

    result = ScalerResult(method=method)
    for m, sfr, str_ in zip(metrics, scaled_fr, scaled_tr):
        result.scaled.append(
            ScaledMetric(
                pipeline=m.pipeline,
                original_failure_rate=_failure_rate(m),
                original_total_records=m.total_records,
                scaled_failure_rate=sfr,
                scaled_total_records=str_,
                method=method,
            )
        )

    return result


def format_scaled(result: ScalerResult) -> str:
    """Return a human-readable table of scaled metrics."""
    lines = [f"Scaler method: {result.method}", ""]
    header = f"  {'Pipeline':<30}  {'Orig FR':>8}  {'Scaled FR':>10}  {'Orig Total':>10}  {'Scaled Total':>12}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for s in result.scaled:
        lines.append(
            f"  {s.pipeline:<30}  {s.original_failure_rate:>8.4f}  "
            f"{s.scaled_failure_rate:>10.4f}  {s.original_total_records:>10}  "
            f"{s.scaled_total_records:>12.4f}"
        )
    return "\n".join(lines)
