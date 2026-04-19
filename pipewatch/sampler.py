"""Metric sampler: periodically sample a subset of metrics for lightweight monitoring."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import random
from pipewatch.metrics import PipelineMetric


@dataclass
class SampleResult:
    pipeline: str
    sampled: List[PipelineMetric]
    total: int
    sample_size: int
    rate: float  # fraction sampled

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total": self.total,
            "sample_size": self.sample_size,
            "rate": self.rate,
            "sampled": [m.to_dict() for m in self.sampled],
        }


def sample_pipeline(
    metrics: List[PipelineMetric],
    pipeline: str,
    n: int = 5,
    seed: Optional[int] = None,
) -> Optional[SampleResult]:
    """Return up to n randomly sampled metrics for a given pipeline."""
    subset = [m for m in metrics if m.pipeline == pipeline]
    if not subset:
        return None
    rng = random.Random(seed)
    k = min(n, len(subset))
    sampled = rng.sample(subset, k)
    return SampleResult(
        pipeline=pipeline,
        sampled=sampled,
        total=len(subset),
        sample_size=k,
        rate=round(k / len(subset), 4),
    )


def sample_all(
    metrics: List[PipelineMetric],
    n: int = 5,
    seed: Optional[int] = None,
) -> List[SampleResult]:
    """Sample metrics for every distinct pipeline."""
    pipelines = sorted({m.pipeline for m in metrics})
    results = []
    for p in pipelines:
        r = sample_pipeline(metrics, p, n=n, seed=seed)
        if r:
            results.append(r)
    return results


def format_sampled(results: List[SampleResult]) -> str:
    lines = []
    for r in results:
        lines.append(f"[{r.pipeline}] sampled {r.sample_size}/{r.total} ({r.rate*100:.1f}%)")
        for m in r.sampled:
            lines.append(f"  ts={m.timestamp} status={m.status} failures={m.failed_records}")
    return "\n".join(lines) if lines else "No samples."
