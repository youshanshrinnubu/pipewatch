"""Pipeline profiler: tracks runtime duration and record throughput stats."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class ProfileResult:
    pipeline: str
    sample_count: int
    avg_duration_s: float
    min_duration_s: float
    max_duration_s: float
    avg_records: float
    throughput_per_s: float  # avg_records / avg_duration_s
    grade: str  # fast / moderate / slow

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "avg_duration_s": round(self.avg_duration_s, 3),
            "min_duration_s": round(self.min_duration_s, 3),
            "max_duration_s": round(self.max_duration_s, 3),
            "avg_records": round(self.avg_records, 1),
            "throughput_per_s": round(self.throughput_per_s, 2),
            "grade": self.grade,
        }


def _grade(throughput: float) -> str:
    if throughput >= 1000:
        return "fast"
    if throughput >= 100:
        return "moderate"
    return "slow"


def profile_pipeline(metrics: List[PipelineMetric]) -> Optional[ProfileResult]:
    """Compute profiling stats for a single pipeline's metric history."""
    metrics = [m for m in metrics if m.duration_s is not None and m.duration_s > 0]
    if not metrics:
        return None
    pipeline = metrics[0].pipeline
    durations = [m.duration_s for m in metrics]
    records = [m.records_processed for m in metrics]
    avg_dur = sum(durations) / len(durations)
    avg_rec = sum(records) / len(records)
    throughput = avg_rec / avg_dur if avg_dur > 0 else 0.0
    return ProfileResult(
        pipeline=pipeline,
        sample_count=len(metrics),
        avg_duration_s=avg_dur,
        min_duration_s=min(durations),
        max_duration_s=max(durations),
        avg_records=avg_rec,
        throughput_per_s=throughput,
        grade=_grade(throughput),
    )


def profile_all(metrics: List[PipelineMetric]) -> List[ProfileResult]:
    """Profile all pipelines found in the metric list."""
    by_pipeline: dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        by_pipeline.setdefault(m.pipeline, []).append(m)
    results = []
    for pipe_metrics in by_pipeline.values():
        r = profile_pipeline(pipe_metrics)
        if r:
            results.append(r)
    results.sort(key=lambda r: r.throughput_per_s)
    return results


def format_profiled(results: List[ProfileResult]) -> str:
    if not results:
        return "No profiling data available."
    lines = ["Pipeline Profiler Report", "=" * 40]
    for r in results:
        lines.append(
            f"[{r.grade.upper()}] {r.pipeline}: "
            f"{r.throughput_per_s:.1f} rec/s "
            f"(avg {r.avg_duration_s:.2f}s, n={r.sample_count})"
        )
    return "\n".join(lines)
