"""Diff consecutive pipeline metrics snapshots to detect changes."""
from dataclasses import dataclass
from typing import Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class MetricDiff:
    pipeline: str
    prev_failure_rate: float
    curr_failure_rate: float
    prev_status: str
    curr_status: str
    status_changed: bool
    failure_rate_delta: float

    def is_degraded(self) -> bool:
        return self.failure_rate_delta > 0 or (
            self.status_changed and self.curr_status != "ok"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "prev_failure_rate": round(self.prev_failure_rate, 4),
            "curr_failure_rate": round(self.curr_failure_rate, 4),
            "failure_rate_delta": round(self.failure_rate_delta, 4),
            "prev_status": self.prev_status,
            "curr_status": self.curr_status,
            "status_changed": self.status_changed,
            "degraded": self.is_degraded(),
        }


def diff_metrics(prev: PipelineMetric, curr: PipelineMetric) -> MetricDiff:
    from pipewatch.metrics import failure_rate
    prev_fr = failure_rate(prev)
    curr_fr = failure_rate(curr)
    return MetricDiff(
        pipeline=curr.pipeline,
        prev_failure_rate=prev_fr,
        curr_failure_rate=curr_fr,
        prev_status=prev.status,
        curr_status=curr.status,
        status_changed=prev.status != curr.status,
        failure_rate_delta=curr_fr - prev_fr,
    )


def diff_all(prev_metrics: list, curr_metrics: list) -> list:
    prev_map = {m.pipeline: m for m in prev_metrics}
    curr_map = {m.pipeline: m for m in curr_metrics}
    results = []
    for name, curr in curr_map.items():
        if name in prev_map:
            results.append(diff_metrics(prev_map[name], curr))
    return results
