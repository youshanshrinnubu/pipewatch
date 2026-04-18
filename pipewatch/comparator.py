"""Compare pipeline metrics across two snapshots and produce a comparison report."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric
from pipewatch.metrics import failure_rate


@dataclass
class ComparisonResult:
    pipeline: str
    before: Optional[PipelineMetric]
    after: Optional[PipelineMetric]
    failure_rate_delta: float = 0.0
    status_changed: bool = False
    only_in_before: bool = False
    only_in_after: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "failure_rate_delta": round(self.failure_rate_delta, 4),
            "status_changed": self.status_changed,
            "only_in_before": self.only_in_before,
            "only_in_after": self.only_in_after,
            "before_status": self.before.status if self.before else None,
            "after_status": self.after.status if self.after else None,
        }


def compare_snapshots(
    before: List[PipelineMetric],
    after: List[PipelineMetric],
) -> List[ComparisonResult]:
    before_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in before}
    after_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in after}
    all_keys = set(before_map) | set(after_map)
    results = []
    for key in sorted(all_keys):
        b = before_map.get(key)
        a = after_map.get(key)
        if b is None:
            results.append(ComparisonResult(pipeline=key, before=None, after=a, only_in_after=True))
        elif a is None:
            results.append(ComparisonResult(pipeline=key, before=b, after=None, only_in_before=True))
        else:
            delta = failure_rate(a) - failure_rate(b)
            changed = a.status != b.status
            results.append(ComparisonResult(
                pipeline=key, before=b, after=a,
                failure_rate_delta=delta,
                status_changed=changed,
            ))
    return results


def format_comparison(results: List[ComparisonResult]) -> str:
    if not results:
        return "No pipelines to compare."
    lines = ["Pipeline Comparison Report", "=" * 40]
    for r in results:
        if r.only_in_before:
            lines.append(f"  {r.pipeline}: removed (was {r.before.status})")
        elif r.only_in_after:
            lines.append(f"  {r.pipeline}: new (status={r.after.status})")
        else:
            delta_str = f"{r.failure_rate_delta:+.2%}"
            status_note = f" [status: {r.before.status}->{r.after.status}]" if r.status_changed else ""
            lines.append(f"  {r.pipeline}: failure_rate {delta_str}{status_note}")
    return "\n".join(lines)
