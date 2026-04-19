"""Patch metrics by overriding fields for testing or manual correction."""
from dataclasses import dataclass, replace
from typing import Optional, List
from pipewatch.metrics import PipelineMetric


@dataclass
class PatchRule:
    pipeline: Optional[str]  # None = match all
    set_status: Optional[str] = None
    set_failure_rate: Optional[float] = None
    note: str = ""

    def matches(self, metric: PipelineMetric) -> bool:
        return self.pipeline is None or metric.pipeline == self.pipeline

    def apply(self, metric: PipelineMetric) -> PipelineMetric:
        kwargs = {}
        if self.set_status is not None:
            kwargs["status"] = self.set_status
        if self.set_failure_rate is not None:
            total = metric.total_records or 1
            kwargs["failed_records"] = int(self.set_failure_rate * total)
        return replace(metric, **kwargs) if kwargs else metric


@dataclass
class PatchResult:
    original: PipelineMetric
    patched: PipelineMetric
    rules_applied: int

    @property
    def changed(self) -> bool:
        return self.original != self.patched

    def to_dict(self) -> dict:
        from pipewatch.metrics import to_dict as md
        return {
            "pipeline": self.original.pipeline,
            "changed": self.changed,
            "rules_applied": self.rules_applied,
            "original_status": self.original.status,
            "patched_status": self.patched.status,
        }


def patch_metric(metric: PipelineMetric, rules: List[PatchRule]) -> PatchResult:
    current = metric
    count = 0
    for rule in rules:
        if rule.matches(current):
            current = rule.apply(current)
            count += 1
    return PatchResult(original=metric, patched=current, rules_applied=count)


def patch_all(metrics: List[PipelineMetric], rules: List[PatchRule]) -> List[PatchResult]:
    return [patch_metric(m, rules) for m in metrics]


def format_patch_results(results: List[PatchResult]) -> str:
    lines = []
    for r in results:
        tag = "CHANGED" if r.changed else "unchanged"
        lines.append(f"  [{tag}] {r.original.pipeline}: status={r.patched.status} rules={r.rules_applied}")
    return "Patch Results:\n" + "\n".join(lines) if lines else "No metrics patched."
