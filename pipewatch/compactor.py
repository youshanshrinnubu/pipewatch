"""compactor.py — Compact a snapshot store by merging duplicate pipeline entries.

Keeps only the most recent metric per pipeline, reducing store size.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CompactResult:
    pipeline: str
    before_count: int
    after_count: int
    kept: PipelineMetric

    @property
    def removed(self) -> int:
        return self.before_count - self.after_count

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "before_count": self.before_count,
            "after_count": self.after_count,
            "removed": self.removed,
            "kept_timestamp": self.kept.timestamp,
        }


def compact(
    metrics: List[PipelineMetric],
) -> tuple[List[PipelineMetric], List[CompactResult]]:
    """Return deduplicated metrics (one per pipeline, most recent) and a result list."""
    grouped: Dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        grouped.setdefault(m.pipeline_name, []).append(m)

    compacted: List[PipelineMetric] = []
    results: List[CompactResult] = []

    for pipeline, entries in grouped.items():
        sorted_entries = sorted(entries, key=lambda m: m.timestamp, reverse=True)
        kept = sorted_entries[0]
        compacted.append(kept)
        results.append(
            CompactResult(
                pipeline=pipeline,
                before_count=len(entries),
                after_count=1,
                kept=kept,
            )
        )

    return compacted, results


def format_compacted(results: List[CompactResult]) -> str:
    if not results:
        return "No pipelines to compact."
    lines = ["Compaction Results:", "-" * 40]
    for r in sorted(results, key=lambda x: x.pipeline):
        lines.append(
            f"  {r.pipeline}: {r.before_count} -> {r.after_count} "
            f"(removed {r.removed})"
        )
    lines.append("-" * 40)
    total_removed = sum(r.removed for r in results)
    lines.append(f"Total removed: {total_removed}")
    return "\n".join(lines)
