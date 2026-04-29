"""zipper.py — Pair metrics from two snapshots by pipeline name for side-by-side analysis."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pipewatch.metrics import PipelineMetric


@dataclass
class ZippedPair:
    pipeline: str
    left: Optional[PipelineMetric]
    right: Optional[PipelineMetric]

    def both_present(self) -> bool:
        return self.left is not None and self.right is not None

    def only_left(self) -> bool:
        return self.left is not None and self.right is None

    def only_right(self) -> bool:
        return self.left is None and self.right is not None

    def to_dict(self) -> dict:
        def _m(m: Optional[PipelineMetric]) -> Optional[dict]:
            if m is None:
                return None
            return {
                "pipeline": m.pipeline_name,
                "status": m.status,
                "total_records": m.total_records,
                "failed_records": m.failed_records,
            }

        return {
            "pipeline": self.pipeline,
            "left": _m(self.left),
            "right": _m(self.right),
            "both_present": self.both_present(),
            "only_left": self.only_left(),
            "only_right": self.only_right(),
        }


def zip_metrics(
    left: List[PipelineMetric],
    right: List[PipelineMetric],
) -> List[ZippedPair]:
    """Pair metrics from two lists by pipeline name. Unmatched entries are included with None."""
    left_map: Dict[str, PipelineMetric] = {m.pipeline_name: m for m in left}
    right_map: Dict[str, PipelineMetric] = {m.pipeline_name: m for m in right}
    all_keys = sorted(set(left_map) | set(right_map))
    return [
        ZippedPair(
            pipeline=key,
            left=left_map.get(key),
            right=right_map.get(key),
        )
        for key in all_keys
    ]


def format_zipped(pairs: List[ZippedPair]) -> str:
    """Return a human-readable side-by-side comparison."""
    if not pairs:
        return "No pipelines to compare."

    lines: List[str] = [f"{'PIPELINE':<24} {'LEFT STATUS':<14} {'RIGHT STATUS':<14} NOTE"]
    lines.append("-" * 70)
    for p in pairs:
        left_status = p.left.status if p.left else "(missing)"
        right_status = p.right.status if p.right else "(missing)"
        note = ""
        if p.only_left():
            note = "only in left"
        elif p.only_right():
            note = "only in right"
        elif p.left and p.right and p.left.status != p.right.status:
            note = "status changed"
        lines.append(f"{p.pipeline:<24} {left_status:<14} {right_status:<14} {note}")
    return "\n".join(lines)
