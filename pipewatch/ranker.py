"""Rank pipelines by health score or failure rate."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, failure_rate
from pipewatch.scorer import score_metric


@dataclass
class RankedPipeline:
    rank: int
    pipeline: str
    score: float
    failure_rate: float
    status: str

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "pipeline": self.pipeline,
            "score": self.score,
            "failure_rate": self.failure_rate,
            "status": self.status,
        }


def rank_by_score(
    metrics: List[PipelineMetric],
    ascending: bool = True,
) -> List[RankedPipeline]:
    """Rank pipelines from worst (lowest score) to best by default."""
    if not metrics:
        return []

    scored = [
        (m, score_metric(m).score)
        for m in metrics
    ]
    scored.sort(key=lambda t: t[1], reverse=not ascending)

    return [
        RankedPipeline(
            rank=i + 1,
            pipeline=m.pipeline,
            score=s,
            failure_rate=failure_rate(m),
            status=m.status,
        )
        for i, (m, s) in enumerate(scored)
    ]


def rank_by_failure_rate(
    metrics: List[PipelineMetric],
    ascending: bool = False,
) -> List[RankedPipeline]:
    """Rank pipelines from highest failure rate to lowest by default."""
    if not metrics:
        return []

    pairs = [(m, failure_rate(m)) for m in metrics]
    pairs.sort(key=lambda t: t[1], reverse=ascending is False)

    return [
        RankedPipeline(
            rank=i + 1,
            pipeline=m.pipeline,
            score=score_metric(m).score,
            failure_rate=fr,
            status=m.status,
        )
        for i, (m, fr) in enumerate(pairs)
    ]


def format_ranked(ranked: List[RankedPipeline]) -> str:
    if not ranked:
        return "No pipelines to rank."
    lines = [f"{'Rank':<6} {'Pipeline':<24} {'Score':>6} {'FailRate':>9} {'Status'}",
             "-" * 60]
    for r in ranked:
        lines.append(
            f"{r.rank:<6} {r.pipeline:<24} {r.score:>6.1f} {r.failure_rate:>8.1%} {r.status}"
        )
    return "\n".join(lines)
