"""partitioner.py — Partition pipeline metrics into time-based buckets.

Useful for analysing how a pipeline behaves across discrete time windows
(e.g. hourly, daily) without the rolling-window semantics of windower.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class Partition:
    """A single time bucket containing a slice of metrics."""

    bucket_start: datetime
    bucket_end: datetime
    pipeline: str
    metrics: List[PipelineMetric] = field(default_factory=list)

    # Derived stats — populated by partition()
    total_records: int = 0
    failed_records: int = 0
    failure_rate: float = 0.0
    dominant_status: str = "ok"
    count: int = 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "bucket_start": self.bucket_start.isoformat(),
            "bucket_end": self.bucket_end.isoformat(),
            "count": self.count,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 4),
            "dominant_status": self.dominant_status,
        }


def _bucket_key(ts: datetime, interval_seconds: int) -> int:
    """Return the epoch-second floor of *ts* aligned to *interval_seconds*."""
    epoch = int(ts.replace(tzinfo=timezone.utc).timestamp())
    return (epoch // interval_seconds) * interval_seconds


def _dominant_status(metrics: List[PipelineMetric]) -> str:
    """Return the most severe status seen across *metrics*."""
    priority = {"error": 3, "critical": 2, "warning": 1, "ok": 0}
    worst = "ok"
    for m in metrics:
        if priority.get(m.status, 0) > priority.get(worst, 0):
            worst = m.status
    return worst


def partition(
    metrics: List[PipelineMetric],
    interval_seconds: int = 3600,
    pipeline: Optional[str] = None,
) -> List[Partition]:
    """Partition *metrics* into fixed-size time buckets.

    Args:
        metrics: Raw pipeline metrics to partition.
        interval_seconds: Bucket width in seconds (default 3600 = 1 hour).
        pipeline: If given, only metrics for this pipeline are included.

    Returns:
        A list of :class:`Partition` objects sorted by bucket start time.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")

    filtered = [
        m for m in metrics
        if (pipeline is None or m.pipeline == pipeline) and m.timestamp is not None
    ]

    # Group by (pipeline, bucket_key)
    buckets: Dict[tuple, List[PipelineMetric]] = {}
    for m in filtered:
        key = (_bucket_key(m.timestamp, interval_seconds), m.pipeline)
        buckets.setdefault(key, []).append(m)

    partitions: List[Partition] = []
    for (bucket_ts, pipe), members in buckets.items():
        start = datetime.utcfromtimestamp(bucket_ts).replace(tzinfo=timezone.utc)
        end = datetime.utcfromtimestamp(bucket_ts + interval_seconds).replace(
            tzinfo=timezone.utc
        )
        total = sum(m.total_records for m in members)
        failed = sum(m.failed_records for m in members)
        rate = failed / total if total > 0 else 0.0

        partitions.append(
            Partition(
                bucket_start=start,
                bucket_end=end,
                pipeline=pipe,
                metrics=members,
                total_records=total,
                failed_records=failed,
                failure_rate=rate,
                dominant_status=_dominant_status(members),
                count=len(members),
            )
        )

    partitions.sort(key=lambda p: (p.pipeline, p.bucket_start))
    return partitions


def partition_all(
    metrics: List[PipelineMetric],
    interval_seconds: int = 3600,
) -> Dict[str, List[Partition]]:
    """Partition *metrics* for every pipeline found.

    Returns:
        A mapping of pipeline name → list of :class:`Partition`.
    """
    pipelines = {m.pipeline for m in metrics if m.timestamp is not None}
    return {
        pipe: partition(metrics, interval_seconds=interval_seconds, pipeline=pipe)
        for pipe in sorted(pipelines)
    }


def format_partitions(partitions: List[Partition]) -> str:
    """Return a human-readable table of partitions."""
    if not partitions:
        return "No partitions."

    lines = [
        f"{'Pipeline':<20} {'Bucket Start':<22} {'Count':>5} "
        f"{'Records':>8} {'Failures':>8} {'Rate':>7} {'Status':<10}",
        "-" * 85,
    ]
    for p in partitions:
        lines.append(
            f"{p.pipeline:<20} {p.bucket_start.strftime('%Y-%m-%dT%H:%M'):<22} "
            f"{p.count:>5} {p.total_records:>8} {p.failed_records:>8} "
            f"{p.failure_rate:>6.1%} {p.dominant_status:<10}"
        )
    return "\n".join(lines)
