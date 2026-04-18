"""Text report generation for pipeline health summaries."""

from datetime import datetime
from typing import List, Optional
from pipewatch.summary import PipelineSummary, summarize_all, format_summary
from pipewatch.snapshot import SnapshotStore
from pipewatch.metrics import PipelineMetric


def _header(title: str, width: int = 60) -> str:
    bar = "=" * width
    return f"{bar}\n{title.center(width)}\n{bar}"


def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def build_report(
    metrics: List[PipelineMetric],
    title: str = "PipeWatch Health Report",
    include_timestamp: bool = True,
) -> str:
    """Build a full text report from a list of metrics."""
    lines = [_header(title)]
    if include_timestamp:
        lines.append(f"Generated: {_timestamp()}")
    lines.append("")

    if not metrics:
        lines.append("  No pipeline metrics available.")
        return "\n".join(lines)

    summaries = summarize_all(metrics)
    for summary in summaries:
        lines.append(format_summary(summary))
        lines.append("")

    healthy = sum(1 for s in summaries if s.healthy)
    total = len(summaries)
    lines.append("-" * 60)
    lines.append(f"Pipelines healthy: {healthy}/{total}")
    return "\n".join(lines)


def build_report_from_store(
    store: SnapshotStore,
    pipeline_names: Optional[List[str]] = None,
    title: str = "PipeWatch Health Report",
) -> str:
    """Build a report by loading recent metrics from a SnapshotStore."""
    names = pipeline_names or store.list_pipelines()
    metrics: List[PipelineMetric] = []
    for name in names:
        recent = store.load(name)
        if recent:
            metrics.append(recent[-1])
    return build_report(metrics, title=title)


def print_report(metrics: List[PipelineMetric], title: str = "PipeWatch Health Report") -> None:
    """Convenience function to print a report to stdout."""
    print(build_report(metrics, title=title))
