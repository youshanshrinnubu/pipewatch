"""Export pipeline metrics and summaries to various formats."""

from __future__ import annotations

import csv
import io
import json
from typing import List

from pipewatch.metrics import PipelineMetric, to_dict
from pipewatch.summary import PipelineSummary, summarize_all


def export_metrics_json(metrics: List[PipelineMetric], indent: int = 2) -> str:
    """Serialize a list of metrics to a JSON string."""
    return json.dumps([to_dict(m) for m in metrics], indent=indent, default=str)


def export_metrics_csv(metrics: List[PipelineMetric]) -> str:
    """Serialize a list of metrics to a CSV string."""
    if not metrics:
        return ""
    buf = io.StringIO()
    fieldnames = list(to_dict(metrics[0]).keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for m in metrics:
        writer.writerow(to_dict(m))
    return buf.getvalue()


def export_summary_json(metrics: List[PipelineMetric], indent: int = 2) -> str:
    """Summarize metrics per pipeline and serialize to JSON."""
    summaries: List[PipelineSummary] = summarize_all(metrics)
    rows = [
        {
            "pipeline": s.pipeline_name,
            "total_runs": s.total_runs,
            "avg_failure_rate": round(s.avg_failure_rate, 4),
            "last_status": s.last_status,
            "healthy_runs": s.healthy_runs,
            "unhealthy_runs": s.unhealthy_runs,
        }
        for s in summaries
    ]
    return json.dumps(rows, indent=indent)


def write_export(content: str, path: str) -> None:
    """Write exported content to a file."""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
