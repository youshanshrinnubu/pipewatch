"""CLI entry-point for the metric streamer."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.metrics import PipelineMetric, MetricsCollector
from pipewatch.alerts import AlertManager
from pipewatch.handlers import stdout_handler
from pipewatch.streamer import MetricStreamer, StreamConfig


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Stream pipeline metric events")
    p.add_argument(
        "--max-events", type=int, default=None, help="Stop after N events"
    )
    p.add_argument(
        "--pipeline", action="append", dest="pipelines", help="Filter by pipeline name"
    )
    p.add_argument(
        "--json", action="store_true", help="Output events as JSON lines"
    )
    p.add_argument(
        "--failure-threshold",
        type=float,
        default=0.1,
        help="Failure rate threshold for warnings (default 0.10)",
    )
    return p


def _demo_metrics() -> List[PipelineMetric]:
    """Return a small fixed set of demo metrics for CLI testing."""
    from pipewatch.metrics import PipelineMetric
    import time

    now = time.time()
    return [
        PipelineMetric("sales", "ok", 1000, 20, now),
        PipelineMetric("inventory", "ok", 500, 80, now),
        PipelineMetric("orders", "error", 200, 200, now),
    ]


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    am = AlertManager()
    am.register_handler(stdout_handler)

    cfg = StreamConfig(
        max_events=args.max_events,
        pipelines=args.pipelines,
    )
    streamer = MetricStreamer(am, cfg)

    for event in streamer.stream(_demo_metrics):
        if args.json:
            print(json.dumps(event.to_dict()))
        else:
            alert_summary = (
                ", ".join(a.severity.upper() for a in event.alerts)
                if event.alerts
                else "none"
            )
            print(
                f"[{event.sequence}] {event.metric.pipeline_name} "
                f"status={event.metric.status} alerts={alert_summary}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
