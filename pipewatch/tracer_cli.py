"""CLI entry point for pipeline tracer."""
import argparse
import json
import time
from pipewatch.metrics import PipelineMetric
from pipewatch.tracer import trace_all, format_trace


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Trace pipeline state transitions")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--transitions-only", action="store_true",
                   help="Only show events with a state transition")
    return p


def _demo_metrics() -> list:
    now = time.time()
    return [
        PipelineMetric("sales", now - 120, "ok", 1000, 5, 1.2),
        PipelineMetric("sales", now - 60, "warning", 1000, 60, 1.5),
        PipelineMetric("sales", now, "ok", 1000, 10, 1.1),
        PipelineMetric("inventory", now - 90, "ok", 500, 2, 0.9),
        PipelineMetric("inventory", now - 30, "error", 500, 200, 0.0),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = _demo_metrics()
    traces = trace_all(metrics)

    if args.pipeline:
        traces = {k: v for k, v in traces.items() if k == args.pipeline}

    if args.transitions_only:
        traces = {k: [e for e in v if e.transition] for k, v in traces.items()}

    if args.json:
        out = {name: [e.to_dict() for e in evts] for name, evts in traces.items()}
        print(json.dumps(out, indent=2))
    else:
        for name, events in traces.items():
            print(f"Pipeline: {name}")
            print(format_trace(events))
            print()


if __name__ == "__main__":
    main()
