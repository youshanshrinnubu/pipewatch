"""CLI for labeling pipeline metrics."""
import argparse
import json
import sys
from pipewatch.labeler import Labeler, LabelRule
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Label pipeline metrics by health rules")
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--label", help="Filter output to metrics with this label")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    metrics = [
        PipelineMetric("ingest", total_records=1000, failed_records=10, status="ok"),
        PipelineMetric("transform", total_records=500, failed_records=150, status="ok"),
        PipelineMetric("export", total_records=200, failed_records=5, status="error"),
    ]
    labeler = Labeler()
    labeled = labeler.label_all(metrics)

    if args.label:
        labeled = [lm for lm in labeled if lm.has_label(args.label)]

    if not labeled:
        print("No metrics matched.")
        return

    if args.format == "json":
        print(json.dumps([lm.to_dict() for lm in labeled], indent=2))
    else:
        for lm in labeled:
            tags = ", ".join(lm.labels) if lm.labels else "(none)"
            print(f"{lm.metric.pipeline_name}: [{tags}] failure_rate={lm.metric.failure_rate:.2%} status={lm.metric.status}")


if __name__ == "__main__":
    main()
