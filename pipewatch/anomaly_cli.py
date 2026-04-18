from __future__ import annotations
import argparse
import json
import sys
from pipewatch.metrics import PipelineMetric
from pipewatch.anomaly import detect_anomalies


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Detect anomalies in pipeline metrics")
    p.add_argument("--warning", type=float, default=0.1, help="Failure rate warning threshold")
    p.add_argument("--critical", type=float, default=0.3, help="Failure rate critical threshold")
    p.add_argument("--spike", type=float, default=3.0, help="Spike multiplier vs historical average")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    p.add_argument("--severity", choices=["warning", "critical"], help="Filter by severity")
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    metrics = [
        PipelineMetric("ingest", total_records=1000, failed_records=350, status="ok"),
        PipelineMetric("transform", total_records=500, failed_records=60, status="ok"),
        PipelineMetric("export", total_records=200, failed_records=0, status="error"),
    ]

    results = detect_anomalies(
        metrics,
        failure_rate_warning=args.warning,
        failure_rate_critical=args.critical,
        spike_multiplier=args.spike,
    )

    if args.severity:
        results = [r for r in results if r.severity == args.severity]

    if not results:
        print("No anomalies detected.")
        return

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            print(f"[{r.severity.upper()}] {r.pipeline}: {r.reason}")


if __name__ == "__main__":
    main()
