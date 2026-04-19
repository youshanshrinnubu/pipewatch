"""CLI demo for the alert router."""

import argparse
import json
from pipewatch.router import Router
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric
from pipewatch.handlers import stdout_handler, stderr_handler


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Demo the pipewatch alert router")
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--severity", default=None, help="Only route alerts of this severity")
    return p


def _demo_alerts():
    return [
        Alert(pipeline="ingest", severity="warning", message="High failure rate"),
        Alert(pipeline="transform", severity="critical", message="Pipeline error"),
        Alert(pipeline="ingest", severity="critical", message="Total failure"),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    router = Router()
    router.add_rule(stderr_handler, severity="critical")
    router.add_rule(stdout_handler, severity="warning")
    router.set_fallback(stdout_handler)

    alerts = _demo_alerts()
    if args.severity:
        alerts = [a for a in alerts if a.severity == args.severity]

    results = []
    for alert in alerts:
        count = router.dispatch(alert)
        results.append({"pipeline": alert.pipeline, "severity": alert.severity, "handlers_called": count})

    if args.format == "json":
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            print(f"[{r['severity'].upper()}] {r['pipeline']} -> {r['handlers_called']} handler(s) called")


if __name__ == "__main__":
    main()
