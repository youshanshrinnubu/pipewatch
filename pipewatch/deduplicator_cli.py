"""CLI demo for the Deduplicator."""
from __future__ import annotations
import argparse
import json
from pipewatch.alerts import Alert
from pipewatch.deduplicator import Deduplicator, DeduplicatorConfig


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Demo alert deduplication")
    p.add_argument("--window", type=float, default=60.0, help="Dedup window in seconds")
    p.add_argument("--json", action="store_true", dest="as_json")
    return p


def _demo_alerts() -> list:
    return [
        Alert(pipeline="etl_main", severity="warning", message="High failure rate"),
        Alert(pipeline="etl_main", severity="warning", message="High failure rate"),
        Alert(pipeline="etl_main", severity="critical", message="Pipeline error"),
        Alert(pipeline="etl_secondary", severity="warning", message="High failure rate"),
        Alert(pipeline="etl_main", severity="warning", message="High failure rate"),
    ]


def main(argv=None) -> None:
    args = _build_parser().parse_args(argv)
    config = DeduplicatorConfig(window_seconds=args.window)
    dedup = Deduplicator(config)
    alerts = _demo_alerts()
    passed = dedup.filter(alerts)
    suppressed = len(alerts) - len(passed)

    if args.as_json:
        out = {
            "total": len(alerts),
            "passed": len(passed),
            "suppressed": suppressed,
            "alerts": [{"pipeline": a.pipeline, "severity": a.severity, "message": a.message} for a in passed],
        }
        print(json.dumps(out, indent=2))
    else:
        print(f"Total alerts   : {len(alerts)}")
        print(f"Passed         : {len(passed)}")
        print(f"Suppressed     : {suppressed}")
        for a in passed:
            print(f"  [{a.severity}] {a.pipeline}: {a.message}")


if __name__ == "__main__":
    main()
