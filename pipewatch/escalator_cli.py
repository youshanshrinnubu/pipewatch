"""CLI demo for the alert escalator."""
from __future__ import annotations

import argparse
import json
from typing import List

from pipewatch.alerts import Alert
from pipewatch.escalator import Escalator, EscalationConfig
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Simulate alert escalation")
    p.add_argument("--repeats", type=int, default=4,
                   help="Number of times to fire the same warning alert")
    p.add_argument("--threshold", type=int, default=3,
                   help="Repeats required to escalate to critical")
    p.add_argument("--window", type=float, default=300.0,
                   help="Rolling window in seconds")
    p.add_argument("--json", action="store_true", dest="as_json")
    return p


def main(argv: List[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    config = EscalationConfig(
        warn_to_critical_after=args.threshold,
        window_seconds=args.window,
    )
    escalator = Escalator(config)

    metric = PipelineMetric(
        pipeline="demo", status="ok", total_records=100,
        failed_records=5, duration_seconds=1.0,
    )
    base_alert = Alert(pipeline="demo", severity="warning",
                       message="High failure rate", metric=metric)

    results = []
    for i in range(args.repeats):
        out = escalator.process(base_alert)
        results.append({"attempt": i + 1, "severity": out.severity,
                        "message": out.message})

    if args.as_json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            print(f"[{r['attempt']}] severity={r['severity']}  {r['message']}")


if __name__ == "__main__":
    main()
