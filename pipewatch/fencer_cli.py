"""fencer_cli.py — CLI entry point for the circuit-breaker fencer."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from pipewatch.fencer import Fencer, FenceConfig
from pipewatch.metrics import PipelineMetric


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-fencer",
        description="Simulate circuit-breaker fencing over a sequence of metrics.",
    )
    p.add_argument("--trip-threshold", type=float, default=0.3,
                   help="Failure rate that counts as a bad check (default: 0.3)")
    p.add_argument("--trip-count", type=int, default=3,
                   help="Consecutive bad checks before fence trips open (default: 3)")
    p.add_argument("--reset-count", type=int, default=2,
                   help="Consecutive good checks to close fence (default: 2)")
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="Output as JSON")
    return p


def _demo_metrics() -> list:
    now = datetime.now(timezone.utc).isoformat()
    base = dict(status="ok", timestamp=now, duration_seconds=1.0, extra={})
    rows = [
        ("sales", 1000, 50),
        ("sales", 1000, 320),
        ("sales", 1000, 350),
        ("sales", 1000, 310),   # trip expected here
        ("sales", 1000, 10),
        ("sales", 1000, 5),     # reset expected here
        ("inventory", 500, 0),
    ]
    return [
        PipelineMetric(
            pipeline_name=p, total_records=t, failed_records=f, **base
        )
        for p, t, f in rows
    ]


def main(argv=None) -> None:
    args = _build_parser().parse_args(argv)
    cfg = FenceConfig(
        trip_threshold=args.trip_threshold,
        trip_count=args.trip_count,
        reset_count=args.reset_count,
    )
    fencer = Fencer(cfg)
    metrics = _demo_metrics()
    results = fencer.check_all(metrics)

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
        return

    for i, r in enumerate(results):
        m = metrics[i]
        rate = (
            m.failed_records / m.total_records if m.total_records > 0 else 0.0
        )
        flag = ""
        if r.tripped:
            flag = "  [TRIPPED]"
        elif r.reset:
            flag = "  [RESET]"
        status = "OPEN" if r.state.open else "closed"
        print(
            f"{m.pipeline_name:<20} rate={rate:.2%}  fence={status}"
            f"  bad={r.state.consecutive_bad}  good={r.state.consecutive_good}{flag}"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
