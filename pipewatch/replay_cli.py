"""CLI for replaying historical metrics through alert evaluation."""
import argparse
import json
import sys
from pipewatch.snapshot import SnapshotStore
from pipewatch.replay import replay, replay_summary


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-replay",
        description="Replay stored metrics through alert evaluation.",
    )
    p.add_argument("snapshot", help="Path to snapshot JSON file")
    p.add_argument("--pipeline", help="Filter to a single pipeline name")
    p.add_argument("--warn", type=float, default=0.05, help="Warning threshold")
    p.add_argument("--critical", type=float, default=0.20, help="Critical threshold")
    p.add_argument("--summary", action="store_true", help="Print summary only")
    p.add_argument("--json", action="store_true", dest="as_json", help="JSON output")
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    store = SnapshotStore(args.snapshot)
    all_metrics = store.load()
    if not all_metrics:
        print("No metrics found.", file=sys.stderr)
        return 1

    metrics = (
        [m for m in all_metrics if m.pipeline_name == args.pipeline]
        if args.pipeline
        else all_metrics
    )

    events = replay(metrics, warn_threshold=args.warn, critical_threshold=args.critical)

    if args.summary:
        data = replay_summary(events)
        if args.as_json:
            print(json.dumps(data, indent=2))
        else:
            for k, v in data.items():
                print(f"{k}: {v}")
        return 0

    if args.as_json:
        print(json.dumps([e.to_dict() for e in events], indent=2))
    else:
        for e in events:
            alert_str = ", ".join(a.severity for a in e.alerts) or "ok"
            print(f"[{e.index}] {e.metric.pipeline_name}: {alert_str}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
