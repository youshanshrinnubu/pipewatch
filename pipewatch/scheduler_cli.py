"""CLI entry point to demonstrate/test the scheduler with a dry run."""

import argparse
import time
from pipewatch.scheduler import Scheduler


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pipewatch scheduler demo")
    p.add_argument("--jobs", nargs="+", default=["pipeline_a", "pipeline_b"],
                   help="Job names to register")
    p.add_argument("--interval", type=float, default=2.0,
                   help="Interval in seconds between job runs")
    p.add_argument("--duration", type=float, default=6.0,
                   help="How long to run the scheduler (seconds)")
    p.add_argument("--json", action="store_true", help="Output run counts as JSON")
    return p


def main(argv=None) -> None:
    args = _build_parser().parse_args(argv)
    scheduler = Scheduler()
    counters = {name: 0 for name in args.jobs}

    for name in args.jobs:
        def make_fn(n):
            def fn():
                counters[n] += 1
                if not args.json:
                    print(f"[scheduler] ran job '{n}' (total: {counters[n]})")
            return fn
        scheduler.register(name, make_fn(name), args.interval)

    scheduler.start(poll_interval=0.5)
    time.sleep(args.duration)
    scheduler.stop()

    if args.json:
        import json
        print(json.dumps(counters, indent=2))
    else:
        for name, count in counters.items():
            print(f"{name}: {count} run(s)")


if __name__ == "__main__":
    main()
