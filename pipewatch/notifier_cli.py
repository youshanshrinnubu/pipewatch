"""CLI to test notifier config and simulate alert suppression."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.alerts import Alert
from pipewatch.handlers import stdout_handler, json_handler
from pipewatch.notifier import Notifier, NotifierConfig


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-notifier",
        description="Simulate alert notifications with rate-limiting.",
    )
    p.add_argument("--pipeline", default="demo", help="Pipeline name")
    p.add_argument("--severity", default="warning", choices=["warning", "critical"])
    p.add_argument("--message", default="Test alert")
    p.add_argument("--count", type=int, default=5, help="Number of alert attempts")
    p.add_argument("--cooldown", type=int, default=300, help="Cooldown seconds")
    p.add_argument("--max-repeats", type=int, default=3, dest="max_repeats")
    p.add_argument("--format", choices=["text", "json"], default="text", dest="fmt")
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    cfg = NotifierConfig(cooldown_seconds=args.cooldown, max_repeats=args.max_repeats)
    notifier = Notifier(cfg)
    handler = json_handler if args.fmt == "json" else stdout_handler
    alert = Alert(
        pipeline=args.pipeline,
        severity=args.severity,
        message=args.message,
    )
    sent = 0
    suppressed = 0
    for i in range(args.count):
        result = notifier.notify(alert, handler)
        if result:
            sent += 1
        else:
            suppressed += 1
    summary = {"sent": sent, "suppressed": suppressed, "total": args.count}
    if args.fmt == "json":
        print(json.dumps(summary))
    else:
        print(f"\nSummary: {sent} sent, {suppressed} suppressed out of {args.count} attempts.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
