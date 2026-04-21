"""CLI entry-point for the pipewatch audit log viewer."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.auditor import AuditLog, AuditEntry


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-audit",
        description="View the pipewatch audit log.",
    )
    p.add_argument("log_file", help="Path to audit log JSONL file")
    p.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    p.add_argument(
        "--type",
        dest="event_type",
        choices=["metric", "alert"],
        default=None,
        help="Filter by event type",
    )
    p.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")
    p.add_argument("--last", type=int, default=None, metavar="N", help="Show last N entries")
    return p


def _format_text(entries: List[AuditEntry]) -> str:
    if not entries:
        return "No audit entries found."
    lines = []
    for e in entries:
        lines.append(
            f"[{e.id}] {e.event_type.upper():6s}  pipeline={e.pipeline}  "
            f"ts={e.timestamp:.2f}  detail={json.dumps(e.detail)}"
        )
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    log = AuditLog(path=args.log_file)
    entries = log.entries(pipeline=args.pipeline, event_type=args.event_type)

    if args.last is not None:
        entries = entries[-args.last :]

    if args.as_json:
        print(json.dumps([e.to_dict() for e in entries], indent=2))
    else:
        print(_format_text(entries))

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
