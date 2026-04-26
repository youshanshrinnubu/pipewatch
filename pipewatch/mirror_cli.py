"""CLI entry point for the mirror module (demo / smoke-test)."""
from __future__ import annotations

import argparse
import json
from typing import List

from pipewatch.metrics import PipelineMetric
from pipewatch.mirror import Mirror, MirrorRule


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Mirror pipeline metrics to multiple destinations."
    )
    p.add_argument(
        "--prefix",
        default=None,
        help="Only mirror pipelines whose names start with this prefix.",
    )
    p.add_argument(
        "--json", action="store_true", dest="as_json", help="Output as JSON."
    )
    return p


def _demo_metrics() -> List[PipelineMetric]:
    import time

    now = time.time()
    return [
        PipelineMetric("sales_etl", "ok", 1000, 5, now),
        PipelineMetric("inventory_sync", "warning", 500, 60, now),
        PipelineMetric("sales_report", "ok", 200, 0, now),
    ]


def main(argv: list | None = None) -> None:
    args = _build_parser().parse_args(argv)

    captured: list = []

    def _capture(dest: str):
        def _h(metric: PipelineMetric) -> None:
            captured.append({"destination": dest, "pipeline": metric.pipeline_name})

        return _h

    mirror = Mirror()
    mirror.register_destination("primary", _capture("primary"))
    mirror.register_destination("backup", _capture("backup"))
    mirror.add_rule(MirrorRule(destination="primary", pipeline_prefix=args.prefix))
    mirror.add_rule(MirrorRule(destination="backup", pipeline_prefix=args.prefix))

    results = mirror.send_all(_demo_metrics())

    if args.as_json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            status = "skipped" if r.skipped else ", ".join(r.destinations)
            print(f"{r.metric.pipeline_name}: {status}")


if __name__ == "__main__":  # pragma: no cover
    main()
