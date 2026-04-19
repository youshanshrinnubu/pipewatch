"""CLI demo for the enricher module."""
import argparse
import json
from pipewatch.metrics import PipelineMetric
from pipewatch.enricher import Enricher


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Enrich pipeline metrics with metadata")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--env", default="production", help="Environment tag value")
    p.add_argument("--team", default="data-eng", help="Team tag value")
    return p


def _demo_metrics():
    return [
        PipelineMetric("sales_etl", "ok", 1000, 5),
        PipelineMetric("sales_reporting", "ok", 500, 0),
        PipelineMetric("inventory_sync", "error", 200, 200),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    enricher = Enricher()
    enricher.add_rule("env", args.env)
    enricher.add_rule("team", args.team, pipeline_prefix="sales")

    metrics = _demo_metrics()
    enriched = enricher.enrich_all(metrics)

    if args.json:
        print(json.dumps([e.to_dict() for e in enriched], indent=2))
    else:
        for e in enriched:
            meta_str = ", ".join(f"{k}={v}" for k, v in e.meta.items())
            print(f"[{e.metric.pipeline_name}] status={e.metric.status} meta=({meta_str})")


if __name__ == "__main__":
    main()
