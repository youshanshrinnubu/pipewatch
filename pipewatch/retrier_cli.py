import argparse
import json
from pipewatch.retrier import Retrier, RetryConfig
from pipewatch.metrics import PipelineMetric
import time


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Simulate pipeline retry evaluation")
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--backoff", type=float, default=2.0)
    p.add_argument("--threshold", type=float, default=0.5)
    p.add_argument("--json", action="store_true")
    return p


def _demo_metrics():
    now = time.time()
    return [
        PipelineMetric("sales", 100, 60, "ok", now - 10),
        PipelineMetric("inventory", 200, 10, "ok", now - 20),
        PipelineMetric("orders", 50, 50, "error", now - 5),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    config = RetryConfig(
        max_retries=args.max_retries,
        backoff_seconds=args.backoff,
        failure_rate_threshold=args.threshold,
    )
    retrier = Retrier(config)
    metrics = _demo_metrics()
    results = []
    for m in metrics:
        if retrier.should_retry(m):
            for i in range(1, config.max_retries + 1):
                succeeded = i == config.max_retries
                retrier.record_attempt(m.pipeline_name, i, succeeded, "simulated")
                if succeeded:
                    break
        result = retrier.evaluate(m)
        results.append(result)

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            status = "OK" if r.succeeded or r.total_attempts == 0 else "FAILED"
            print(f"{r.pipeline}: attempts={r.total_attempts} status={status}")


if __name__ == "__main__":
    main()
