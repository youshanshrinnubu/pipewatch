from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert
import time


@dataclass
class RetryConfig:
    max_retries: int = 3
    backoff_seconds: float = 2.0
    failure_rate_threshold: float = 0.5


@dataclass
class RetryRecord:
    pipeline: str
    attempt: int
    succeeded: bool
    reason: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "attempt": self.attempt,
            "succeeded": self.succeeded,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


@dataclass
class RetryResult:
    pipeline: str
    total_attempts: int
    succeeded: bool
    records: List[RetryRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_attempts": self.total_attempts,
            "succeeded": self.succeeded,
            "records": [r.to_dict() for r in self.records],
        }


class Retrier:
    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()
        self._history: Dict[str, List[RetryRecord]] = {}

    def should_retry(self, metric: PipelineMetric) -> bool:
        total = metric.total_records
        if total == 0:
            return False
        rate = metric.failed_records / total
        return rate >= self.config.failure_rate_threshold or metric.status == "error"

    def record_attempt(self, pipeline: str, attempt: int, succeeded: bool, reason: str) -> RetryRecord:
        rec = RetryRecord(pipeline=pipeline, attempt=attempt, succeeded=succeeded, reason=reason)
        self._history.setdefault(pipeline, []).append(rec)
        return rec

    def evaluate(self, metric: PipelineMetric) -> RetryResult:
        pipeline = metric.pipeline_name
        history = self._history.get(pipeline, [])
        attempts = len(history)
        succeeded = any(r.succeeded for r in history)
        return RetryResult(
            pipeline=pipeline,
            total_attempts=attempts,
            succeeded=succeeded,
            records=list(history),
        )

    def exhausted(self, pipeline: str) -> bool:
        return len(self._history.get(pipeline, [])) >= self.config.max_retries
