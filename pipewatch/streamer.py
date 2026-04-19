"""Stream metrics in real-time, yielding events as they arrive."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert, AlertManager


@dataclass
class StreamEvent:
    metric: PipelineMetric
    alerts: List[Alert]
    sequence: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "pipeline": self.metric.pipeline_name,
            "status": self.metric.status,
            "alerts": [a.to_dict() for a in self.alerts],
        }


@dataclass
class StreamConfig:
    max_events: Optional[int] = None
    interval: float = 0.0  # seconds between yields (0 = no sleep)
    pipelines: Optional[List[str]] = None  # None means all


class MetricStreamer:
    def __init__(
        self,
        alert_manager: AlertManager,
        config: Optional[StreamConfig] = None,
    ) -> None:
        self._am = alert_manager
        self._config = config or StreamConfig()
        self._sequence = 0

    def _accept(self, metric: PipelineMetric) -> bool:
        if self._config.pipelines is None:
            return True
        return metric.pipeline_name in self._config.pipelines

    def stream(
        self, source: Callable[[], List[PipelineMetric]]
    ) -> Iterator[StreamEvent]:
        """Repeatedly call *source* and yield StreamEvents."""
        emitted = 0
        while True:
            metrics = source()
            for metric in metrics:
                if not self._accept(metric):
                    continue
                alerts = self._am.evaluate(metric)
                self._sequence += 1
                event = StreamEvent(
                    metric=metric,
                    alerts=alerts,
                    sequence=self._sequence,
                )
                yield event
                emitted += 1
                if (
                    self._config.max_events is not None
                    and emitted >= self._config.max_events
                ):
                    return
            if self._config.interval > 0:
                time.sleep(self._config.interval)
            else:
                return  # single-pass when no interval
