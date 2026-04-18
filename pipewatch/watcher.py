"""Watcher: periodically polls MetricsCollector and evaluates alerts."""
import time
import logging
from typing import Optional
from pipewatch.metrics import MetricsCollector
from pipewatch.alerts import AlertManager

logger = logging.getLogger(__name__)


class PipelineWatcher:
    """Poll a MetricsCollector at a fixed interval and run alert evaluation."""

    def __init__(
        self,
        collector: MetricsCollector,
        alert_manager: AlertManager,
        interval: float = 60.0,
        max_iterations: Optional[int] = None,
    ):
        self.collector = collector
        self.alert_manager = alert_manager
        self.interval = interval
        self.max_iterations = max_iterations
        self._iteration = 0

    def run_once(self) -> int:
        """Evaluate all current metrics. Returns number of alerts fired."""
        metrics = self.collector.all_metrics()
        fired = 0
        for metric in metrics:
            alert = self.alert_manager.evaluate(metric)
            if alert:
                logger.warning("Alert fired: %s", alert.reason)
                fired += 1
        return fired

    def start(self) -> None:
        """Block and poll until max_iterations reached or KeyboardInterrupt."""
        logger.info("PipelineWatcher started (interval=%.1fs)", self.interval)
        try:
            while True:
                self.run_once()
                self._iteration += 1
                if (
                    self.max_iterations is not None
                    and self._iteration >= self.max_iterations
                ):
                    break
                time.sleep(self.interval)
        except KeyboardInterrupt:
            logger.info("PipelineWatcher stopped by user.")
