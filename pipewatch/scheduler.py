"""Simple interval-based scheduler for periodic pipeline checks."""

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional


@dataclass
class ScheduledJob:
    name: str
    fn: Callable
    interval_seconds: float
    last_run: Optional[float] = field(default=None)
    run_count: int = 0

    def is_due(self, now: float) -> bool:
        if self.last_run is None:
            return True
        return (now - self.last_run) >= self.interval_seconds

    def run(self) -> None:
        self.fn()
        self.last_run = time.monotonic()
        self.run_count += 1


class Scheduler:
    def __init__(self) -> None:
        self._jobs: Dict[str, ScheduledJob] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def register(self, name: str, fn: Callable, interval_seconds: float) -> None:
        self._jobs[name] = ScheduledJob(name=name, fn=fn, interval_seconds=interval_seconds)

    def unregister(self, name: str) -> None:
        self._jobs.pop(name, None)

    def tick(self) -> None:
        now = time.monotonic()
        for job in list(self._jobs.values()):
            if job.is_due(now):
                job.run()

    def start(self, poll_interval: float = 1.0) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, args=(poll_interval,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self, poll_interval: float) -> None:
        while self._running:
            self.tick()
            time.sleep(poll_interval)

    def job_names(self):
        return list(self._jobs.keys())
