"""Tests for pipewatch.scheduler."""

import time
import pytest
from pipewatch.scheduler import Scheduler, ScheduledJob


def test_job_is_due_first_time():
    calls = []
    job = ScheduledJob(name="j", fn=lambda: calls.append(1), interval_seconds=5.0)
    assert job.is_due(time.monotonic())


def test_job_not_due_after_run():
    calls = []
    job = ScheduledJob(name="j", fn=lambda: calls.append(1), interval_seconds=60.0)
    job.run()
    assert not job.is_due(time.monotonic())


def test_job_due_after_interval():
    calls = []
    job = ScheduledJob(name="j", fn=lambda: calls.append(1), interval_seconds=0.0)
    job.run()
    time.sleep(0.01)
    assert job.is_due(time.monotonic())


def test_job_run_increments_count():
    calls = []
    job = ScheduledJob(name="j", fn=lambda: calls.append(1), interval_seconds=0.0)
    job.run()
    job.run()
    assert job.run_count == 2


def test_scheduler_register_and_tick():
    calls = []
    s = Scheduler()
    s.register("test", lambda: calls.append(1), interval_seconds=0.0)
    s.tick()
    assert len(calls) == 1


def test_scheduler_tick_respects_interval():
    calls = []
    s = Scheduler()
    s.register("test", lambda: calls.append(1), interval_seconds=999.0)
    s.tick()
    s.tick()
    assert len(calls) == 1


def test_scheduler_unregister():
    calls = []
    s = Scheduler()
    s.register("test", lambda: calls.append(1), interval_seconds=0.0)
    s.unregister("test")
    s.tick()
    assert len(calls) == 0


def test_scheduler_job_names():
    s = Scheduler()
    s.register("a", lambda: None, 1.0)
    s.register("b", lambda: None, 1.0)
    assert set(s.job_names()) == {"a", "b"}


def test_scheduler_start_stop_runs_jobs():
    calls = []
    s = Scheduler()
    s.register("bg", lambda: calls.append(1), interval_seconds=0.1)
    s.start(poll_interval=0.05)
    time.sleep(0.4)
    s.stop()
    assert len(calls) >= 2
