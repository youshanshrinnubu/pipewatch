"""Tests for pipewatch.cli."""

import json
import pytest
from unittest.mock import MagicMock, patch
from pipewatch.cli import run, build_alert_manager
from pipewatch.config import WatcherConfig, PipelineConfig


@pytest.fixture
def config_file(tmp_path):
    data = {
        "default_interval": 10,
        "alert_handlers": ["stdout"],
        "pipelines": [{"name": "test_pipe"}],
    }
    p = tmp_path / "config.json"
    p.write_text(json.dumps(data))
    return str(p)


def test_run_missing_config_returns_error():
    result = run([])
    assert result == 1


def test_run_nonexistent_config_returns_error():
    result = run(["-c", "/no/such/file.json"])
    assert result == 1


def test_run_once_calls_run_once(config_file):
    with patch("pipewatch.cli.PipelineWatcher") as MockWatcher:
        instance = MockWatcher.return_value
        result = run(["-c", config_file, "--once"])
        instance.run_once.assert_called_once()
        assert result == 0


def test_run_start_called_without_once(config_file):
    with patch("pipewatch.cli.PipelineWatcher") as MockWatcher:
        instance = MockWatcher.return_value
        result = run(["-c", config_file])
        instance.start.assert_called_once()
        assert result == 0


def test_interval_override(config_file):
    with patch("pipewatch.cli.PipelineWatcher") as MockWatcher:
        run(["-c", config_file, "--interval", "99", "--once"])
        _, kwargs = MockWatcher.call_args
        assert kwargs.get("interval") == 99


def test_default_interval_from_config(config_file):
    """When no --interval flag is given, the interval should come from the config file."""
    with patch("pipewatch.cli.PipelineWatcher") as MockWatcher:
        run(["-c", config_file, "--once"])
        _, kwargs = MockWatcher.call_args
        assert kwargs.get("interval") == 10


def test_build_alert_manager_stdout():
    cfg = WatcherConfig(alert_handlers=["stdout"])
    manager = build_alert_manager(cfg)
    assert len(manager._handlers) == 1


def test_build_alert_manager_file(tmp_path):
    log = str(tmp_path / "out.log")
    cfg = WatcherConfig(alert_handlers=["file"], log_file=log)
    manager = build_alert_manager(cfg)
    assert len(manager._handlers) == 1


def test_build_alert_manager_multiple_handlers(tmp_path):
    """When multiple alert handlers are configured, all should be registered."""
    log = str(tmp_path / "out.log")
    cfg = WatcherConfig(alert_handlers=["stdout", "file"], log_file=log)
    manager = build_alert_manager(cfg)
    assert len(manager._handlers) == 2
