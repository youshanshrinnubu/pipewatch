"""Tests for pipewatch.config."""

import json
import os
import pytest
from pipewatch.config import load_config, load_config_from_env, PipelineConfig, WatcherConfig


SAMPLE_CONFIG = {
    "default_interval": 30,
    "log_file": "/tmp/pipewatch.log",
    "alert_handlers": ["stdout", "stderr"],
    "pipelines": [
        {
            "name": "etl_main",
            "interval": 45,
            "failure_rate_warning": 0.05,
            "failure_rate_critical": 0.25,
            "tags": ["prod", "nightly"],
        },
        {
            "name": "etl_secondary",
        },
    ],
}


@pytest.fixture
def config_file(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(SAMPLE_CONFIG))
    return str(p)


def test_load_config_returns_watcher_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, WatcherConfig)


def test_load_config_pipelines(config_file):
    cfg = load_config(config_file)
    assert len(cfg.pipelines) == 2
    assert cfg.pipelines[0].name == "etl_main"
    assert cfg.pipelines[1].name == "etl_secondary"


def test_pipeline_defaults(config_file):
    cfg = load_config(config_file)
    secondary = cfg.pipelines[1]
    assert secondary.interval == 60
    assert secondary.failure_rate_warning == 0.1
    assert secondary.failure_rate_critical == 0.3
    assert secondary.tags == []


def test_pipeline_custom_values(config_file):
    cfg = load_config(config_file)
    main = cfg.pipelines[0]
    assert main.interval == 45
    assert main.failure_rate_warning == 0.05
    assert main.tags == ["prod", "nightly"]


def test_watcher_config_fields(config_file):
    cfg = load_config(config_file)
    assert cfg.default_interval == 30
    assert cfg.log_file == "/tmp/pipewatch.log"
    assert cfg.alert_handlers == ["stdout", "stderr"]


def test_missing_config_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.json")


def test_load_config_from_env(monkeypatch):
    monkeypatch.setenv("PIPEWATCH_CONFIG", "/some/path.json")
    assert load_config_from_env() == "/some/path.json"


def test_load_config_from_env_missing(monkeypatch):
    monkeypatch.delenv("PIPEWATCH_CONFIG", raising=False)
    assert load_config_from_env() is None
