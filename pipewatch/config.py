"""Configuration loading for pipewatch."""

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PipelineConfig:
    name: str
    interval: int = 60
    failure_rate_warning: float = 0.1
    failure_rate_critical: float = 0.3
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WatcherConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    default_interval: int = 60
    log_file: Optional[str] = None
    alert_handlers: List[str] = field(default_factory=lambda: ["stdout"])


def _parse_pipeline(data: Dict[str, Any]) -> PipelineConfig:
    return PipelineConfig(
        name=data["name"],
        interval=data.get("interval", 60),
        failure_rate_warning=data.get("failure_rate_warning", 0.1),
        failure_rate_critical=data.get("failure_rate_critical", 0.3),
        tags=data.get("tags", []),
        extra={k: v for k, v in data.items() if k not in (
            "name", "interval", "failure_rate_warning",
            "failure_rate_critical", "tags"
        )},
    )


def load_config(path: str) -> WatcherConfig:
    """Load watcher config from a JSON file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        data = json.load(f)
    pipelines = [_parse_pipeline(p) for p in data.get("pipelines", [])]
    return WatcherConfig(
        pipelines=pipelines,
        default_interval=data.get("default_interval", 60),
        log_file=data.get("log_file"),
        alert_handlers=data.get("alert_handlers", ["stdout"]),
    )


def load_config_from_env() -> Optional[str]:
    """Return config path from environment variable PIPEWATCH_CONFIG."""
    return os.environ.get("PIPEWATCH_CONFIG")
