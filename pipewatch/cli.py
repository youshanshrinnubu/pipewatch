"""CLI entry point for pipewatch."""

import argparse
import sys
from typing import List, Optional

from pipewatch.config import load_config, load_config_from_env
from pipewatch.handlers import stdout_handler, stderr_handler, make_file_handler
from pipewatch.alerts import AlertManager
from pipewatch.watcher import PipelineWatcher


def build_alert_manager(cfg) -> AlertManager:
    manager = AlertManager()
    for handler_name in cfg.alert_handlers:
        if handler_name == "stdout":
            manager.register_handler(stdout_handler)
        elif handler_name == "stderr":
            manager.register_handler(stderr_handler)
        elif handler_name == "file" and cfg.log_file:
            manager.register_handler(make_file_handler(cfg.log_file))
    return manager


def run(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Lightweight CLI monitor for ETL pipeline health.",
    )
    parser.add_argument(
        "-c", "--config",
        help="Path to JSON config file (overrides PIPEWATCH_CONFIG env var)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single check cycle and exit",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Override polling interval in seconds",
    )
    args = parser.parse_args(argv)

    config_path = args.config or load_config_from_env()
    if not config_path:
        print("Error: no config file specified. Use -c or set PIPEWATCH_CONFIG.", file=sys.stderr)
        return 1

    try:
        cfg = load_config(config_path)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    alert_manager = build_alert_manager(cfg)
    interval = args.interval or cfg.default_interval
    watcher = PipelineWatcher(alert_manager=alert_manager, interval=interval)

    if args.once:
        watcher.run_once()
    else:
        watcher.start()

    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
