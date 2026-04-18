"""Built-in alert handlers for pipewatch."""
import json
import sys
from typing import TextIO
from pipewatch.alerts import Alert


def stdout_handler(alert: Alert) -> None:
    """Print alert as a formatted string to stdout."""
    print(
        f"[{alert.severity.upper()}] {alert.triggered_at.strftime('%Y-%m-%dT%H:%M:%S')} "
        f"| {alert.pipeline_name} | {alert.reason}"
    )


def stderr_handler(alert: Alert) -> None:
    """Print alert to stderr."""
    print(
        f"[{alert.severity.upper()}] {alert.pipeline_name} | {alert.reason}",
        file=sys.stderr,
    )


def json_handler(stream: TextIO = sys.stdout):
    """Return a handler that writes JSON-serialised alerts to a stream."""
    def _handler(alert: Alert) -> None:
        stream.write(json.dumps(alert.to_dict()) + "\n")
        stream.flush()
    return _handler


def make_file_handler(path: str):
    """Return a handler that appends JSON alert lines to a file."""
    def _handler(alert: Alert) -> None:
        with open(path, "a") as fh:
            fh.write(json.dumps(alert.to_dict()) + "\n")
    return _handler
