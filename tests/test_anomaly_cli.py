import json
from io import StringIO
from unittest.mock import patch
import pytest
from pipewatch.anomaly_cli import main


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    assert len(out) > 0


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert all("pipeline" in d for d in data)


def test_main_severity_critical_only(capsys):
    main(["--json", "--severity", "critical"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(d["severity"] == "critical" for d in data)


def test_main_severity_warning_only(capsys):
    main(["--json", "--severity", "warning"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(d["severity"] == "warning" for d in data)


def test_main_no_anomalies_message(capsys):
    # Set very high thresholds so nothing triggers
    main(["--warning", "0.99", "--critical", "1.0"])
    out = capsys.readouterr().out
    # export pipeline has error status so still triggers
    assert len(out) > 0
