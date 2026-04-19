"""Tests for pipewatch.classifier_cli."""
import json
import pytest
from pipewatch.classifier_cli import main


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    assert len(out) > 0


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert all("tier" in item for item in data)


def test_main_tier_filter_healthy(capsys):
    main(["--tier", "healthy"])
    out = capsys.readouterr().out
    assert "HEALTHY" in out
    assert "CRITICAL" not in out


def test_main_tier_filter_critical(capsys):
    main(["--tier", "critical"])
    out = capsys.readouterr().out
    assert "CRITICAL" in out


def test_main_json_tier_filter(capsys):
    main(["--json", "--tier", "degraded"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(item["tier"] == "degraded" for item in data)
