"""Tests for grouper_cli."""
import json
from pipewatch.grouper_cli import main


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    assert "ok" in out


def test_main_by_status(capsys):
    main(["--by", "status"])
    out = capsys.readouterr().out
    assert "ok" in out
    assert "error" in out


def test_main_by_pipeline(capsys):
    main(["--by", "pipeline"])
    out = capsys.readouterr().out
    assert "sales" in out
    assert "billing" in out


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, dict)
    assert "ok" in data


def test_main_json_by_pipeline(capsys):
    main(["--by", "pipeline", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "sales" in data
    assert data["sales"]["count"] >= 1
