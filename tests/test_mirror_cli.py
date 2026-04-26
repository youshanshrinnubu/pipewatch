"""Tests for pipewatch.mirror_cli."""
from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

import pytest

from pipewatch.mirror_cli import main


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    # All three demo pipelines should appear
    assert "sales_etl" in out
    assert "inventory_sync" in out
    assert "sales_report" in out


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 3
    assert all("pipeline" in item for item in data)
    assert all("destinations" in item for item in data)


def test_main_prefix_filters_destinations(capsys):
    main(["--prefix", "sales"])
    out = capsys.readouterr().out
    lines = {line.split(":")[0].strip(): line for line in out.strip().splitlines()}
    # sales_etl and sales_report should be mirrored
    assert "primary" in lines["sales_etl"]
    assert "primary" in lines["sales_report"]
    # inventory_sync should be skipped
    assert "skipped" in lines["inventory_sync"]


def test_main_json_prefix_filter(capsys):
    main(["--prefix", "inventory", "--json"])
    data = json.loads(capsys.readouterr().out)
    by_name = {item["pipeline"]: item for item in data}
    assert by_name["inventory_sync"]["skipped"] is False
    assert by_name["sales_etl"]["skipped"] is True


def test_main_no_prefix_no_skipped(capsys):
    main(["--json"])
    data = json.loads(capsys.readouterr().out)
    assert all(item["skipped"] is False for item in data)
