"""Tests for pipewatch.freezer_cli."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore
from pipewatch.freezer_cli import main


def make_metric(name: str, status: str = "ok") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=50,
        failed_records=0,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@pytest.fixture
def snap_file(tmp_path):
    path = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    store.save(make_metric("pipeline_a"))
    store.save(make_metric("pipeline_b", status="warning"))
    return path


@pytest.fixture
def freeze_file(tmp_path):
    return str(tmp_path / "freeze.json")


def test_freeze_text_output(snap_file, freeze_file, capsys):
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "v1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v1" in out


def test_freeze_json_output(snap_file, freeze_file, capsys):
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "v1", "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["label"] == "v1"
    assert isinstance(data["metrics"], list)


def test_thaw_returns_frozen(snap_file, freeze_file, capsys):
    main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "mysnap"])
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "thaw", "mysnap", "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["label"] == "mysnap"


def test_thaw_missing_label_returns_error(snap_file, freeze_file):
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "thaw", "ghost"])
    assert rc == 1


def test_list_labels(snap_file, freeze_file, capsys):
    main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "alpha"])
    main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "beta"])
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_list_json_output(snap_file, freeze_file, capsys):
    main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "z1"])
    main(["--store", snap_file, "--freeze-store", freeze_file, "list", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert "z1" in data


def test_delete_label(snap_file, freeze_file, capsys):
    main(["--store", snap_file, "--freeze-store", freeze_file, "freeze", "del_me"])
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "delete", "del_me"])
    assert rc == 0


def test_delete_missing_label_returns_error(snap_file, freeze_file):
    rc = main(["--store", snap_file, "--freeze-store", freeze_file, "delete", "no_such"])
    assert rc == 1


def test_no_command_returns_error(snap_file, freeze_file):
    rc = main(["--store", snap_file, "--freeze-store", freeze_file])
    assert rc == 1
