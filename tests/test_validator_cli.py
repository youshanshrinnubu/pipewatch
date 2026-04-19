import json
import pytest
from unittest.mock import patch, MagicMock
from pipewatch.metrics import PipelineMetric
from pipewatch import validator_cli


def make_metric(**kwargs) -> PipelineMetric:
    defaults = dict(
        pipeline_name="sales",
        total_records=100,
        failed_records=2,
        status="ok",
        duration_seconds=1.5,
    )
    defaults.update(kwargs)
    return PipelineMetric(**defaults)


def _patch_store(metrics):
    mock_store = MagicMock()
    mock_store.load.return_value = metrics
    return patch("pipewatch.validator_cli.SnapshotStore", return_value=mock_store)


def test_main_text_output(capsys):
    with _patch_store([make_metric()]):
        validator_cli.main([])
    out = capsys.readouterr().out
    assert "PASS" in out


def test_main_json_output(capsys):
    with _patch_store([make_metric()]):
        validator_cli.main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "sales"
    assert data[0]["valid"] is True


def test_main_errors_only_hides_passing(capsys):
    metrics = [make_metric(pipeline_name="good"), make_metric(pipeline_name="bad", total_records=-1)]
    with _patch_store(metrics):
        with pytest.raises(SystemExit):
            validator_cli.main(["--errors-only"])
    out = capsys.readouterr().out
    assert "good" not in out
    assert "bad" in out


def test_main_exits_1_on_errors():
    with _patch_store([make_metric(total_records=-1)]):
        with pytest.raises(SystemExit) as exc:
            validator_cli.main([])
    assert exc.value.code == 1


def test_main_no_exit_when_valid():
    with _patch_store([make_metric()]):
        validator_cli.main([])  # should not raise


def test_main_pipeline_filter(capsys):
    metrics = [make_metric(pipeline_name="sales"), make_metric(pipeline_name="inventory")]
    with _patch_store(metrics):
        validator_cli.main(["--pipeline", "sales"])
    out = capsys.readouterr().out
    assert "sales" in out
    assert "inventory" not in out
