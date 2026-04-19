import json
from tests.test_sampler import make_metric
from pipewatch.sampler_cli import main
from unittest.mock import patch


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    assert len(out) > 0


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "pipeline" in data[0]
    assert "sampled" in data[0]


def test_main_n_limits_samples(capsys):
    main(["--n", "1", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    for entry in data:
        assert entry["sample_size"] <= 1


def test_main_seed_reproducible(capsys):
    main(["--seed", "42", "--json"])
    out1 = capsys.readouterr().out
    main(["--seed", "42", "--json"])
    out2 = capsys.readouterr().out
    assert out1 == out2


def test_main_text_contains_pipeline(capsys):
    main(["--n", "2"])
    out = capsys.readouterr().out
    assert "sales" in out or "inventory" in out
