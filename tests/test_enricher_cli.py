import json
from pipewatch.enricher_cli import main


def test_main_text_output(capsys):
    main([])
    out = capsys.readouterr().out
    assert "sales_etl" in out
    assert "env=production" in out


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 3
    assert "meta" in data[0]


def test_main_custom_env(capsys):
    main(["--env", "staging"])
    out = capsys.readouterr().out
    assert "env=staging" in out


def test_main_team_only_on_sales(capsys):
    main(["--json", "--team", "analytics"])
    out = capsys.readouterr().out
    data = json.loads(out)
    sales = [d for d in data if d["pipeline"].startswith("sales")]
    non_sales = [d for d in data if not d["pipeline"].startswith("sales")]
    assert all(d["meta"].get("team") == "analytics" for d in sales)
    assert all("team" not in d["meta"] for d in non_sales)


def test_main_inventory_has_no_team(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    inv = next(d for d in data if d["pipeline"] == "inventory_sync")
    assert "team" not in inv["meta"]
