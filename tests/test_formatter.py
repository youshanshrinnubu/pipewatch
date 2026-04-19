"""Tests for pipewatch.formatter."""
import json
import pytest
from pipewatch.formatter import format_records, format_single, FormatResult


DEMO = [
    {"pipeline": "sales", "status": "ok", "failure_rate": 0.01},
    {"pipeline": "orders", "status": "error", "failure_rate": 0.5},
]


def test_format_records_json_valid():
    result = format_records(DEMO, mode="json")
    assert result.mode == "json"
    parsed = json.loads(result.output)
    assert len(parsed) == 2
    assert parsed[0]["pipeline"] == "sales"


def test_format_records_json_empty():
    result = format_records([], mode="json")
    assert json.loads(result.output) == []


def test_format_records_csv_has_header():
    result = format_records(DEMO, mode="csv")
    lines = result.output.splitlines()
    assert lines[0] == "pipeline,status,failure_rate"
    assert len(lines) == 3  # header + 2 rows


def test_format_records_csv_empty():
    result = format_records([], mode="csv")
    assert result.output == ""


def test_format_records_text_contains_values():
    result = format_records(DEMO, mode="text")
    assert "sales" in result.output
    assert "orders" in result.output


def test_format_records_text_with_title():
    result = format_records(DEMO, mode="text", title="My Report")
    assert "My Report" in result.output


def test_format_records_text_empty_message():
    result = format_records([], mode="text")
    assert "no records" in result.output


def test_format_single_uses_to_dict():
    class Fake:
        def to_dict(self):
            return {"x": 1, "y": 2}

    result = format_single(Fake(), mode="json")
    parsed = json.loads(result.output)
    assert parsed == [{"x": 1, "y": 2}]


def test_format_result_print(capsys):
    result = FormatResult(mode="text", output="hello world")
    result.print()
    captured = capsys.readouterr()
    assert "hello world" in captured.out


def test_csv_flattens_nested_dict():
    records = [{"pipeline": "a", "meta": {"env": "prod", "team": "ops"}}]
    result = format_records(records, mode="csv")
    lines = result.output.splitlines()
    assert "meta.env" in lines[0]
    assert "meta.team" in lines[0]
    assert "prod" in lines[1]
