"""Flexible output formatter for pipeline data (text/json/csv)."""
from __future__ import annotations
import csv
import io
import json
from dataclasses import dataclass
from typing import Any, List, Literal

FormatMode = Literal["text", "json", "csv"]


@dataclass
class FormatResult:
    mode: FormatMode
    output: str

    def print(self) -> None:
        print(self.output)


def _to_rows(records: List[dict]) -> List[dict]:
    """Flatten nested dicts one level for CSV."""
    rows = []
    for r in records:
        row = {}
        for k, v in r.items():
            if isinstance(v, dict):
                for sk, sv in v.items():
                    row[f"{k}.{sk}"] = sv
            else:
                row[k] = v
        rows.append(row)
    return rows


def format_records(records: List[dict], mode: FormatMode = "text",
                   title: str = "") -> FormatResult:
    if mode == "json":
        output = json.dumps(records, indent=2, default=str)
        return FormatResult(mode=mode, output=output)

    if mode == "csv":
        if not records:
            return FormatResult(mode=mode, output="")
        rows = _to_rows(records)
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return FormatResult(mode=mode, output=buf.getvalue().rstrip())

    # text
    lines = []
    if title:
        lines.append(f"=== {title} ===")
    for rec in records:
        parts = [f"{k}={v}" for k, v in rec.items()]
        lines.append("  " + "  ".join(parts))
    if not records:
        lines.append("(no records)")
    return FormatResult(mode=mode, output="\n".join(lines))


def format_single(record: Any, mode: FormatMode = "text") -> FormatResult:
    """Format a single dataclass-like object that exposes to_dict()."""
    d = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    return format_records([d], mode=mode)
