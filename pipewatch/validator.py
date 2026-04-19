from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class ValidationIssue:
    field: str
    message: str
    severity: str = "error"  # "error" or "warning"


@dataclass
class ValidationResult:
    pipeline: str
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "valid": self.valid,
            "issues": [{"field": i.field, "message": i.message, "severity": i.severity} for i in self.issues],
        }


def validate_metric(metric: PipelineMetric) -> ValidationResult:
    issues: List[ValidationIssue] = []

    if not metric.pipeline_name or not metric.pipeline_name.strip():
        issues.append(ValidationIssue("pipeline_name", "Pipeline name must not be empty"))

    if metric.total_records < 0:
        issues.append(ValidationIssue("total_records", "total_records must be >= 0"))

    if metric.failed_records < 0:
        issues.append(ValidationIssue("failed_records", "failed_records must be >= 0"))

    if metric.total_records > 0 and metric.failed_records > metric.total_records:
        issues.append(ValidationIssue("failed_records", "failed_records exceeds total_records"))

    if metric.status not in ("ok", "warning", "error", "unknown"):
        issues.append(ValidationIssue("status", f"Unknown status value: {metric.status!r}", severity="warning"))

    if metric.duration_seconds is not None and metric.duration_seconds < 0:
        issues.append(ValidationIssue("duration_seconds", "duration_seconds must be >= 0"))

    return ValidationResult(pipeline=metric.pipeline_name, issues=issues)


def validate_all(metrics: List[PipelineMetric]) -> List[ValidationResult]:
    return [validate_metric(m) for m in metrics]


def format_validation(results: List[ValidationResult]) -> str:
    lines = []
    for r in results:
        status = "PASS" if r.valid else "FAIL"
        lines.append(f"[{status}] {r.pipeline}")
        for issue in r.issues:
            lines.append(f"  [{issue.severity.upper()}] {issue.field}: {issue.message}")
    return "\n".join(lines) if lines else "No metrics to validate."
