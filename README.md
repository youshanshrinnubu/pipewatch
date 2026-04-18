# pipewatch

Lightweight CLI monitor for ETL pipeline health with alerting hooks.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/youruser/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Monitor a pipeline by pointing pipewatch at your pipeline config:

```bash
pipewatch monitor --config pipeline.yaml --interval 60
```

Check pipeline status manually:

```bash
pipewatch status --pipeline my_etl_job
```

Define alerting hooks in your config file:

```yaml
pipeline: my_etl_job
checks:
  - type: row_count
    threshold: 1000
alerts:
  - type: slack
    webhook: https://hooks.slack.com/services/your/webhook/url
  - type: email
    to: ops@example.com
```

Run with verbose output for debugging:

```bash
pipewatch monitor --config pipeline.yaml --verbose
```

---

## Features

- Real-time ETL pipeline health monitoring from the command line
- Configurable alerting hooks (Slack, email, webhooks)
- Lightweight with minimal dependencies
- YAML-based configuration

---

## License

This project is licensed under the [MIT License](LICENSE).