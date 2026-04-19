import argparse
import json
from pipewatch.alerts import Alert
from pipewatch.dispatcher import DispatchRule, Dispatcher, format_dispatch_results


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Dispatch alerts to labeled handlers")
    p.add_argument("--json", action="store_true", help="Output as JSON")
    p.add_argument("--severity", default=None, help="Filter rule by severity")
    return p


def _demo_alerts():
    return [
        Alert(pipeline="sales", severity="warning", message="High failure rate"),
        Alert(pipeline="inventory", severity="critical", message="Pipeline error"),
        Alert(pipeline="orders", severity="warning", message="Slow run"),
    ]


def main(argv=None):
    args = _build_parser().parse_args(argv)
    alerts = _demo_alerts()

    received = []

    def capture(label):
        def _h(alert: Alert):
            received.append((label, alert))
        return _h

    dispatcher = Dispatcher()
    dispatcher.add_rule(DispatchRule(severity="critical", pipeline=None, handler=capture("critical-channel"), label="critical-channel"))
    dispatcher.add_rule(DispatchRule(severity="warning", pipeline=None, handler=capture("warning-channel"), label="warning-channel"))
    if args.severity:
        alerts = [a for a in alerts if a.severity == args.severity]

    results = dispatcher.dispatch_all(alerts)

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_dispatch_results(results))


if __name__ == "__main__":
    main()
