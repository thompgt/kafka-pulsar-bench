"""Command line interface.

    kpbench run configs/smoke.yaml
    kpbench run configs/smoke.yaml --driver loopback --repeat 10
    kpbench show results/<run_id>/manifest.json

Overrides exist for sweeps and quick experiments, and every one of them is
recorded in the manifest's config block. A tuning value that reached a run
without appearing in its manifest would be untraceable, which requirement M-9
exists to prevent.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Any

from kpbench.config import RunConfig, load_run_config
from kpbench.drivers.registry import AVAILABLE, build_driver
from kpbench.env import fingerprint
from kpbench.results import manifest as manifest_mod
from kpbench.workload.runner import BenchmarkRunner, RunOutcome


def _fmt_us(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}s"
    if v >= 1_000:
        return f"{v / 1000:.2f}ms"
    return f"{v:.0f}us"


def _print_summary(outcome: RunOutcome, config: RunConfig) -> None:
    m = outcome.metrics
    resp = m["latency"]["response"]
    svc = m["latency"]["service"]
    lag = m["latency"]["generator_lag"]
    d = m["delivery"]

    print()
    print(f"  run        {outcome.run_id}")
    print(f"  driver     {config.driver}")
    print(
        f"  workload   {config.workload.message_bytes}B "
        f"@ {config.workload.target_rate_hz:,.0f}/s "
        f"for {config.workload.duration_s:g}s "
        f"({config.topic.partitions}p, durability={config.producer.durability.value})"
    )
    print(
        f"  rate       {m['achieved_rate_hz']:,.0f}/s achieved "
        f"({m['achieved_rate_ratio']:.1%} of target)"
    )
    print(
        f"  delivery   {d['unique_received']:,} received, "
        f"{d['missing']:,} missing, {d['duplicates']:,} duplicate, "
        f"{d['out_of_order']:,} out of order"
    )
    print()
    print("  latency          p50       p99      p99.9       max")
    for label, h in (("response", resp), ("service", svc), ("gen lag", lag)):
        p = h["percentiles_us"]
        print(
            f"  {label:<12} {_fmt_us(p['p50']):>9} {_fmt_us(p['p99']):>9} "
            f"{_fmt_us(p['p99.9']):>10} {_fmt_us(h['max_us']):>9}"
        )
    print()
    print("  response = measured from intended send time (the honest number)")
    print("  service  = measured from actual send time (what a closed loop reports)")
    print()

    if outcome.valid:
        print("  VALID")
    else:
        print("  INVALID - not reportable as a latency result:")
        for r in outcome.reasons:
            print(f"    - {r}")
    print()


def _apply_overrides(config: RunConfig, args: argparse.Namespace) -> RunConfig:
    patch: dict[str, Any] = {}
    workload: dict[str, Any] = {}

    if args.driver:
        patch["driver"] = args.driver
    if args.rate is not None:
        workload["target_rate_hz"] = args.rate
    if args.duration is not None:
        workload["duration_s"] = args.duration
    if args.message_bytes is not None:
        workload["message_bytes"] = args.message_bytes
    if workload:
        patch["workload"] = config.workload.model_copy(update=workload)
    if args.partitions is not None:
        patch["topic"] = config.topic.model_copy(update={"partitions": args.partitions})
    if args.bootstrap:
        opts = dict(config.driver_options)
        opts["bootstrap.servers"] = args.bootstrap
        patch["driver_options"] = opts

    return config.model_copy(update=patch) if patch else config


def _cmd_run(args: argparse.Namespace) -> int:
    base = load_run_config(args.config)
    config = _apply_overrides(base, args)

    results_dir = pathlib.Path(args.results_dir)
    env = fingerprint.collect() if not args.no_fingerprint else {}

    # Discard runs come first. A broker's warm-up spans runs, not just the
    # warm-up window inside one (ADR-0004), so measuring immediately after a
    # broker reset samples the cold end of that curve.
    warmup_runs = args.warmup_runs
    total_runs = warmup_runs + args.repeat

    failures = 0
    for i in range(total_runs):
        is_warmup = i < warmup_runs
        if total_runs > 1:
            label = (
                f"warm-up {i + 1}/{warmup_runs}"
                if is_warmup
                else f"repeat {i - warmup_runs + 1}/{args.repeat}"
            )
            print(f"\n=== {label} ===")

        # A fresh topic name per run. Reusing one would let a slow drain from
        # the previous run leak into the next one's measurements.
        run_config = config
        if total_runs > 1:
            run_config = config.model_copy(
                update={"topic": config.topic.model_copy(update={"name": f"{config.topic.name}-{i}"})}
            )

        driver = build_driver(run_config)
        runner = BenchmarkRunner(run_config, driver)
        try:
            outcome = runner.run()
        finally:
            runner.cleanup()

        _print_summary(outcome, run_config)
        doc = manifest_mod.build(outcome, run_config, env, driver.client_info())
        # Retained rather than deleted: they are evidence the procedure was
        # followed, and a discard run that beats the measured run that follows
        # it is a signal something is wrong.
        doc["warmup_run"] = is_warmup
        path = manifest_mod.write(doc, results_dir)
        print(f"  manifest   {path}{' (warm-up, excluded)' if is_warmup else ''}")

        if not outcome.valid and not is_warmup:
            failures += 1

    if failures:
        print(f"\n{failures}/{args.repeat} run(s) invalid", file=sys.stderr)
        # Non-zero so a sweep or CI notices, rather than treating a ceiling as
        # a result.
        return 2
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    doc = manifest_mod.read(args.manifest)
    m = doc["metrics"]
    print(f"run      {doc['run_id']}")
    print(f"driver   {doc['driver']}")
    print(f"valid    {doc['valid']}")
    for r in doc.get("invalid_reasons", []):
        print(f"         - {r}")
    print(f"rate     {m['achieved_rate_hz']:,.0f}/s ({m['achieved_rate_ratio']:.1%})")
    p = m["latency"]["response"]["percentiles_us"]
    print(f"response p50={_fmt_us(p['p50'])} p99={_fmt_us(p['p99'])} p99.9={_fmt_us(p['p99.9'])}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kpbench", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute a benchmark run")
    run.add_argument("config", help="path to a run config YAML")
    run.add_argument("--driver", choices=AVAILABLE, help="override the configured driver")
    run.add_argument("--rate", type=float, help="override target_rate_hz")
    run.add_argument("--duration", type=float, help="override duration_s")
    run.add_argument("--message-bytes", type=int, help="override message_bytes")
    run.add_argument("--partitions", type=int, help="override topic partitions")
    run.add_argument("--bootstrap", help="override bootstrap.servers / service URL")
    run.add_argument("--repeat", type=int, default=1, help="run N times (variance checking)")
    run.add_argument(
        "--warmup-runs",
        type=int,
        default=0,
        help="discard runs before measuring; 3 recommended after a broker reset (ADR-0004)",
    )
    run.add_argument("--results-dir", default="results", help="where to write manifests")
    run.add_argument(
        "--no-fingerprint",
        action="store_true",
        help="skip environment capture (faster; makes the run unpublishable)",
    )
    run.set_defaults(func=_cmd_run)

    show = sub.add_parser("show", help="summarise a manifest")
    show.add_argument("manifest")
    show.set_defaults(func=_cmd_show)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result: int = args.func(args)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
