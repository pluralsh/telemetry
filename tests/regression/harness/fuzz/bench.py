"""Run the fuzz benchmark for several products in sequence and record history:

PYTHONPATH=tests/regression python -m harness.fuzz.bench --duration 30m

With `--scenarios recent,historical` every product runs once per scenario.
Recent-scenario metrics runs once per `--metrics-oracles` entry (Prometheus and
Mimir by default); historical metrics always compares against `mimir-blocks`.
Implementations run with `--config production` (what the operator deploys) by
default; `--config regression` uses the small pages, segments and caches that
exercise boundaries, for a quick correctness pass rather than a benchmark.

Every run gets its own process and Compose stack, so product-specific
environment changes cannot leak between runs. With the default `--lanes 1` runs
go one at a time. `--lanes N` runs N at once, each pinned to its own disjoint
slice of the Docker CPUs (`FUZZ_CPU_RANGE`), never two runs of the same product
together (they share Compose project names, volumes and ports), and, in the
runner container, each in its own runner container so service names never
resolve across stacks. Images are built once before the first lane starts so
no build overlaps a measurement. Lanes share memory and disk, so leave
headroom for the largest pair. Every other `FUZZ_*`
variable is passed through unchanged. A failing run does not stop the others;
the exit status is non-zero if any failed.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .config import REPO_ROOT, SCENARIOS, parse_duration
from .cpus import RANGE_ENV, docker_cpus, lane_ranges
from .history import BATCH_ENV, DEFAULT_DIR, HISTORY_ENV, PRODUCTS, rebuild
from .metrics import ORACLES as METRICS_ORACLES
from .network import IN_NETWORK_ENV, in_network
from .resources import DockerApi, docker_socket

PRODUCTS_DIR = REPO_ROOT / "tests" / "regression" / "products"
RUNNER_COMPOSE = PRODUCTS_DIR / "runner" / "docker-compose.yml"
FORWARDED_PREFIXES = ("FUZZ_", "REGRESSION_", "MIMIR_", "TEMPO_")
POLL_S = 1.0


@dataclass(frozen=True)
class Plan:
    products: tuple[str, ...]
    scenarios: tuple[str, ...]
    history: Path
    output_root: Path
    batch: str
    duration: str | None
    seed: str | None
    cooldown_s: float
    impl_config: str = "production"
    metrics_oracles: tuple[str, ...] = ("prometheus",)
    lanes: int = 1


IMPL_CONFIGS = ("production", "regression", "production-sharded")
# Logs only deploys standalone.
SHARDED_PRODUCTS = ("metrics", "traces")
DEFAULT_METRICS_ORACLES = ("prometheus", "mimir")


@dataclass(frozen=True)
class Job:
    product: str
    scenario: str
    oracle: str | None = None

    @property
    def label(self) -> str:
        oracle = f"/{self.oracle}" if self.oracle else ""
        return f"{self.product}{oracle} ({self.scenario})"


@dataclass(frozen=True)
class Outcome:
    product: str
    scenario: str
    returncode: int
    elapsed_s: float
    output_dir: Path
    oracle: str | None = None

    @property
    def status(self) -> str:
        return {0: "passed", 1: "failed"}.get(self.returncode, "error")


def _products(value: str) -> tuple[str, ...]:
    products = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = sorted(set(products) - set(PRODUCTS))
    if not products or unknown:
        raise argparse.ArgumentTypeError(
            f"products must be a comma-separated subset of {','.join(PRODUCTS)}"
        )
    return products


def _scenarios(value: str) -> tuple[str, ...]:
    scenarios = tuple(item.strip() for item in value.split(",") if item.strip())
    if not scenarios or set(scenarios) - set(SCENARIOS):
        raise argparse.ArgumentTypeError(
            f"scenarios must be a comma-separated subset of {','.join(SCENARIOS)}"
        )
    return scenarios


def _metrics_oracles(value: str) -> tuple[str, ...]:
    oracles = tuple(item.strip() for item in value.split(",") if item.strip())
    if not oracles or set(oracles) - set(METRICS_ORACLES):
        raise argparse.ArgumentTypeError(
            f"metrics oracles must be a comma-separated subset of "
            f"{','.join(METRICS_ORACLES)}"
        )
    return oracles


def _duration(value: str) -> str:
    parse_duration(value)
    return value


def parse_plan(argv: Sequence[str], environ: Mapping[str, str]) -> Plan:
    parser = argparse.ArgumentParser(prog="python -m harness.fuzz.bench")
    parser.add_argument(
        "--products",
        type=_products,
        default=PRODUCTS,
        help="comma-separated products, run in this order (default: all)",
    )
    parser.add_argument(
        "--scenarios",
        type=_scenarios,
        help="comma-separated scenarios run for every product (default: "
        "FUZZ_SCENARIO, then recent)",
    )
    parser.add_argument(
        "--duration",
        type=_duration,
        help="FUZZ_DURATION per product; defaults to the environment, then 30m",
    )
    parser.add_argument(
        "--seed", help="FUZZ_SEED shared by every product (default: random each)"
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        help=f"history root; defaults to {HISTORY_ENV}, then documentation/"
        "benchmarks/fuzz",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="parent of each product's FUZZ_OUTPUT_DIR (default: "
        "target/fuzz/bench-<batch>)",
    )
    parser.add_argument(
        "--cooldown",
        type=parse_duration,
        default=10.0,
        help="pause between products so the previous stack's teardown settles",
    )
    parser.add_argument(
        "--config",
        choices=IMPL_CONFIGS,
        default="production",
        help="implementation configs for every product, overriding "
        "FUZZ_<PRODUCT>_CONFIG (default: production)",
    )
    parser.add_argument(
        "--metrics-oracles",
        type=_metrics_oracles,
        help="oracles recent-scenario metrics runs against, one run each "
        "(default: FUZZ_METRICS_ORACLE, then prometheus,mimir)",
    )
    parser.add_argument(
        "--lanes",
        type=int,
        default=1,
        help="concurrent runs, each pinned to its own slice of the Docker CPUs "
        "(default: 1)",
    )
    args = parser.parse_args(argv)
    if args.lanes < 1:
        parser.error("--lanes must be at least 1")
    if args.config == "production-sharded" and (
        unsharded := sorted(set(args.products) - set(SHARDED_PRODUCTS))
    ):
        parser.error(f"--config production-sharded does not apply to {unsharded}")
    batch = environ.get(BATCH_ENV) or time.strftime(
        "bench-%Y%m%dT%H%M%SZ", time.gmtime()
    )
    history = args.history_dir or Path(environ.get(HISTORY_ENV) or DEFAULT_DIR)
    if not history.is_absolute():
        history = REPO_ROOT / history
    return Plan(
        products=args.products,
        scenarios=args.scenarios or (environ.get("FUZZ_SCENARIO") or "recent",),
        history=history,
        output_root=args.output_root or REPO_ROOT / "target" / "fuzz" / batch,
        batch=batch,
        duration=args.duration,
        seed=args.seed,
        cooldown_s=args.cooldown,
        impl_config=args.config,
        metrics_oracles=args.metrics_oracles
        or (
            (environ["FUZZ_METRICS_ORACLE"],)
            if environ.get("FUZZ_METRICS_ORACLE")
            else DEFAULT_METRICS_ORACLES
        ),
        lanes=args.lanes,
    )


def jobs(plan: Plan) -> list[Job]:
    """Scenario-major, in `--products` order."""
    planned = []
    for scenario in plan.scenarios:
        for product in plan.products:
            if product == "metrics" and scenario == "recent":
                planned += [Job(product, scenario, o) for o in plan.metrics_oracles]
            else:
                planned.append(Job(product, scenario))
    return planned


def output_dir(plan: Plan, job: Job) -> Path:
    name = job.product
    if len(plan.scenarios) > 1:
        name += f"-{job.scenario}"
    if job.oracle and len(plan.metrics_oracles) > 1:
        name += f"-{job.oracle}"
    return plan.output_root / name


def product_environment(
    plan: Plan, job: Job, environ: Mapping[str, str]
) -> dict[str, str]:
    env = dict(environ)
    pythonpath = str(REPO_ROOT / "tests" / "regression")
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (pythonpath, environ.get("PYTHONPATH")) if part
    )
    env[HISTORY_ENV] = str(plan.history)
    env[BATCH_ENV] = plan.batch
    env["FUZZ_OUTPUT_DIR"] = str(output_dir(plan, job))
    env["FUZZ_SCENARIO"] = job.scenario
    env[f"FUZZ_{job.product.upper()}_CONFIG"] = plan.impl_config
    env.pop("FUZZ_RUN_ID", None)
    if job.product == "metrics":
        if job.oracle:
            env["FUZZ_METRICS_ORACLE"] = job.oracle
        else:
            env.pop("FUZZ_METRICS_ORACLE", None)
    if plan.duration:
        env["FUZZ_DURATION"] = plan.duration
    if plan.seed:
        env["FUZZ_SEED"] = plan.seed
    return env


def command(job: Job, env: Mapping[str, str], *, sibling: bool) -> list[str]:
    """`sibling` starts the run in a fresh runner container, so it joins only
    its own stack's network."""
    module = ["-m", "harness.fuzz", job.product]
    if not sibling:
        return [sys.executable, *module]
    forward = [
        flag
        for name in sorted(env)
        if name.startswith(FORWARDED_PREFIXES) and name != IN_NETWORK_ENV
        for flag in ("--env", name)
    ]
    return [
        "docker",
        "compose",
        "--file",
        str(RUNNER_COMPOSE),
        "--profile",
        "bench",
        "run",
        "--rm",
        *forward,
        "fuzz-runner",
        "python",
        *module,
    ]


def prebuild(plan: Plan, environ: Mapping[str, str]) -> None:
    """Build every image up front; lanes then start with `REGRESSION_BUILD=0`."""
    env = {**environ, "REPO_ROOT": str(REPO_ROOT)}
    files = [PRODUCTS_DIR / product / "docker-compose.yml" for product in plan.products]
    if in_network():
        files.append(RUNNER_COMPOSE)
    for file in files:
        print(f"bench {plan.batch}: building {file.relative_to(REPO_ROOT)}", flush=True)
        subprocess.run(
            ["docker", "compose", "--file", str(file), "--profile", "*", "build"],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )


def run(plan: Plan, environ: Mapping[str, str]) -> list[Outcome]:
    planned = jobs(plan)
    print(
        f"bench {plan.batch}: {', '.join(job.label for job in planned)} with "
        f"{plan.impl_config} configs in {plan.lanes} lane(s); "
        f"history {plan.history}; output {plan.output_root}",
        flush=True,
    )
    if plan.lanes == 1:
        outcomes = _sequential(plan, planned, environ)
    else:
        outcomes = _parallel(plan, planned, environ)
    rebuild(plan.history)
    return outcomes


def _sequential(
    plan: Plan, planned: Sequence[Job], environ: Mapping[str, str]
) -> list[Outcome]:
    outcomes = []
    for number, job in enumerate(planned):
        if number and plan.cooldown_s:
            time.sleep(plan.cooldown_s)
        started = time.monotonic()
        print(f"bench {plan.batch}: starting {job.label}", flush=True)
        env = product_environment(plan, job, environ)
        returncode = subprocess.run(
            command(job, env, sibling=False), cwd=REPO_ROOT, env=env, check=False
        ).returncode
        outcomes.append(_outcome(plan, job, returncode, started))
    return outcomes


@dataclass
class _Running:
    job: Job
    lane: int
    process: subprocess.Popen[bytes]
    started: float
    log: Path


def _parallel(
    plan: Plan, planned: Sequence[Job], environ: Mapping[str, str]
) -> list[Outcome]:
    socket_path = docker_socket()
    cpus = docker_cpus(DockerApi(socket_path)) if socket_path else None
    if cpus is None:
        raise SystemExit("--lanes needs the Docker socket to split CPUs")
    ranges = lane_ranges(cpus, plan.lanes)
    if environ.get("REGRESSION_BUILD", "1") != "0":
        prebuild(plan, environ)
    pending = list(planned)
    free = list(range(plan.lanes))
    idle_since = dict.fromkeys(free, 0.0)
    running: list[_Running] = []
    finished: dict[Job, Outcome] = {}
    try:
        _schedule(plan, pending, free, idle_since, running, finished, ranges, environ)
    except BaseException:
        for leftover in running:
            leftover.process.terminate()
        for leftover in running:
            leftover.process.wait()
        raise
    return [finished[job] for job in planned]


def _schedule(
    plan: Plan,
    pending: list[Job],
    free: list[int],
    idle_since: dict[int, float],
    running: list[_Running],
    finished: dict[Job, Outcome],
    ranges: Sequence[str],
    environ: Mapping[str, str],
) -> None:
    sibling = in_network()
    while pending or running:
        ready = [lane for lane in free if time.monotonic() >= idle_since[lane]]
        for lane in ready:
            busy = {r.job.product for r in running}
            job = next((j for j in pending if j.product not in busy), None)
            if job is None:
                break
            free.remove(lane)
            pending.remove(job)
            running.append(_start(plan, job, lane, ranges[lane], environ, sibling))
        time.sleep(POLL_S)
        for done in [r for r in running if r.process.poll() is not None]:
            running.remove(done)
            done.process.wait()
            finished[done.job] = _outcome(
                plan, done.job, done.process.returncode, done.started
            )
            print(
                f"bench {plan.batch}: {done.job.label} {finished[done.job].status} "
                f"in lane {done.lane}; log {done.log}",
                flush=True,
            )
            free.append(done.lane)
            idle_since[done.lane] = time.monotonic() + plan.cooldown_s


def _start(
    plan: Plan,
    job: Job,
    lane: int,
    cpu_range: str,
    environ: Mapping[str, str],
    sibling: bool,
) -> _Running:
    env = product_environment(plan, job, environ)
    env[RANGE_ENV] = cpu_range
    env["REGRESSION_BUILD"] = "0"
    env["REPO_ROOT"] = str(REPO_ROOT)
    log = output_dir(plan, job).with_suffix(".log")
    log.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"bench {plan.batch}: starting {job.label} in lane {lane} "
        f"(cpus {cpu_range}); log {log}",
        flush=True,
    )
    with log.open("wb") as sink:
        process = subprocess.Popen(
            command(job, env, sibling=sibling),
            cwd=REPO_ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=sink,
            stderr=subprocess.STDOUT,
        )
    return _Running(job, lane, process, time.monotonic(), log)


def _outcome(plan: Plan, job: Job, returncode: int, started: float) -> Outcome:
    return Outcome(
        job.product,
        job.scenario,
        returncode,
        time.monotonic() - started,
        output_dir(plan, job),
        job.oracle,
    )


def report(plan: Plan, outcomes: Sequence[Outcome]) -> str:
    lines = [f"bench {plan.batch} finished; history index {plan.history / 'README.md'}"]
    for outcome in outcomes:
        oracle = f"/{outcome.oracle}" if outcome.oracle else ""
        lines.append(
            f"  {outcome.product}{oracle} ({outcome.scenario}): {outcome.status} "
            f"(exit {outcome.returncode}, "
            f"{outcome.elapsed_s:.0f}s incl. stack startup) "
            f"{outcome.output_dir / 'summary.md'}"
        )
    return "\n".join(lines)


def main(argv: Sequence[str]) -> int:
    plan = parse_plan(argv, os.environ)
    outcomes = run(plan, os.environ)
    print(report(plan, outcomes), flush=True)
    return 0 if all(outcome.returncode == 0 for outcome in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
