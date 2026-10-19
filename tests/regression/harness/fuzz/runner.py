"""Product-agnostic differential fuzz loop: load, verify visibility, query, record."""

from __future__ import annotations

import json
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..compose import ComposeProject
from .config import Budget, Endpoint, FuzzConfig, _flag
from .history import collect_images, history_dir, record_output
from .history.environment import collect_docker
from .rand import derive
from .recorder import CaseRecord, IngestRecord, Recorder, RoundRecord, Summary
from .resources import DockerApi, ResourceSampler, docker_socket
from .transport import SUCCESS, Exchange, Handler, Request, Transport

# How long a round the implementation never showed is polled further, to
# tell a late round from a lost one.
DIAGNOSE_WAIT_S = 120.0
DIAGNOSTIC_BODY_BYTES = 16384


def describe(exchange: Exchange) -> dict[str, object]:
    body = exchange.body[:DIAGNOSTIC_BODY_BYTES].decode("utf-8", "replace")
    return {**exchange.describe(), "body": body}


class Mismatch(Exception):
    """Both sides answered successfully but semantically differently."""


class Inconclusive(Exception):
    """The responses differ in a way the API contract allows (e.g. truncation)."""


Comparator = Callable[[Exchange, Exchange], None]


@dataclass(frozen=True)
class Case:
    family: str
    query: str
    request: Request
    compare: Comparator
    ok_statuses: Collection[int] = SUCCESS
    # Returns why an implementation error is a documented oracle quirk when
    # the oracle answered; the case is then inconclusive instead of failing.
    oracle_quirk: Callable[[Exchange, Exchange], str | None] | None = None
    # Mutating cases are never re-sent to recheck a mismatch or a slow answer.
    idempotent: bool = True


@dataclass(frozen=True)
class Batch:
    name: str
    oracle: Request
    impl: Request
    items: int


@dataclass(frozen=True)
class Probe:
    """Polled on both sides until `ready` holds, gating queries on a round."""

    name: str
    request: Request
    ready: Callable[[Exchange], bool]


@dataclass
class Round:
    index: int
    profile: str
    batches: list[Batch]
    probes: list[Probe]
    dataset: Any = None
    stats: dict[str, Any] = field(default_factory=dict)


class FuzzProduct(ABC):
    name: str
    # Data timestamps span this much history before each round's load time.
    default_window: str = "20m"
    # Compose service -> `impl` or `oracle` for resource sampling; unlisted
    # services such as object storage count as `shared`.
    resource_roles: dict[str, str] = {}
    # False when the oracle is an in-process model whose latency means nothing,
    # so reports leave out oracle latency and ratios.
    reference_latency: bool = True
    oracle_read: Endpoint
    oracle_write: Endpoint
    impl_read: Endpoint
    impl_write: Endpoint

    def __init__(self, config: FuzzConfig) -> None:
        self.config = config

    @abstractmethod
    def stack(self) -> AbstractContextManager[object]:
        """Start (or, for external stacks, merely wait for) both systems."""

    @abstractmethod
    def generate_round(self, index: int, rng: random.Random) -> Round:
        """Generate one randomized data round and add it to the query catalog."""

    @abstractmethod
    def next_case(self, rng: random.Random) -> Case:
        """Generate one randomized query against the data loaded so far."""

    def after_ingest(self, index: int) -> None:
        """Called once a round's writes are sent, before visibility polling."""
        return None

    def diagnose_invisible(
        self, index: int, probes: list[Probe], send: Callable[[Request], Exchange]
    ) -> dict[str, Exchange]:
        """Requests that narrow down why `probes` never held on the
        implementation, sent there through `send` and keyed by description."""
        return {}

    def floor_urls(self) -> dict[str, str]:
        """Compose service -> a trivial URL (readiness) timed every round."""
        return {}

    def handlers(self) -> dict[str, Handler]:
        """URL scheme -> sender for this product's non-HTTP endpoints."""
        return {}

    def describe(self) -> dict[str, object]:
        return {
            "oracle_read": self.oracle_read.describe(),
            "oracle_write": self.oracle_write.describe(),
            "impl_read": self.impl_read.describe(),
            "impl_write": self.impl_write.describe(),
        }


def classify(case: Case, oracle: Exchange, impl: Exchange) -> tuple[str, str | None]:
    oracle_ok = oracle.ok(case.ok_statuses)
    impl_ok = impl.ok(case.ok_statuses)
    if oracle_ok and impl_ok:
        try:
            case.compare(oracle, impl)
        except Inconclusive as error:
            return "inconclusive", str(error)
        except Mismatch as error:
            return "mismatch", str(error)
        except Exception as error:
            # Reused regression normalizers signal differences via assertions,
            # and unexpected response shapes surface as arbitrary exceptions;
            # either way one odd response must not abort a long run.
            return "mismatch", f"{type(error).__name__}: {error}"
        return "match", None
    if oracle_ok:
        if case.oracle_quirk and (reason := case.oracle_quirk(oracle, impl)):
            return "inconclusive", reason
        return ("impl_timeout" if impl.timed_out else "impl_error"), _error(impl)
    if impl_ok:
        return ("oracle_timeout" if oracle.timed_out else "oracle_error"), _error(
            oracle
        )
    return "both_error", f"oracle: {_error(oracle)}; impl: {_error(impl)}"


def _error(exchange: Exchange) -> str:
    if exchange.error:
        return exchange.error
    return f"HTTP {exchange.status}: {exchange.body[:500].decode(errors='replace')}"


def _stable(case: Case, first: Exchange, second: Exchange) -> bool:
    if first.ok(case.ok_statuses) != second.ok(case.ok_statuses):
        return False
    if not first.ok(case.ok_statuses):
        return True
    return classify(case, first, second)[0] in ("match", "inconclusive")


def _agrees(case: Case, oracle: Exchange, impl: Exchange) -> bool:
    return classify(case, oracle, impl)[0] in ("match", "inconclusive")


# Most oracle answers a mismatch collects before it counts the oracle stable.
# Only mismatches pay for them, so they leave latency measurements alone.
ORACLE_ANSWERS = 5


def settle_mismatch(
    case: Case,
    impl_answers: Sequence[Exchange],
    oracle_answers: Sequence[Exchange],
    ask_oracle: Callable[[], Exchange | None],
    limit: int = ORACLE_ANSWERS,
) -> tuple[str, str | None, list[Exchange]]:
    """Re-asks the oracle after a mismatch until two of its successful answers
    disagree or it has been asked `limit` times in all, and returns the
    outcome, an extra detail and the answers asked for here. A query whose
    result depends on an order the oracle leaves unspecified (Prometheus sums
    a binary operator's output in Go map order, so 1e308 + 1e308 + -Inf is NaN
    or -Inf from run to run) is `unstable_oracle` only while every
    implementation answer equals one of the oracle's; one equal to none of
    them stays a mismatch.
    `ask_oracle` returns None once no further request may be sent. A failed
    oracle answer is no answer: it neither shows instability nor excuses one."""
    answers = [answer for answer in oracle_answers if answer.ok(case.ok_statuses)]
    asked: list[Exchange] = []

    def disagree() -> bool:
        return any(
            not _stable(case, earlier, later)
            for index, later in enumerate(answers)
            for earlier in answers[:index]
        )

    while len(oracle_answers) + len(asked) < limit and not disagree():
        answer = ask_oracle()
        if answer is None:
            break
        asked.append(answer)
        if answer.ok(case.ok_statuses):
            answers.append(answer)
    if disagree():
        if all(
            any(_agrees(case, oracle, impl) for oracle in answers)
            for impl in impl_answers
        ):
            return (
                "unstable_oracle",
                f"the oracle gave {len(answers)} answers that disagree; the "
                "implementation's equals one of them",
                asked,
            )
        return (
            "mismatch",
            f"the oracle gave {len(answers)} answers that disagree, and the "
            "implementation's equals none of them",
            asked,
        )
    if any(not _stable(case, impl_answers[0], impl) for impl in impl_answers[1:]):
        return "unstable_impl", None, asked
    return "mismatch", None, asked


FLOOR_SAMPLES = 5
FLOOR_REQUEST = Request("GET", "")

SLOW_RATIO = 2.0
SLOW_FLOOR_MS = 150.0


def _slow(oracle: Exchange, impl: Exchange) -> bool:
    return impl.latency_ms > max(SLOW_FLOOR_MS, SLOW_RATIO * oracle.latency_ms)


class Runner:
    def __init__(
        self,
        product: FuzzProduct,
        config: FuzzConfig,
        sampler: ResourceSampler | None = None,
        layout: dict[str, Any] | None = None,
    ) -> None:
        self.product = product
        self.config = config
        self.sampler = sampler
        self.transport = Transport(
            timeout_s=config.request_timeout_s, handlers=product.handlers()
        )
        self.budget = Budget(config.duration_s)
        # Leave room to write the report even when a request is mid-flight.
        self.reserve = min(30.0, max(2.0, config.duration_s * 0.05))
        self.recorder = Recorder(
            config,
            product.describe(),
            layout=layout,
            reference_latency=product.reference_latency,
        )
        self.order = derive(config.seed, "order")
        self._last_progress = time.monotonic()

    def run(self) -> Summary:
        config = self.config
        print(
            f"fuzz {config.product}: seed={config.seed} run={config.run_id} "
            f"budget={config.duration_s:.0f}s output={config.output_dir}",
            flush=True,
        )
        cases = 0
        index = 0
        loaded = 0
        if self.sampler is not None:
            self.sampler.start()
        try:
            while not self._out_of_time():
                if not config.max_rounds or index < config.max_rounds:
                    if not self._load(index) and loaded == 0:
                        self.recorder.note(
                            "aborting: the first round never became visible on both "
                            "sides, so no query would be meaningful"
                        )
                        break
                    loaded += 1
                elif loaded == 0:
                    break
                rng = derive(config.seed, "queries", index)
                for _ in range(rng.randint(*config.queries_per_round)):
                    if self._out_of_time() or (
                        config.max_cases and cases >= config.max_cases
                    ):
                        break
                    self._execute(f"r{index:04d}-q{cases:06d}", index, rng)
                    cases += 1
                if config.max_cases and cases >= config.max_cases:
                    break
                index += 1
        finally:
            elapsed_s = self.budget.elapsed()
            resources = self.sampler.stop() if self.sampler is not None else None
            summary = self.recorder.finish(elapsed_s=elapsed_s, resources=resources)
        return summary

    def _out_of_time(self) -> bool:
        return self.budget.expired(self.reserve)

    def _load(self, index: int) -> bool:
        rng = derive(self.config.seed, "data", index)
        started = time.monotonic()
        generated = self.product.generate_round(index, rng)
        if self.config.record_data and generated.dataset is not None:
            self.recorder.record_dataset(index, generated.dataset)
        for batch in generated.batches:
            oracle = self.transport.send_with_retry(
                self.product.oracle_write, batch.oracle, budget=self.budget
            )
            impl = self.transport.send_with_retry(
                self.product.impl_write, batch.impl, budget=self.budget
            )
            self.recorder.record_ingest(
                IngestRecord(index, batch.name, batch.items, oracle, impl)
            )
            if self._out_of_time():
                break
        self.product.after_ingest(index)
        visible = {
            "oracle": self._wait(self.product.oracle_read, generated.probes),
            "impl": self._wait(self.product.impl_read, generated.probes),
        }
        if visible["oracle"] and not visible["impl"] and not self._out_of_time():
            self._diagnose(index, generated.probes)
        if all(visible.values()) and self.config.settle_s:
            time.sleep(min(self.config.settle_s, self.budget.remaining()))
        self.recorder.record_round(
            RoundRecord(
                index=index,
                profile=generated.profile,
                batches=len(generated.batches),
                stats=generated.stats,
                visible=visible,
                load_ms=(time.monotonic() - started) * 1000,
                truncated=not all(visible.values()) and self._out_of_time(),
            )
        )
        self._measure_floor()
        return all(visible.values())

    def _measure_floor(self) -> None:
        if self.config.stack == "external":
            return
        for service, url in self.product.floor_urls().items():
            role = self.product.resource_roles.get(service, "shared")
            endpoint = Endpoint(url)
            for _ in range(FLOOR_SAMPLES):
                exchange = self.transport.send(endpoint, FLOOR_REQUEST)
                self.recorder.record_floor(service, role, exchange)

    def _diagnose(self, index: int, probes: list[Probe]) -> None:
        """Records what the implementation serves for a round it never showed,
        then how much longer the round takes to appear, if it does."""

        def send(request: Request) -> Exchange:
            return self.transport.send(self.product.impl_read, request)

        diagnostics = self.product.diagnose_invisible(index, probes, send)
        started = time.monotonic()
        late = self._wait(self.product.impl_read, probes, DIAGNOSE_WAIT_S)
        self.recorder.record_diagnostic(
            index,
            {
                "requests": {
                    name: describe(exchange) for name, exchange in diagnostics.items()
                },
                "visible_after_s": round(time.monotonic() - started, 3)
                if late
                else None,
                "waited_s": round(time.monotonic() - started, 3),
            },
        )

    def _wait(
        self, endpoint: Endpoint, probes: list[Probe], timeout_s: float | None = None
    ) -> bool:
        deadline = time.monotonic() + min(
            self.config.visibility_timeout_s if timeout_s is None else timeout_s,
            self.budget.remaining(),
        )
        pending = list(probes)
        while pending:
            pending = [
                probe
                for probe in pending
                if not probe.ready(self.transport.send(endpoint, probe.request))
            ]
            if not pending:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.25)
        return True

    def _pair(self, case: Case) -> tuple[Exchange, Exchange]:
        read = self.transport.send
        # Alternate who goes first so neither side systematically benefits
        # from host-level warm caches or suffers from noisy neighbours.
        if self.order.random() < 0.5:
            oracle = read(self.product.oracle_read, case.request)
            impl = read(self.product.impl_read, case.request)
        else:
            impl = read(self.product.impl_read, case.request)
            oracle = read(self.product.oracle_read, case.request)
        return oracle, impl

    def _ask_oracle(self, case: Case) -> Callable[[], Exchange | None]:
        def ask() -> Exchange | None:
            if self._out_of_time():
                return None
            return self.transport.send(self.product.oracle_read, case.request)

        return ask

    def _execute(self, case_id: str, index: int, rng: random.Random) -> None:
        case = self.product.next_case(rng)
        oracle, impl = self._pair(case)
        outcome, detail = classify(case, oracle, impl)
        recheck = None
        oracle_rechecks: list[Exchange] = []
        recheck_allowed = self.config.recheck and case.idempotent
        if outcome == "mismatch" and recheck_allowed:
            oracle_again, impl_again = self._pair(case)
            recheck = (oracle_again, impl_again)
            outcome, extra, oracle_rechecks = settle_mismatch(
                case, (impl, impl_again), (oracle, oracle_again), self._ask_oracle(case)
            )
            if extra:
                detail = f"{detail}; {extra}"
        elif recheck_allowed and self.product.reference_latency and _slow(oracle, impl):
            # Tells transient stalls apart from consistently slow queries.
            recheck = self._pair(case)
        self.recorder.record_case(
            CaseRecord(
                id=case_id,
                round=index,
                family=case.family,
                query=case.query,
                request=case.request,
                outcome=outcome,
                detail=detail,
                oracle=oracle,
                impl=impl,
                recheck=recheck,
                oracle_rechecks=tuple(oracle_rechecks),
            )
        )
        if time.monotonic() - self._last_progress >= 60:
            self._last_progress = time.monotonic()
            counts = dict(self.recorder.outcomes)
            print(
                f"fuzz {self.config.product}: {self.budget.elapsed():.0f}s, "
                f"round {index}, {sum(counts.values())} cases {counts}",
                flush=True,
            )


def run_fuzz(product_type: type[FuzzProduct]) -> Summary:
    """Build config from the environment, bring up the stack, and fuzz it
    (or, in the `cold` scenario, run the cold battery; see `cold.py`).

    The budget covers fuzzing only; image builds and stack startup happen
    before it starts, so bound those with the CI job timeout instead.
    """
    config = FuzzConfig.from_env(
        product_type.name, default_window=product_type.default_window
    )
    product = product_type(config)
    root = history_dir()
    images: dict[str, Any] = {}
    with product.stack() as project:
        layout: dict[str, Any] = {"docker": collect_docker()}
        if isinstance(project, ComposeProject) and project.cpu_layout is not None:
            layout["cpus"] = project.cpu_layout.describe()
        sampler = _sampler(project, product, config)
        if config.cold is not None:
            from .cold import ColdRunner

            if not isinstance(project, ComposeProject):
                raise ValueError(
                    "FUZZ_SCENARIO=cold restarts services: FUZZ_STACK=compose"
                )
            summary = ColdRunner(product, config, project, sampler, layout).run()
        else:
            summary = Runner(product, config, sampler, layout=layout).run()
        if root is not None and isinstance(project, ComposeProject):
            images = collect_images(project.name)
    if root is not None:
        summary.history = _record_history(summary, root, images)
    return summary


def _record_history(
    summary: Summary, root: Path, images: dict[str, Any]
) -> Path | None:
    # A broken history directory must not turn a finished run into an error.
    try:
        path = record_output(summary.output_dir, root, images=images)
    except Exception as error:
        print(f"fuzz: could not record history in {root}: {error}", flush=True)
        return None
    print(f"fuzz: recorded history entry {path}", flush=True)
    return path


def _sampler(
    project: object, product: FuzzProduct, config: FuzzConfig
) -> ResourceSampler | None:
    if not isinstance(project, ComposeProject) or not _flag("FUZZ_RESOURCES", True):
        return None
    path = docker_socket()
    if path is None:
        return None
    config.output_dir.mkdir(parents=True, exist_ok=True)
    return ResourceSampler(
        DockerApi(path),
        project.name,
        product.resource_roles,
        config.output_dir / "resources.jsonl",
    )


def json_body(exchange: Exchange) -> Any:
    try:
        return exchange.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise Mismatch(f"response is not JSON: {error}") from error
