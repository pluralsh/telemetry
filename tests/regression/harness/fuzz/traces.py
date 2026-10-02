"""Differential TraceQL fuzzing of Traces against Tempo."""

from __future__ import annotations

import os
import random
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import (
    ResourceSpans,
    ScopeSpans,
    Span,
    Status,
)

from ..compose import ComposeProject
from ..process import wait_http
from ..traces.auth import basic, bearer
from ..traces.normalize import (
    normalize_search,
    normalize_tag_names,
    normalize_tag_values,
    normalize_trace,
)
from ..traces.wire import protobuf_body
from .config import Endpoint, FuzzConfig, endpoint_from_env
from .rand import ODD_STRINGS, chance, quote, regex_escape, scaled, weighted, zipf
from .runner import (
    Batch,
    Case,
    FuzzProduct,
    Inconclusive,
    Mismatch,
    Probe,
    Round,
    json_body,
)
from .stack import fuzz_stack
from .transport import Exchange, Request

PRODUCT_DIR = Path(__file__).resolve().parents[2] / "products" / "traces"
SECOND = 1_000_000_000
SENTINELS = 16
BATCH_SPANS = 2000
MAX_LIMIT = 1000
# `tempo-s3` serves search and tag lookups from flushed blocks in MinIO
# instead of ingester memory, matching the Traces reader's read path.
ORACLE_CONFIGS = {"tempo": "tempo.yaml", "tempo-s3": "tempo-s3.yaml"}
# `production` uses the crate defaults for page size, segment duration and
# IO concurrency instead of the small values that exercise edge cases.
IMPL_CONFIG_DIRS = {"regression": ".", "production": "production"}
RESERVOIR = 5000

SERVICES = (
    "frontend",
    "checkout",
    "cart",
    "payments",
    "inventory",
    "shipping",
    "auth",
    "search",
)
OPERATIONS = (
    "GET /api/cart",
    "POST /api/checkout",
    "SELECT orders",
    "publish order",
    "consume order",
    "render",
    "authorize",
    "lookup",
)
ROUTES = ("/api/cart", "/api/checkout", "/api/items/{id}", "/login", "/healthz")
METHODS = ("GET", "POST", "PUT", "DELETE")
STATUS_CODES = (200, 201, 204, 400, 401, 404, 429, 500, 503)
DB_SYSTEMS = ("postgresql", "redis", "mysql")
ENVIRONMENTS = ("prod", "staging")
KINDS = {
    "unspecified": Span.SPAN_KIND_UNSPECIFIED,
    "internal": Span.SPAN_KIND_INTERNAL,
    "server": Span.SPAN_KIND_SERVER,
    "client": Span.SPAN_KIND_CLIENT,
    "producer": Span.SPAN_KIND_PRODUCER,
    "consumer": Span.SPAN_KIND_CONSUMER,
}
# Tag names the fuzzer generates; the tags API is compared only over these
# because Tempo also reports its own internal and intrinsic names.
GENERATED_TAGS = {
    "resource": {
        "service.name",
        "deployment.environment",
        "service.instance.id",
        "fuzz.run",
        "fuzz.round",
    },
    "span": {
        "http.method",
        "http.status_code",
        "http.route",
        "db.system",
        "db.rows",
        "fuzz.ratio",
        "fuzz.flag",
        "fuzz.tag",
        "fuzz.odd",
    },
}
LOW_CARDINALITY_TAGS = (
    ("span", "http.method"),
    ("span", "http.status_code"),
    ("span", "db.system"),
    ("span", "fuzz.flag"),
    ("resource", "service.name"),
    ("resource", "deployment.environment"),
)

PROFILES = (
    (3.0, "mixed"),
    (2.0, "wide"),
    (1.5, "deep"),
    (1.5, "fanout"),
    (1.0, "attributes"),
    (1.0, "errors"),
    (1.0, "split"),
)
# (traces low, traces high, spans/trace low, spans/trace high)
SHAPES = {
    "mixed": (10, 60, 1, 20),
    "wide": (50, 400, 1, 3),
    "deep": (2, 8, 10, 60),
    "fanout": (2, 6, 20, 200),
    "attributes": (10, 40, 3, 10),
    "errors": (10, 40, 2, 8),
    "split": (10, 40, 4, 20),
}


def _value(value: str | bool | int | float) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    if isinstance(value, int):
        return AnyValue(int_value=value)
    if isinstance(value, float):
        return AnyValue(double_value=value)
    return AnyValue(string_value=value)


def _attributes(values: dict[str, str | bool | int | float]) -> list[KeyValue]:
    return [KeyValue(key=key, value=_value(value)) for key, value in values.items()]


def format_duration(nanoseconds: int, rng: random.Random) -> str:
    """TraceQL duration literal, sometimes exact to probe boundary handling."""
    if chance(rng, 0.2) or nanoseconds < 1000:
        return f"{nanoseconds}ns"
    for unit, size in (("s", SECOND), ("ms", 1_000_000), ("us", 1000)):
        if nanoseconds >= size:
            return f"{round(nanoseconds / size)}{unit}"
    return f"{nanoseconds}ns"


@dataclass
class Catalog:
    services: set[str] = field(default_factory=set)
    names: set[str] = field(default_factory=set)
    tags: list[str] = field(default_factory=list)
    durations: list[int] = field(default_factory=list)
    trace_ids: list[str] = field(default_factory=list)
    rounds: int = 0
    latest_end_ns: int = 0

    def remember(self, values: list, value: object, rng: random.Random) -> None:
        if len(values) < RESERVOIR:
            values.append(value)
        else:
            values[rng.randrange(RESERVOIR)] = value


def _normalize_search(response: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    """Tempo strips leading zeros from trace IDs and reports whole-millisecond
    durations, so compare at that precision."""
    return tuple(
        sorted(
            (
                trace_id.rjust(32, "0"),
                root_service,
                root_name,
                start,
                duration // 1_000_000,
            )
            for trace_id, root_service, root_name, start, duration in (
                normalize_search(response)
            )
        )
    )


def search_comparator(limit: int):
    def compare(oracle: Exchange, impl: Exchange) -> None:
        expected = _normalize_search(json_body(oracle))
        actual = _normalize_search(json_body(impl))
        if expected == actual:
            return
        oracle_ids = {item[0] for item in expected}
        impl_ids = {item[0] for item in actual}
        summary = (
            f"oracle {len(expected)} traces, impl {len(actual)}; "
            f"only oracle {sorted(oracle_ids - impl_ids)[:5]}, "
            f"only impl {sorted(impl_ids - oracle_ids)[:5]}"
        )
        if len(expected) >= limit or len(actual) >= limit:
            raise Inconclusive(
                f"limit={limit} reached; selection may differ: {summary}"
            )
        if oracle_ids == impl_ids:
            differing = next(
                pair
                for pair in zip(expected, actual, strict=True)
                if pair[0] != pair[1]
            )
            summary = (
                f"same traces, different metadata: {differing[0]} != {differing[1]}"
            )
        raise Mismatch(summary)

    return compare


def _trace_found(exchange: Exchange) -> bool:
    # Tempo's v2 endpoint answers an unknown ID with 200 and `{"trace": {}}`.
    if exchange.status != 200:
        return False
    body = json_body(exchange)
    return not isinstance(body, dict) or bool(body.get("trace", True))


def trace_comparator(oracle: Exchange, impl: Exchange) -> None:
    found = _trace_found(oracle)
    if found != _trace_found(impl):
        raise Mismatch(f"status {oracle.status} != {impl.status}")
    if not found:
        return
    expected = normalize_trace(json_body(oracle))
    actual = normalize_trace(json_body(impl))
    if expected == actual:
        return
    differing = next(
        (pair for pair in zip(expected, actual, strict=False) if pair[0] != pair[1]),
        None,
    )
    raise Mismatch(
        f"oracle {len(expected)} spans, impl {len(actual)}; first difference "
        f"{differing!r}"
    )


# Tempo answers tag discovery per block, and its live/head blocks cover every
# recent span whatever the requested range; Traces narrows to its segments.
# For windows that cut into the data, Traces must return a subset of Tempo.


def tag_names_comparator(*, partial: bool):
    def compare(oracle: Exchange, impl: Exchange) -> None:
        expected = dict(normalize_tag_names(json_body(oracle)))
        actual = dict(normalize_tag_names(json_body(impl)))
        for scope, generated in GENERATED_TAGS.items():
            left = set(expected.get(scope, ())) & generated
            right = set(actual.get(scope, ())) & generated
            if right <= left if partial else right == left:
                continue
            if scope == "span" and right - left == {"http.route"} and left <= right:
                raise Inconclusive(
                    "Tempo leaves span http.route out of tag names for backend "
                    "blocks, though searches on it match"
                )
            raise Mismatch(
                f"{scope} tags: oracle {sorted(left)} != impl {sorted(right)}"
            )

    return compare


def tag_values_comparator(*, partial: bool):
    def compare(oracle: Exchange, impl: Exchange) -> None:
        expected = normalize_tag_values(json_body(oracle))
        actual = normalize_tag_values(json_body(impl))
        if set(actual) <= set(expected) if partial else actual == expected:
            return
        raise Mismatch(f"oracle {expected!r} != impl {actual!r}")

    return compare


class TracesFuzz(FuzzProduct):
    name = "traces"
    default_window = "10m"
    resource_roles = {
        "tempo": "oracle",
        "traces-writer-0": "impl",
        "traces-writer-1": "impl",
        "traces-reader": "impl",
    }

    def __init__(self, config: FuzzConfig) -> None:
        super().__init__(config)
        tenant = os.environ.get("FUZZ_TRACES_NAMESPACE", "regression")
        scope = (("X-Scope-OrgID", tenant),)
        expires_from = int(time.time() + config.duration_s)
        self.oracle_read = endpoint_from_env(
            "traces", "oracle_read", Endpoint("http://localhost:13200", headers=scope)
        )
        self.oracle_write = endpoint_from_env(
            "traces", "oracle_write", Endpoint("http://localhost:14320", headers=scope)
        )
        self.impl_write = endpoint_from_env(
            "traces",
            "impl_write",
            Endpoint(
                f"http://localhost:13201/write/ns/{tenant}",
                bearer("regression-write", now=expires_from),
                scope,
            ),
        )
        self.impl_read = endpoint_from_env(
            "traces",
            "impl_read",
            Endpoint(
                f"http://localhost:13203/read/ns/{tenant}",
                basic("regression-reader", "regression-read"),
                scope,
            ),
        )
        # Tempo keeps re-sent spans until a trace's block is cut and
        # compacted, so its answer changes over time; Traces deduplicates at
        # query time. Opt-in only.
        self.duplicate_rate = float(os.environ.get("FUZZ_TRACES_DUPLICATE_RATE", "0"))
        self.window_ns = int(config.window_s * SECOND)
        self.catalog = Catalog()
        self.oracle = os.environ.get("FUZZ_TRACES_ORACLE") or "tempo"
        if self.oracle not in ORACLE_CONFIGS:
            raise ValueError(
                f"FUZZ_TRACES_ORACLE must be one of {tuple(ORACLE_CONFIGS)}"
            )
        self.impl_config = os.environ.get("FUZZ_TRACES_CONFIG") or "regression"
        if self.impl_config not in IMPL_CONFIG_DIRS:
            raise ValueError(
                f"FUZZ_TRACES_CONFIG must be one of {tuple(IMPL_CONFIG_DIRS)}"
            )
        self._project: ComposeProject | None = None
        self._reader_started = False

    def describe(self) -> dict[str, object]:
        return {
            "oracle": self.oracle,
            "impl_config": self.impl_config,
            **super().describe(),
        }

    def stack(self) -> AbstractContextManager[object]:
        # Tempo < 2.9 returns nothing for `{A} !> {B}` when no span matches A;
        # 2.9 made negated structural operators keep B, which Traces follows.
        os.environ.setdefault("TEMPO_IMAGE", "grafana/tempo:2.10.8")
        os.environ["TEMPO_CONFIG"] = ORACLE_CONFIGS[self.oracle]
        os.environ["TRACES_CONFIG_DIR"] = IMPL_CONFIG_DIRS[self.impl_config]

        @contextmanager
        def managed() -> Iterator[object]:
            with fuzz_stack(
                self.config,
                lambda: ComposeProject(
                    file=PRODUCT_DIR / "docker-compose.yml",
                    name="traces-fuzz",
                    services=(
                        "tempo",
                        "minio",
                        "minio-init",
                        "traces-writer-0",
                        "traces-writer-1",
                    ),
                    readiness_urls=(
                        "http://localhost:13200/ready",
                        "http://localhost:13201/-/ready",
                        "http://localhost:13202/-/ready",
                    ),
                ),
            ) as project:
                self._project = project
                yield project

        return managed()

    def after_ingest(self, index: int) -> None:
        # Mirrors the regression suite: the reader starts once writers have
        # created shard state.
        if self._project is not None and not self._reader_started:
            self._project.up("traces-reader")
            wait_http("http://localhost:13203/-/ready")
            self._reader_started = True

    # Data generation ---------------------------------------------------------

    def generate_round(self, index: int, rng: random.Random) -> Round:
        end = (time.time_ns() // SECOND - 2) * SECOND
        start = end - self.window_ns
        profile = weighted(rng, PROFILES)
        low, high, spans_low, spans_high = SHAPES[profile]
        pieces: list[ResourceSpans] = []
        retries: list[ResourceSpans] = []
        span_count = 0
        traces = scaled(rng, low, high, self.config.scale)
        for _ in range(traces):
            trace_id = rng.randbytes(16)
            self.catalog.remember(self.catalog.trace_ids, trace_id.hex(), rng)
            spans = self._trace(
                rng, profile, trace_id, start, end, spans_low, spans_high
            )
            span_count += len(spans)
            trace_pieces, trace_retries = self._resources(rng, index, profile, spans)
            pieces.extend(trace_pieces)
            retries.extend(trace_retries)
        sentinel_resource = {
            "service.name": "fuzz-sentinel",
            "fuzz.run": self.config.run_id,
            "fuzz.round": index,
        }
        sentinels = []
        for number in range(SENTINELS):
            span = Span(
                trace_id=rng.randbytes(16),
                span_id=rng.randbytes(8),
                name=f"sentinel-{number}",
                kind=Span.SPAN_KIND_SERVER,
                start_time_unix_nano=end - SECOND,
                end_time_unix_nano=end - SECOND + 1_000_000,
            )
            sentinels.append(self._resource(sentinel_resource, [span]))
        rng.shuffle(pieces)
        # Retries go out after the originals in their own requests, as a
        # client re-sending a failed export would.
        batches = (
            self._batches(pieces, "data")
            + self._batches(retries, "retry")
            + self._batches(sentinels, "sentinel")
        )
        self.catalog.rounds = index + 1
        self.catalog.latest_end_ns = end
        query = (
            '{ resource.service.name = "fuzz-sentinel" && '
            f"resource.fuzz.run = {quote(self.config.run_id)} && "
            f"resource.fuzz.round = {index} }}"
        )

        def ready(exchange: Exchange) -> bool:
            if not exchange.ok():
                return False
            try:
                return len(exchange.json().get("traces", ())) >= SENTINELS
            except (ValueError, AttributeError):
                return False

        probe = Probe(
            "sentinel",
            Request(
                "GET",
                "/api/search",
                (
                    ("q", query),
                    ("start", str(start // SECOND - 60)),
                    ("end", str(end // SECOND + 60)),
                    ("limit", "100"),
                ),
            ),
            ready,
        )
        dataset = None
        if self.config.record_data:
            dataset = {
                "window_ns": [start, end],
                "requests": [
                    MessageToDict(ExportTraceServiceRequest(resource_spans=[piece]))
                    for piece in pieces + retries
                ],
            }
        return Round(
            index=index,
            profile=profile,
            batches=batches,
            probes=[probe],
            dataset=dataset,
            stats={"traces": traces, "spans": span_count, "pieces": len(pieces)},
        )

    def _batches(self, pieces: list[ResourceSpans], prefix: str) -> list[Batch]:
        batches: list[Batch] = []
        current: list[ResourceSpans] = []
        spans = 0

        def flush() -> None:
            body = protobuf_body(ExportTraceServiceRequest(resource_spans=current))
            request = Request(
                "POST",
                "/v1/traces",
                body=body,
                headers=(("Content-Type", "application/x-protobuf"),),
            )
            batches.append(Batch(f"{prefix}-{len(batches)}", request, request, spans))

        for piece in pieces:
            size = sum(len(scope.spans) for scope in piece.scope_spans)
            if current and spans + size > BATCH_SPANS:
                flush()
                current, spans = [], 0
            current.append(piece)
            spans += size
        if current:
            flush()
        return batches

    def _duration(self, rng: random.Random) -> int:
        shape = weighted(rng, [(0.02, "zero"), (0.95, "normal"), (0.03, "huge")])
        if shape == "zero":
            return 0
        if shape == "huge":
            return rng.randint(5, 30) * SECOND
        return max(1, int(rng.lognormvariate(3, 1.5) * 1_000_000))

    def _trace(
        self,
        rng: random.Random,
        profile: str,
        trace_id: bytes,
        start: int,
        end: int,
        low: int,
        high: int,
    ) -> list[tuple[str, Span]]:
        count = scaled(rng, low, high, self.config.scale)
        root_service = zipf(rng, SERVICES)
        root_start = rng.randint(start, end - 40 * SECOND)
        root = self._span(
            rng,
            profile,
            trace_id,
            b"",
            root_start,
            self._duration(rng),
            Span.SPAN_KIND_SERVER,
        )
        spans = [(root_service, root)]
        for _ in range(count - 1):
            if profile == "deep":
                parent_service, parent = spans[-1]
            elif profile == "fanout":
                parent_service, parent = spans[0]
            else:
                parent_service, parent = rng.choice(spans)
            parent_duration = parent.end_time_unix_nano - parent.start_time_unix_nano
            child_start = parent.start_time_unix_nano + rng.randint(0, parent_duration)
            child_duration = rng.randint(0, parent.end_time_unix_nano - child_start)
            if chance(rng, 0.05):
                # Clock skew: children escaping their parent's interval.
                child_start -= rng.randint(0, SECOND)
                child_duration += rng.randint(0, SECOND)
            service = parent_service if chance(rng, 0.6) else zipf(rng, SERVICES)
            kind = rng.choice(list(KINDS.values()))
            spans.append(
                (
                    service,
                    self._span(
                        rng,
                        profile,
                        trace_id,
                        parent.span_id,
                        child_start,
                        child_duration,
                        kind,
                    ),
                )
            )
        return spans

    def _span(
        self,
        rng: random.Random,
        profile: str,
        trace_id: bytes,
        parent_id: bytes,
        start: int,
        duration: int,
        kind: int,
    ) -> Span:
        name = zipf(rng, OPERATIONS)
        self.catalog.names.add(name)
        self.catalog.remember(self.catalog.durations, duration, rng)
        attributes: dict[str, str | bool | int | float] = {}
        if kind == Span.SPAN_KIND_SERVER or chance(rng, 0.2):
            attributes["http.method"] = zipf(rng, METHODS)
            attributes["http.status_code"] = zipf(rng, STATUS_CODES)
            attributes["http.route"] = zipf(rng, ROUTES)
        if kind == Span.SPAN_KIND_CLIENT and chance(rng, 0.7):
            attributes["db.system"] = zipf(rng, DB_SYSTEMS)
            attributes["db.rows"] = rng.randint(0, 5000)
        rich = profile == "attributes"
        if chance(rng, 0.9 if rich else 0.5):
            attributes["fuzz.ratio"] = round(rng.uniform(-1, 2), rng.choice((1, 3, 6)))
        if chance(rng, 0.9 if rich else 0.5):
            attributes["fuzz.flag"] = chance(rng, 0.5)
        if chance(rng, 0.9 if rich else 0.3):
            tag = f"t{rng.randrange(1000)}"
            attributes["fuzz.tag"] = tag
            self.catalog.remember(self.catalog.tags, tag, rng)
        if chance(rng, 0.5 if rich else 0.05):
            attributes["fuzz.odd"] = rng.choice(ODD_STRINGS)
        error_rate = 0.5 if profile == "errors" else 0.1
        status = weighted(
            rng,
            [
                (1 - error_rate - 0.25, Status.STATUS_CODE_UNSET),
                (0.25, Status.STATUS_CODE_OK),
                (error_rate, Status.STATUS_CODE_ERROR),
            ],
        )
        events = [
            Span.Event(
                time_unix_nano=start + rng.randint(0, max(duration, 1)),
                name=rng.choice(("retry", "cache.miss", "exception")),
                attributes=_attributes({"event.sequence": sequence}),
            )
            for sequence in range(weighted(rng, [(6, 0), (2, 1), (1, 2)]))
        ]
        return Span(
            trace_id=trace_id,
            span_id=rng.randbytes(8),
            parent_span_id=parent_id,
            name=name,
            kind=kind,
            start_time_unix_nano=start,
            end_time_unix_nano=start + duration,
            attributes=_attributes(attributes),
            events=events,
            status=Status(
                code=status,
                message="boom" if status == Status.STATUS_CODE_ERROR else "",
            ),
        )

    def _resource(self, attributes: dict, spans: list[Span]) -> ResourceSpans:
        return ResourceSpans(
            resource=Resource(attributes=_attributes(attributes)),
            scope_spans=[
                ScopeSpans(
                    scope=InstrumentationScope(name="fuzz", version="1"), spans=spans
                )
            ],
        )

    def _resources(
        self,
        rng: random.Random,
        index: int,
        profile: str,
        spans: list[tuple[str, Span]],
    ) -> tuple[list[ResourceSpans], list[ResourceSpans]]:
        """Returns the trace's pieces and the retried copies to re-send."""
        by_service: dict[str, list[Span]] = {}
        resent: dict[str, list[Span]] = {}
        for service, span in spans:
            self.catalog.services.add(service)
            by_service.setdefault(service, []).append(span)
            if self.duplicate_rate and chance(rng, self.duplicate_rate):
                resent.setdefault(service, []).append(span)
        pieces = []
        retries = []
        for service, members in by_service.items():
            attributes = {
                "service.name": service,
                "deployment.environment": rng.choice(ENVIRONMENTS),
                "service.instance.id": f"{service}-{rng.randrange(8)}",
                "fuzz.run": self.config.run_id,
                "fuzz.round": index,
            }
            # Split deliveries spread one trace over several requests, which
            # both sides must stitch back together.
            if profile == "split" and len(members) > 1:
                cut = rng.randint(1, len(members) - 1)
                pieces.append(self._resource(attributes, members[:cut]))
                pieces.append(self._resource(attributes, members[cut:]))
            else:
                pieces.append(self._resource(attributes, members))
            if service in resent:
                retries.append(self._resource(attributes, resent[service]))
        return pieces, retries

    # Query generation --------------------------------------------------------

    def next_case(self, rng: random.Random) -> Case:
        isolated = 1.0 if self.config.isolated else 0.0
        kind = weighted(
            rng,
            [
                (8, "search"),
                (2, "trace"),
                (0.5 * isolated, "tag_names"),
                (0.5 * isolated, "tag_values"),
            ],
        )
        start, end = self._time_range(rng)
        window = (("start", str(start)), ("end", str(end)))
        partial = end - start < self.window_ns // SECOND
        if kind == "search":
            query, family = self._query(rng)
            limit = rng.randint(1, 20) if chance(rng, 0.15) else 500
            return Case(
                f"traceql.{family}",
                query,
                Request(
                    "GET", "/api/search", (("q", query), *window, ("limit", str(limit)))
                ),
                search_comparator(limit),
            )
        if kind == "trace":
            known = self.catalog.trace_ids and chance(rng, 0.9)
            trace_id = (
                rng.choice(self.catalog.trace_ids) if known else rng.randbytes(16).hex()
            )
            return Case(
                "trace.by_id",
                trace_id,
                Request("GET", f"/api/v2/traces/{trace_id}"),
                trace_comparator,
                ok_statuses=frozenset({200, 404}),
            )
        if kind == "tag_names":
            scope = rng.choice(("", "span", "resource"))
            params = (*window, *((("scope", scope),) if scope else ()))
            return Case(
                "tags.names",
                scope or "all",
                Request("GET", "/api/v2/search/tags", params),
                tag_names_comparator(partial=partial),
            )
        scope, tag = rng.choice(LOW_CARDINALITY_TAGS)
        return Case(
            "tags.values",
            f"{scope}.{tag}",
            Request("GET", f"/api/v2/search/tag/{scope}.{tag}/values", window),
            tag_values_comparator(partial=partial),
        )

    def _time_range(self, rng: random.Random) -> tuple[int, int]:
        end = self.catalog.latest_end_ns // SECOND
        window = self.window_ns // SECOND
        if chance(rng, 0.75):
            return end - window * 3 // 2, end + 60
        low = rng.randint(end - window, end - 1)
        return low, rng.randint(low + 1, end + 60)

    def _scope(self, rng: random.Random) -> str:
        scope = f"resource.fuzz.run = {quote(self.config.run_id)}"
        if self.catalog.rounds and chance(rng, 0.5):
            scope += f" && resource.fuzz.round = {rng.randrange(self.catalog.rounds)}"
        return scope

    def _spanset(self, rng: random.Random) -> str:
        scope = self._scope(rng)
        if chance(rng, 0.1):
            return f"{{ {scope} }}"
        return f"{{ {scope} && ({self._condition(rng, 0)}) }}"

    def _condition(self, rng: random.Random, depth: int) -> str:
        if depth < 2 and chance(rng, 0.3):
            joiner = rng.choice(("&&", "||"))
            left = self._condition(rng, depth + 1)
            right = self._condition(rng, depth + 1)
            return f"({left}) {joiner} ({right})"
        return self._comparison(rng)

    def _threshold(self, rng: random.Random) -> str:
        durations = self.catalog.durations or [50_000_000]
        return format_duration(rng.choice(durations), rng)

    def _comparison(self, rng: random.Random) -> str:
        services = sorted(self.catalog.services) or list(SERVICES)
        names = sorted(self.catalog.names) or list(OPERATIONS)
        order = rng.choice((">", ">=", "<", "<="))
        form = weighted(
            rng,
            [
                (2, "name"),
                (1.5, "status"),
                (1, "kind"),
                (2, "duration"),
                (0.5, "trace_duration"),
                (1, "root"),
                (2, "status_code"),
                (1, "method"),
                (1, "route"),
                (1, "rows"),
                (1, "ratio"),
                (1, "flag"),
                (0.5, "tag"),
                (2, "service"),
                (0.5, "environment"),
                (0.3, "nil"),
                (0.2, "missing"),
                (0.3, "status_message"),
                (0.3, "arithmetic"),
            ],
        )
        if form == "name":
            op = weighted(rng, [(3, "="), (1, "!="), (1, "=~")])
            name = rng.choice(names)
            value = regex_escape(name.split(" ")[0]) + ".*" if op == "=~" else name
            return f"name {op} {quote(value)}"
        if form == "status":
            status = rng.choice(("error", "ok", "unset"))
            return f"status {rng.choice(('=', '!='))} {status}"
        if form == "kind":
            return f"kind {rng.choice(('=', '!='))} {rng.choice(list(KINDS))}"
        if form == "duration":
            field_name = rng.choice(("duration", "span:duration"))
            return f"{field_name} {order} {self._threshold(rng)}"
        if form == "trace_duration":
            field_name = rng.choice(("traceDuration", "trace:duration"))
            return f"{field_name} {order} {self._threshold(rng)}"
        if form == "root":
            if chance(rng, 0.5):
                field_name = rng.choice(("rootServiceName", "trace:rootService"))
                return f"{field_name} = {quote(rng.choice(services))}"
            field_name = rng.choice(("rootName", "trace:rootName"))
            return f"{field_name} = {quote(rng.choice(names))}"
        if form == "status_code":
            op = rng.choice(("=", "!=", ">", ">=", "<", "<="))
            return f"span.http.status_code {op} {rng.choice(STATUS_CODES)}"
        if form == "method":
            if chance(rng, 0.3):
                return 'span.http.method =~ "P.*"'
            return f"span.http.method = {quote(rng.choice(METHODS))}"
        if form == "route":
            return f".http.route = {quote(rng.choice(ROUTES))}"
        if form == "rows":
            return f"span.db.rows {order} {rng.randint(0, 5000)}"
        if form == "ratio":
            return f"span.fuzz.ratio {order} {round(rng.uniform(-1, 2), 2)}"
        if form == "flag":
            return f"span.fuzz.flag = {rng.choice(('true', 'false'))}"
        if form == "tag":
            tag = rng.choice(self.catalog.tags) if self.catalog.tags else "t1"
            return f"span.fuzz.tag = {quote(tag)}"
        if form == "service":
            op = weighted(rng, [(3, "="), (1, "!="), (1, "=~")])
            service = rng.choice(services)
            value = regex_escape(service[:3]) + ".*" if op == "=~" else service
            return f"resource.service.name {op} {quote(value)}"
        if form == "environment":
            return (
                f"resource.deployment.environment = {quote(rng.choice(ENVIRONMENTS))}"
            )
        if form == "nil":
            # Tempo's grammar only accepts `!= nil`.
            return "span.db.system != nil"
        if form == "missing":
            return 'span.does.not.exist = "x"'
        if form == "status_message":
            return 'statusMessage = "boom"'
        return f"span.db.rows * 2 {order} {rng.randint(0, 10000)}"

    def _query(self, rng: random.Random) -> tuple[str, str]:
        form = weighted(
            rng,
            [
                (6, "spanset"),
                (3, "structural"),
                (2, "aggregate"),
                (0.7, "by"),
                (0.5, "select"),
                (0.7, "pipeline"),
            ],
        )
        first = self._spanset(rng)
        if form == "spanset":
            return first, "spanset"
        if form == "structural":
            operator = weighted(
                rng,
                [
                    (2, "&&"),
                    (2, "||"),
                    (1.5, ">>"),
                    (1, ">"),
                    (1, "~"),
                    (0.5, "<<"),
                    (0.5, "<"),
                    (0.3, "!>>"),
                    (0.3, "!>"),
                    (0.2, "&>>"),
                ],
            )
            return f"{first} {operator} {self._spanset(rng)}", "structural"
        if form == "aggregate":
            aggregate, threshold = weighted(
                rng,
                [
                    (3, ("count()", str(rng.randint(1, 10)))),
                    (1, ("avg(duration)", self._threshold(rng))),
                    (1, ("max(duration)", self._threshold(rng))),
                    (1, ("min(duration)", self._threshold(rng))),
                    (0.5, ("sum(span.db.rows)", str(rng.randint(0, 20000)))),
                    (0.5, ("max(span.db.rows)", str(rng.randint(0, 5000)))),
                    # Tempo reads a missing dedicated column (http.status_code)
                    # as the string "nil", which sorts above every int, so max
                    # and avg over it are wrong upstream; min is unaffected.
                    (
                        0.5,
                        ("min(span.http.status_code)", str(rng.choice(STATUS_CODES))),
                    ),
                ],
            )
            order = rng.choice((">", ">=", "<", "<=", "="))
            return f"{first} | {aggregate} {order} {threshold}", "aggregate"
        if form == "by":
            group = rng.choice(("resource.service.name", "span.http.method", "status"))
            tail = rng.choice(
                ("", f" | count() > {rng.randint(1, 5)}", " | coalesce()")
            )
            return f"{first} | by({group}){tail}", "by"
        if form == "select":
            return f"{first} | select(span.http.method, status, duration)", "select"
        return f"{first} | {self._spanset(rng)}", "pipeline"
