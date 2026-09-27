"""Live Track-versus-Tempo differential scenarios."""

from __future__ import annotations

import time
from dataclasses import dataclass

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from .auth import basic, bearer
from .client import Target, TraceClient
from .fixture import (
    JAEGER_TRACE_ID,
    TRACE_IDS,
    ZIPKIN_TRACE_ID,
    TraceFixture,
    assert_spans_writer_ranges,
    regression_fixture,
    zipkin_fixture,
)
from .normalize import (
    assert_trace_equivalent,
    normalize_search,
    normalize_tag_names,
    normalize_tag_values,
)
from .wire import jaeger_request, protobuf_body, zipkin_body

TRACEQL_CASES = (
    ("resource", '{ resource."service.name" = "checkout" }', set(TRACE_IDS)),
    ("typed-int", "{ span.http.status_code = 500 }", {TRACE_IDS[2]}),
    ("typed-bool", "{ span.regression.error = true }", {TRACE_IDS[2]}),
    ("status", "{ status = error }", {TRACE_IDS[2]}),
    ("duration", "{ duration >= 50ms }", set(TRACE_IDS)),
    ("aggregate", "{} | count() >= 2", set(TRACE_IDS)),
)


@dataclass
class TrackSuite:
    client: TraceClient
    tempo: Target
    tempo_otlp_http: Target
    tempo_zipkin: Target
    reader: Target
    writer_0: Target
    writer_1: Target
    fixture: TraceFixture

    @classmethod
    def create(cls) -> TrackSuite:
        now = time.time_ns()
        base_ns = (now // 1_000_000_000 - 30) * 1_000_000_000
        read_auth = basic("regression-reader", "regression-read")
        write_auth = bearer("regression-write")
        return cls(
            client=TraceClient(),
            tempo=Target("http://localhost:13200", "regression"),
            tempo_otlp_http=Target("http://localhost:14320", "regression"),
            tempo_zipkin=Target("http://localhost:19411", "regression"),
            reader=Target(
                "http://localhost:13203",
                "regression",
                read_auth,
                read_prefix="/read/ns/regression",
            ),
            writer_0=Target(
                "http://localhost:13201",
                "regression",
                write_auth,
                write_prefix="/write/ns/regression",
            ),
            writer_1=Target(
                "http://localhost:13202",
                "regression",
                write_auth,
                write_prefix="/write/ns/regression",
            ),
            fixture=regression_fixture(base_ns),
        )

    @property
    def start_seconds(self) -> int:
        return self.fixture.start_ns // 1_000_000_000 - 1

    @property
    def end_seconds(self) -> int:
        return self.fixture.end_ns // 1_000_000_000 + 2

    def seed(self) -> None:
        assert_spans_writer_ranges(self.fixture.trace_ids)
        resources = self.fixture.request.resource_spans
        http_request = ExportTraceServiceRequest(resource_spans=resources[:2])
        grpc_request = ExportTraceServiceRequest(resource_spans=resources[2:])
        body = protobuf_body(http_request)
        assert self.client.otlp_http(self.tempo_otlp_http, body).status == 200
        assert self.client.otlp_http(self.writer_0, body).status == 200
        self.client.otlp_grpc("localhost:14319", grpc_request, namespace="regression")
        self.client.otlp_grpc(
            "localhost:14317",
            grpc_request,
            namespace="regression",
            authorization=bearer("regression-write"),
        )
        zipkin = zipkin_body(zipkin_fixture(self.fixture.end_ns + 1_000_000_000))
        tempo_zipkin = self.client.zipkin(self.tempo_zipkin, zipkin)
        track_zipkin = self.client.zipkin(self.writer_1, zipkin)
        assert 200 <= tempo_zipkin.status < 300, tempo_zipkin.body
        assert track_zipkin.status == 202, track_zipkin.body
        jaeger = jaeger_request(JAEGER_TRACE_ID, self.fixture.end_ns + 1_500_000_000)
        self.client.jaeger_grpc("localhost:14253", jaeger, namespace="regression")
        self.client.jaeger_grpc(
            "localhost:14251",
            jaeger,
            namespace="regression",
            authorization=bearer("regression-write"),
        )
        self.client.wait_for_trace(self.tempo, TRACE_IDS[-1])
        self.client.wait_for_trace(self.tempo, ZIPKIN_TRACE_ID)
        self.client.wait_for_trace(self.tempo, JAEGER_TRACE_ID)
        self.wait_for_search(
            self.tempo,
            '{ resource."service.name" = "checkout" }',
            set(TRACE_IDS),
        )

    def wait_for_reader(self) -> None:
        self.client.wait_for_trace(self.reader, TRACE_IDS[-1])
        self.client.wait_for_trace(self.reader, ZIPKIN_TRACE_ID)
        self.client.wait_for_trace(self.reader, JAEGER_TRACE_ID)

    def compare_trace(self, trace_id: str) -> None:
        assert_trace_equivalent(
            self.client.wait_for_trace(self.tempo, trace_id),
            self.client.wait_for_trace(self.reader, trace_id),
        )

    def search(self, target: Target, query: str) -> dict[str, object]:
        return self.client.json(
            target,
            "/api/search",
            parameters=[
                ("q", query),
                ("start", str(self.start_seconds)),
                ("end", str(self.end_seconds)),
                ("limit", "20"),
            ],
        )

    def wait_for_search(
        self,
        target: Target,
        query: str,
        expected_ids: set[str],
        *,
        timeout: float = 20,
    ) -> tuple[tuple[object, ...], ...]:
        deadline = time.monotonic() + timeout
        result = ()
        while time.monotonic() < deadline:
            result = normalize_search(self.search(target, query))
            if {item[0] for item in result} == expected_ids:
                return result
            time.sleep(0.2)
        raise AssertionError(
            f"{target.base_url} search {query!r} returned {result!r}, "
            f"expected trace IDs {expected_ids!r}"
        )

    def compare_search(self, query: str, expected_ids: set[str]) -> None:
        tempo = self.wait_for_search(self.tempo, query, expected_ids)
        track = normalize_search(self.search(self.reader, query))
        assert {item[0] for item in track} == expected_ids
        assert track == tempo

    def compare_tags(self) -> None:
        tempo_names = dict(
            normalize_tag_names(self.client.json(self.tempo, "/api/v2/search/tags"))
        )
        track_names = dict(
            normalize_tag_names(self.client.json(self.reader, "/api/v2/search/tags"))
        )
        expected = {
            "resource": {
                "service.name",
                "deployment.environment",
                "service.instance.id",
            },
            "span": {
                "http.method",
                "http.status_code",
                "regression.error",
                "regression.ratio",
                "db.system",
                "db.rows",
            },
        }
        for scope, names in expected.items():
            assert names <= set(tempo_names[scope])
            assert names <= set(track_names[scope])
            assert set(track_names[scope]) & names == set(tempo_names[scope]) & names
        for name in ("http.status_code", "regression.error", "regression.ratio"):
            path = f"/api/v2/search/tag/span.{name}/values"
            track_values = normalize_tag_values(self.client.json(self.reader, path))
            tempo_values = normalize_tag_values(self.client.json(self.tempo, path))
            assert track_values == tempo_values, (
                f"{name}: Track {track_values!r}, Tempo {tempo_values!r}"
            )

    def check_auth_and_namespace_isolation(self) -> None:
        path = f"/api/v2/traces/{TRACE_IDS[0]}"
        unauthorized = self.client.request(
            "GET", self.reader, path, read=True, authorization=None
        )
        assert unauthorized.status == 401
        wrong_namespace = Target(
            self.reader.base_url,
            "other",
            bearer("other-read"),
            read_prefix="/read/ns/other",
        )
        assert (
            self.client.request("GET", wrong_namespace, path, read=True).status == 404
        )
        wrong_token = self.client.request(
            "GET",
            self.reader,
            path,
            read=True,
            authorization=bearer("other-read"),
        )
        assert wrong_token.status == 401

    def check_reader_rejects_writes(self) -> None:
        response = self.client.otlp_http(
            Target(
                self.reader.base_url,
                "regression",
                bearer("regression-write"),
                write_prefix="/write/ns/regression",
            ),
            protobuf_body(self.fixture.request),
        )
        assert response.status == 404
