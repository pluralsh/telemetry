"""Reusable Meter regression scenarios for pytest and Kubernetes handoffs."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

from .auth import basic, bearer
from .client import PrometheusClient, Target
from .fixture import (
    assert_spans_writer_ranges,
    classic_histogram_fixture,
    native_histogram_fixture,
    regression_fixture,
    series,
)
from .normalize import (
    assert_json_data_equivalent,
    assert_metadata_equivalent,
    assert_query_equivalent,
    ensure_success,
    result_len,
)
from .wire import (
    otlp_explicit_histogram_fixture,
    otlp_exponential_histogram_fixture,
    otlp_fixture,
    remote_write_body,
)

INSTANT_QUERIES = (
    ("instant selector", "regression_gauge", ()),
    ("regex selector", 'regression_gauge{instance=~"a|b"}', ()),
    ("sum", "sum(regression_gauge)", ()),
    ("grouped aggregate", "sum by (job) (regression_gauge)", ()),
    ("counter rate", "rate(regression_counter_total[5m])", ("__name__",)),
    ("offset", "regression_gauge offset 2m", ()),
    (
        "binary join",
        "regression_left + on(instance) group_left(zone) regression_right",
        (),
    ),
    ("empty", 'regression_gauge{instance="missing"}', ()),
)

RANGE_QUERIES = (
    ("range selector", "regression_gauge", ()),
    ("range aggregate", "sum by (job) (regression_gauge)", ()),
    ("range rate", "rate(regression_counter_total[5m])", ("__name__",)),
)

OTLP_IGNORED_LABELS = ("otel_scope_name", "otel_scope_version", "job")

NATIVE_INSTANT_QUERIES = (
    ("native selector", "regression_native_seconds", ()),
    ("native rate", "rate(regression_native_seconds[5m])", ("__name__",)),
    ("native increase", "increase(regression_native_seconds[5m])", ("__name__",)),
    ("native sum", "sum by (job) (rate(regression_native_seconds[5m]))", ()),
    (
        "native histogram_count",
        "histogram_count(rate(regression_native_seconds[5m]))",
        ("__name__",),
    ),
    (
        "native histogram_sum",
        "histogram_sum(regression_native_seconds)",
        ("__name__",),
    ),
    (
        "native histogram_avg",
        "histogram_avg(rate(regression_native_seconds[5m]))",
        ("__name__",),
    ),
    (
        "native histogram_quantile",
        "histogram_quantile(0.9, rate(regression_native_seconds[5m]))",
        ("__name__",),
    ),
    (
        "native histogram_fraction",
        "histogram_fraction(0, 2, regression_native_seconds)",
        (),
    ),
    ("custom buckets selector", "regression_nhcb_seconds", ()),
    (
        "custom buckets quantile",
        "histogram_quantile(0.5, rate(regression_nhcb_seconds[5m]))",
        ("__name__",),
    ),
    ("otlp exponential selector", "otlp_regression_latency", OTLP_IGNORED_LABELS),
    (
        "otlp exponential quantile",
        "histogram_quantile(0.5, otlp_regression_latency)",
        OTLP_IGNORED_LABELS,
    ),
)

NATIVE_RANGE_QUERIES = (
    ("native range selector", "regression_native_seconds", ()),
    ("native range sum", "sum(rate(regression_native_seconds[5m]))", ()),
    (
        "native range quantile",
        "histogram_quantile(0.9, rate(regression_native_seconds[5m]))",
        ("__name__",),
    ),
)

CLASSIC_INSTANT_QUERIES = (
    ("classic bucket selector", "regression_classic_seconds_bucket", ()),
    (
        "classic bucket rate",
        "rate(regression_classic_seconds_bucket[5m])",
        ("__name__",),
    ),
    (
        "classic quantile",
        "histogram_quantile(0.9, rate(regression_classic_seconds_bucket[5m]))",
        ("__name__",),
    ),
    (
        "classic aggregated quantile",
        "histogram_quantile(0.5, sum by (le) "
        "(rate(regression_classic_seconds_bucket[5m])))",
        (),
    ),
    (
        "classic average",
        "rate(regression_classic_seconds_sum[5m]) "
        "/ rate(regression_classic_seconds_count[5m])",
        (),
    ),
    ("otlp explicit buckets", "otlp_regression_duration_bucket", OTLP_IGNORED_LABELS),
    ("otlp explicit count", "otlp_regression_duration_count", OTLP_IGNORED_LABELS),
    (
        "otlp explicit quantile",
        "histogram_quantile(0.5, otlp_regression_duration_bucket)",
        (*OTLP_IGNORED_LABELS, "__name__"),
    ),
)

CLASSIC_RANGE_QUERIES = (
    ("classic range buckets", "regression_classic_seconds_bucket", ()),
    (
        "classic range quantile",
        "histogram_quantile(0.9, rate(regression_classic_seconds_bucket[5m]))",
        ("__name__",),
    ),
)

DISCOVERY_QUERIES = (
    ("labels", "/api/v1/labels"),
    ("label values", "/api/v1/label/instance/values"),
    ("series", "/api/v1/series"),
)


@dataclass(frozen=True)
class AuthCase:
    name: str
    authorization: str | None
    expected_status: int


@dataclass
class MeterSuite:
    prometheus: Target
    writer: Target
    reader: Target
    base_ms: int
    client: PrometheusClient

    @classmethod
    def from_env(cls) -> MeterSuite:
        now_ms = time.time_ns() // 1_000_000
        base_ms = int(
            os.getenv(
                "REGRESSION_BASE_MS",
                str(now_ms // 60_000 * 60_000 - 600_000),
            )
        )
        return cls(
            prometheus=Target(os.getenv("PROMETHEUS_URL", "http://127.0.0.1:19090")),
            writer=Target(
                os.getenv(
                    "METER_WRITE_URL",
                    "http://127.0.0.1:18080/write/ns/regression",
                ),
                bearer("regression-write"),
            ),
            reader=Target(
                os.getenv(
                    "METER_READ_URL",
                    "http://127.0.0.1:18082/read/ns/regression",
                ),
                basic("regression-reader", "regression-read"),
            ),
            base_ms=base_ms,
            client=PrometheusClient(),
        )

    @property
    def end_seconds(self) -> str:
        return str((self.base_ms + 540_000) / 1000)

    def fixture_body(self) -> bytes:
        value = regression_fixture(self.base_ms)
        assert_spans_writer_ranges(value)
        return remote_write_body(value)

    def seed(self) -> None:
        body = self.fixture_body()
        prometheus = self.client.remote_write(self.prometheus, body)
        assert 200 <= prometheus.status < 300, "Prometheus remote write failed"
        for _ in range(2):
            meter = self.client.remote_write(
                self.writer,
                body,
                request_id="regression-idempotent",
            )
            assert 200 <= meter.status < 300, (
                f"Meter remote write failed: {meter.status} {meter.body!r}"
            )
        self.wait_for_metric(
            'regression_gauge{instance="a"}',
            timeout=60,
            error="Meter reader did not observe durable writer data within 60s",
        )
        self.seed_native_histograms()
        self.seed_classic_histograms()

    def seed_classic_histograms(self) -> None:
        body = remote_write_body(classic_histogram_fixture(self.base_ms))
        for name, target in (("Prometheus", self.prometheus), ("Meter", self.writer)):
            response = self.client.remote_write(
                target, body, request_id="regression-classic"
            )
            assert 200 <= response.status < 300, (
                f"{name} classic histogram write failed: "
                f"{response.status} {response.body!r}"
            )
        otlp = otlp_explicit_histogram_fixture(
            tuple(self.base_ms + index * 60_000 for index in range(5, 10))
        ).SerializeToString()
        for name, target, path in (
            ("Prometheus", self.prometheus, "/api/v1/otlp/v1/metrics"),
            ("Meter", self.writer, "/v1/metrics"),
        ):
            response = self.client.otlp_write(
                target, otlp, path=path, request_id="regression-otlp-classic"
            )
            assert 200 <= response.status < 300, (
                f"{name} OTLP explicit histogram write failed: "
                f"{response.status} {response.body!r}"
            )
        for metric in (
            "regression_classic_seconds_bucket",
            "otlp_regression_duration_bucket",
        ):
            self.wait_for_metric(metric, timeout=60)

    def seed_native_histograms(self) -> None:
        body = remote_write_body(native_histogram_fixture(self.base_ms))
        for name, target in (("Prometheus", self.prometheus), ("Meter", self.writer)):
            response = self.client.remote_write(
                target, body, request_id="regression-native"
            )
            assert 200 <= response.status < 300, (
                f"{name} native histogram write failed: "
                f"{response.status} {response.body!r}"
            )
        otlp = otlp_exponential_histogram_fixture(
            tuple(self.base_ms + index * 60_000 for index in range(5, 10))
        ).SerializeToString()
        for name, target, path in (
            ("Prometheus", self.prometheus, "/api/v1/otlp/v1/metrics"),
            ("Meter", self.writer, "/v1/metrics"),
        ):
            response = self.client.otlp_write(
                target, otlp, path=path, request_id="regression-otlp-histogram"
            )
            assert 200 <= response.status < 300, (
                f"{name} OTLP histogram write failed: "
                f"{response.status} {response.body!r}"
            )
        for metric in (
            "regression_native_seconds",
            "regression_nhcb_seconds",
            "otlp_regression_latency",
        ):
            self.wait_for_metric(metric, timeout=60)

    def wait_for_metric(
        self,
        metric: str,
        *,
        timeout: float,
        error: str | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                value = self.client.query(
                    self.reader,
                    "/api/v1/query",
                    [("query", metric), ("time", self.end_seconds)],
                )
                if result_len(value):
                    return
            except (AssertionError, OSError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError(error or f"reader freshness timed out for {metric}")
            time.sleep(1)

    def compare_instant(
        self,
        name: str,
        expression: str,
        ignored_labels: tuple[str, ...],
    ) -> None:
        if name == "boundary":
            expression = f"regression_gauge @ {self.base_ms / 1000}"
        parameters = [("query", expression), ("time", self.end_seconds)]
        expected = self.client.query(self.prometheus, "/api/v1/query", parameters)
        actual = self.client.query(self.reader, "/api/v1/query", parameters)
        assert_query_equivalent(
            name,
            expected,
            actual,
            ignored_labels=ignored_labels,
        )

    def compare_range(
        self,
        name: str,
        expression: str,
        ignored_labels: tuple[str, ...],
    ) -> None:
        parameters = [
            ("query", expression),
            ("start", str(self.base_ms / 1000)),
            ("end", self.end_seconds),
            ("step", "60"),
        ]
        expected = self.client.query(self.prometheus, "/api/v1/query_range", parameters)
        actual = self.client.query(self.reader, "/api/v1/query_range", parameters)
        assert_query_equivalent(
            name,
            expected,
            actual,
            ignored_labels=ignored_labels,
        )

    def compare_discovery(self, name: str, path: str) -> None:
        parameters = [
            ("match[]", "regression_gauge"),
            ("start", str(self.base_ms / 1000)),
            ("end", self.end_seconds),
        ]
        expected = self.client.query(self.prometheus, path, parameters)
        actual = self.client.query(self.reader, path, parameters)
        assert_json_data_equivalent(name, expected, actual)

    def check_remote_write_metadata_endpoints(self) -> None:
        parameters = [("metric", "regression_gauge")]
        for name, target in (
            ("Prometheus metadata", self.prometheus),
            ("Meter metadata", self.reader),
        ):
            ensure_success(
                name,
                self.client.query(target, "/api/v1/metadata", parameters),
            )

    def check_otlp(self) -> None:
        body = otlp_fixture(self.base_ms + 540_000).SerializeToString()
        meter = self.client.otlp_write(
            self.writer,
            body,
            request_id="regression-otlp",
        )
        assert 200 <= meter.status < 300, (
            f"OTLP write failed: {meter.status} {meter.body!r}"
        )
        prometheus = self.client.otlp_write(
            self.prometheus,
            body,
            path="/api/v1/otlp/v1/metrics",
        )
        assert 200 <= prometheus.status < 300, (
            f"Prometheus OTLP write failed: {prometheus.status} {prometheus.body!r}"
        )
        self.wait_for_metric("otlp_regression_temperature", timeout=60)
        parameters = [
            ("query", "otlp_regression_temperature"),
            ("time", self.end_seconds),
        ]
        expected = self.client.query(self.prometheus, "/api/v1/query", parameters)
        actual = self.client.query(self.reader, "/api/v1/query", parameters)
        assert_query_equivalent(
            "OTLP equivalent samples",
            expected,
            actual,
            ignored_labels=OTLP_IGNORED_LABELS,
        )
        metadata_parameters = [("metric", "otlp_regression_temperature")]
        ensure_success(
            "Prometheus OTLP metadata endpoint",
            self.client.query(
                self.prometheus,
                "/api/v1/metadata",
                metadata_parameters,
            ),
        )
        meter_metadata = self.client.query(
            self.reader,
            "/api/v1/metadata",
            metadata_parameters,
        )
        expected_metadata = {
            "status": "success",
            "data": {
                "otlp_regression_temperature": [
                    {
                        "type": "gauge",
                        "help": "OTLP regression gauge",
                        "unit": "",
                    }
                ]
            },
        }
        assert_metadata_equivalent(
            "OTLP metadata",
            expected_metadata,
            meter_metadata,
        )

    def check_namespace_isolation(self) -> None:
        other_writer = Target(
            self.writer.base_url.replace("/regression", "/other"),
            bearer("other-write"),
        )
        body = remote_write_body(
            (
                series(
                    "namespace_private",
                    (("tenant", "other"),),
                    ((self.base_ms + 540_000, 1.0),),
                ),
            )
        )
        response = self.client.remote_write(
            other_writer,
            body,
            request_id="other-write",
        )
        assert 200 <= response.status < 300
        parameters = [
            ("query", "namespace_private"),
            ("time", self.end_seconds),
        ]
        own = self.client.query(self.reader, "/api/v1/query", parameters)
        assert result_len(own) == 0, "namespace data leaked into regression"
        other_reader = Target(
            self.reader.base_url.replace("/regression", "/other"),
            bearer("other-read"),
        )
        denied = self.client.request(
            "GET",
            other_reader,
            "/api/v1/query",
            parameters=parameters,
            authorization=self.reader.authorization,
        )
        assert denied.status == 401, "cross-tenant credential unexpectedly authorized"

    def read_auth_cases(self) -> tuple[AuthCase, ...]:
        return (
            AuthCase(
                "namespace Basic read",
                basic("regression-reader", "regression-read"),
                200,
            ),
            AuthCase("namespace JWT read", bearer("regression-read-token"), 200),
            AuthCase("global JWT read", bearer("global-read"), 200),
            AuthCase("write JWT on read", bearer("regression-write"), 200),
            AuthCase("other namespace JWT read", bearer("other-read"), 401),
            AuthCase(
                "invalid Basic read",
                basic("regression-reader", "wrong"),
                401,
            ),
            AuthCase("missing read credential", None, 401),
        )

    def write_auth_cases(self) -> tuple[AuthCase, ...]:
        return (
            AuthCase("namespace JWT write", bearer("regression-write"), 200),
            AuthCase("global JWT write", bearer("global-write"), 200),
            AuthCase("read JWT on write", bearer("regression-read-token"), 401),
            AuthCase("other namespace JWT write", bearer("other-write"), 401),
            AuthCase(
                "read Basic on write",
                basic("regression-reader", "regression-read"),
                401,
            ),
            AuthCase("missing write credential", None, 401),
        )

    def check_read_auth(self, case: AuthCase) -> None:
        response = self.client.request(
            "GET",
            self.reader,
            "/api/v1/query",
            parameters=[
                ("query", "regression_gauge"),
                ("time", self.end_seconds),
            ],
            authorization=case.authorization,
        )
        if case.expected_status == 200:
            assert 200 <= response.status < 300, (
                f"{case.name}: expected success, got {response.status}"
            )
            return
        assert response.status == case.expected_status, (
            f"{case.name}: expected {case.expected_status}, got {response.status}"
        )

    def check_write_auth(self, case: AuthCase) -> None:
        response = self.client.remote_write(
            self.writer,
            self.fixture_body(),
            request_id=f"auth-{case.name.replace(' ', '-')}",
            authorization=case.authorization,
        )
        if case.expected_status == 200:
            assert 200 <= response.status < 300, (
                f"{case.name}: expected success, got {response.status}"
            )
            return
        assert response.status == case.expected_status, (
            f"{case.name}: expected {case.expected_status}, got {response.status}"
        )

    def check_reader_rejects_writes(self) -> None:
        response = self.client.remote_write(
            self.reader,
            self.fixture_body(),
            authorization=bearer("global-write"),
        )
        assert response.status in (404, 405), "read-only reader accepted a write"

    def run_meter_only(self, request_id: str) -> None:
        response = self.client.remote_write(
            self.writer,
            self.fixture_body(),
            request_id=request_id,
        )
        assert 200 <= response.status < 300, (
            f"Meter remote write failed: {response.status} {response.body!r}"
        )
        self.wait_for_metric(
            'regression_gauge{instance="a"}',
            timeout=60,
            error="Meter reader did not observe durable writer data within 60s",
        )


def run_meter_only_from_env() -> None:
    suite = MeterSuite.from_env()
    request_id = os.getenv(
        "REGRESSION_RUN_ID",
        f"meter-only-{suite.base_ms}",
    )
    suite.run_meter_only(request_id)
    print("Meter-only forwarding/read check passed")
