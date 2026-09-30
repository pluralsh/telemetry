"""Prometheus remote-write and OTLP protobuf fixtures."""

from __future__ import annotations

import snappy
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import Message
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    AggregationTemporality,
    ExponentialHistogram,
    ExponentialHistogramDataPoint,
    Gauge,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from .fixture import NativeHistogram, Series, Spans


def _remote_write_types() -> tuple[type[Message], type[Message]]:
    file = descriptor_pb2.FileDescriptorProto(
        name="prometheus_remote_write.proto",
        package="prometheus",
        syntax="proto3",
    )

    def message(name: str) -> descriptor_pb2.DescriptorProto:
        value = file.message_type.add()
        value.name = name
        return value

    def field(
        owner: descriptor_pb2.DescriptorProto,
        name: str,
        number: int,
        kind: int,
        *,
        repeated: bool = False,
        type_name: str = "",
    ) -> None:
        value = owner.field.add(
            name=name,
            number=number,
            type=kind,
            label=(
                descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
                if repeated
                else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
            ),
        )
        if type_name:
            value.type_name = type_name

    label = message("Label")
    field(label, "name", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    field(label, "value", 2, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    sample = message("Sample")
    field(sample, "value", 1, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE)
    field(sample, "timestamp", 2, descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    span = message("BucketSpan")
    field(span, "offset", 1, descriptor_pb2.FieldDescriptorProto.TYPE_SINT32)
    field(span, "length", 2, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32)
    # Field numbers follow prometheus/prompb/types.proto. The count and
    # zero_count oneofs are flattened to their integer members, which is
    # wire-identical for integer histograms.
    histogram = message("Histogram")
    for name, number, kind, repeated in (
        ("count_int", 1, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64, False),
        ("sum", 3, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, False),
        ("schema", 4, descriptor_pb2.FieldDescriptorProto.TYPE_SINT32, False),
        ("zero_threshold", 5, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, False),
        ("zero_count_int", 6, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64, False),
        ("negative_deltas", 9, descriptor_pb2.FieldDescriptorProto.TYPE_SINT64, True),
        ("positive_deltas", 12, descriptor_pb2.FieldDescriptorProto.TYPE_SINT64, True),
        ("reset_hint", 14, descriptor_pb2.FieldDescriptorProto.TYPE_INT32, False),
        ("timestamp", 15, descriptor_pb2.FieldDescriptorProto.TYPE_INT64, False),
        ("custom_values", 16, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, True),
    ):
        field(histogram, name, number, kind, repeated=repeated)
    for name, number in (("negative_spans", 8), ("positive_spans", 11)):
        field(
            histogram,
            name,
            number,
            descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
            repeated=True,
            type_name=".prometheus.BucketSpan",
        )
    timeseries = message("TimeSeries")
    field(
        timeseries,
        "labels",
        1,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        repeated=True,
        type_name=".prometheus.Label",
    )
    field(
        timeseries,
        "samples",
        2,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        repeated=True,
        type_name=".prometheus.Sample",
    )
    field(
        timeseries,
        "histograms",
        4,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        repeated=True,
        type_name=".prometheus.Histogram",
    )
    metadata = message("MetricMetadata")
    field(
        metadata,
        "metric_family_type",
        1,
        descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
    )
    field(
        metadata,
        "metric_family_name",
        2,
        descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    )
    field(metadata, "help", 4, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    field(metadata, "unit", 5, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    request = message("WriteRequest")
    field(
        request,
        "timeseries",
        1,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        repeated=True,
        type_name=".prometheus.TimeSeries",
    )
    field(
        request,
        "metadata",
        3,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        repeated=True,
        type_name=".prometheus.MetricMetadata",
    )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return (
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("prometheus.WriteRequest")
        ),
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("prometheus.TimeSeries")
        ),
    )


WriteRequest, TimeSeries = _remote_write_types()


def _spans_and_deltas(spans: Spans) -> tuple[list[dict[str, int]], list[int]]:
    wire_spans = []
    deltas = []
    previous = 0
    for offset, counts in spans:
        wire_spans.append({"offset": offset, "length": len(counts)})
        for count in counts:
            deltas.append(count - previous)
            previous = count
    return wire_spans, deltas


def _histogram(value: NativeHistogram) -> dict[str, object]:
    positive_spans, positive_deltas = _spans_and_deltas(value.positive)
    negative_spans, negative_deltas = _spans_and_deltas(value.negative)
    return {
        "count_int": value.count,
        "sum": value.sum,
        "schema": value.schema,
        "zero_threshold": value.zero_threshold,
        "zero_count_int": value.zero_count,
        "positive_spans": positive_spans,
        "positive_deltas": positive_deltas,
        "negative_spans": negative_spans,
        "negative_deltas": negative_deltas,
        "timestamp": value.timestamp_ms,
        "custom_values": list(value.custom_values),
    }


def remote_write_protobuf(values: tuple[Series, ...]) -> bytes:
    timeseries = [
        TimeSeries(
            labels=[{"name": name, "value": value} for name, value in item.labels],
            samples=[
                {"timestamp": sample.timestamp_ms, "value": sample.value}
                for sample in item.samples
            ],
            histograms=[_histogram(histogram) for histogram in item.histograms],
        )
        for item in values
    ]
    return WriteRequest(timeseries=timeseries).SerializeToString()


def remote_write_body(values: tuple[Series, ...]) -> bytes:
    return snappy.compress(remote_write_protobuf(values))


def decode_remote_write(body: bytes) -> Message:
    request = WriteRequest()
    request.ParseFromString(snappy.decompress(body))
    return request


def _kv(key: str, value: str) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(string_value=value))


def _otlp_request(metric: Metric) -> ExportMetricsServiceRequest:
    return ExportMetricsServiceRequest(
        resource_metrics=[
            ResourceMetrics(
                resource=Resource(attributes=[_kv("service.name", "regression")]),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="regression", version="1"),
                        metrics=[metric],
                    )
                ],
            )
        ]
    )


def otlp_fixture(timestamp_ms: int) -> ExportMetricsServiceRequest:
    return _otlp_request(
        Metric(
            name="otlp.regression.temperature",
            description="OTLP regression gauge",
            gauge=Gauge(
                data_points=[
                    NumberDataPoint(
                        attributes=[_kv("host", "host-a")],
                        time_unix_nano=timestamp_ms * 1_000_000,
                        as_double=42.5,
                    )
                ]
            ),
        )
    )


def otlp_exponential_histogram_fixture(
    timestamps_ms: tuple[int, ...],
) -> ExportMetricsServiceRequest:
    """Cumulative exponential histogram at scale 10.

    Scale 10 exceeds the native maximum of 8, so both receivers must merge
    four OTLP buckets into each native bucket. The zero threshold is left
    unset, as most SDKs do.
    """
    start_ns = (timestamps_ms[0] - 60_000) * 1_000_000
    points = []
    for index, timestamp in enumerate(timestamps_ms, start=1):
        buckets = [index, 0, 0, 0, 2 * index, index + 1, 0, 1, 3 * index]
        points.append(
            ExponentialHistogramDataPoint(
                attributes=[_kv("host", "host-a")],
                start_time_unix_nano=start_ns,
                time_unix_nano=timestamp * 1_000_000,
                count=index + sum(buckets),
                sum=1.25 * index,
                scale=10,
                zero_count=index,
                positive=ExponentialHistogramDataPoint.Buckets(
                    offset=3, bucket_counts=buckets
                ),
            )
        )
    return _otlp_request(
        Metric(
            name="otlp.regression.latency",
            description="OTLP regression exponential histogram",
            exponential_histogram=ExponentialHistogram(
                aggregation_temporality=(
                    AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                ),
                data_points=points,
            ),
        )
    )
