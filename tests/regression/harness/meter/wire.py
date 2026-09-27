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
    Gauge,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from .fixture import Series


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


def remote_write_protobuf(values: tuple[Series, ...]) -> bytes:
    timeseries = [
        TimeSeries(
            labels=[{"name": name, "value": value} for name, value in item.labels],
            samples=[
                {"timestamp": sample.timestamp_ms, "value": sample.value}
                for sample in item.samples
            ],
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


def otlp_fixture(timestamp_ms: int) -> ExportMetricsServiceRequest:
    return ExportMetricsServiceRequest(
        resource_metrics=[
            ResourceMetrics(
                resource=Resource(attributes=[_kv("service.name", "regression")]),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="regression", version="1"),
                        metrics=[
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
                        ],
                    )
                ],
            )
        ]
    )
