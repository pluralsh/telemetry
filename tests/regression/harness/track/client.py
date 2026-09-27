"""Synchronous HTTP and OTLP gRPC clients for Track/Tempo regression."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import grpc
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceStub,
)


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes

    def json(self) -> dict[str, Any]:
        return json.loads(self.body)


@dataclass(frozen=True)
class Target:
    base_url: str
    namespace: str
    authorization: str | None = None
    read_prefix: str = ""
    write_prefix: str = ""


class TraceClient:
    def __init__(self, *, timeout: float = 15) -> None:
        self.timeout = timeout

    def request(
        self,
        method: str,
        target: Target,
        path: str,
        *,
        read: bool = False,
        write: bool = False,
        parameters: list[tuple[str, str]] | None = None,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        authorization: str | None | object = ...,
    ) -> Response:
        prefix = target.read_prefix if read else target.write_prefix if write else ""
        url = f"{target.base_url}{prefix}{path}"
        if parameters:
            url = f"{url}?{urllib.parse.urlencode(parameters)}"
        request_headers = {"X-Scope-OrgID": target.namespace, **(headers or {})}
        credential = target.authorization if authorization is ... else authorization
        if credential:
            request_headers["Authorization"] = str(credential)
        request = urllib.request.Request(
            url, data=body, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return Response(response.status, response.read())
        except urllib.error.HTTPError as error:
            return Response(error.code, error.read())

    def json(
        self,
        target: Target,
        path: str,
        *,
        parameters: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        response = self.request("GET", target, path, read=True, parameters=parameters)
        assert response.status == 200, (
            f"{target.base_url}{path}: {response.status} "
            f"{response.body.decode(errors='replace')}"
        )
        return response.json()

    def otlp_http(
        self, target: Target, body: bytes, *, authorization: str | None | object = ...
    ) -> Response:
        return self.request(
            "POST",
            target,
            "/v1/traces",
            write=True,
            body=body,
            headers={"Content-Type": "application/x-protobuf"},
            authorization=authorization,
        )

    def zipkin(
        self, target: Target, body: bytes, *, authorization: str | None | object = ...
    ) -> Response:
        return self.request(
            "POST",
            target,
            "/api/v2/spans",
            write=True,
            body=body,
            headers={"Content-Type": "application/json"},
            authorization=authorization,
        )

    def otlp_grpc(
        self,
        endpoint: str,
        request: ExportTraceServiceRequest,
        *,
        namespace: str,
        authorization: str | None = None,
    ) -> None:
        metadata = [("x-scope-orgid", namespace)]
        if authorization:
            metadata.append(("authorization", authorization))
        with grpc.insecure_channel(endpoint) as channel:
            TraceServiceStub(channel).Export(
                request, metadata=metadata, timeout=self.timeout
            )

    def jaeger_grpc(
        self,
        endpoint: str,
        request: bytes,
        *,
        namespace: str,
        authorization: str | None = None,
    ) -> None:
        metadata = [("x-scope-orgid", namespace)]
        if authorization:
            metadata.append(("authorization", authorization))
        with grpc.insecure_channel(endpoint) as channel:
            post_spans = channel.unary_unary(
                "/jaeger.api_v2.CollectorService/PostSpans",
                request_serializer=lambda value: value,
                response_deserializer=lambda value: value,
            )
            post_spans(request, metadata=metadata, timeout=self.timeout)

    def wait_for_trace(
        self, target: Target, trace_id: str, *, timeout: float = 15
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last = Response(0, b"not attempted")
        while time.monotonic() < deadline:
            last = self.request("GET", target, f"/api/v2/traces/{trace_id}", read=True)
            if last.status == 200:
                return last.json()
            time.sleep(0.1)
        raise AssertionError(
            f"trace {trace_id} did not appear at {target.base_url}: "
            f"{last.status} {last.body.decode(errors='replace')}"
        )
