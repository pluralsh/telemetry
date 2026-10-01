"""Small synchronous HTTP client for Metrics regression scenarios."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

_DEFAULT_AUTH = object()


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes

    def json(self) -> dict[str, Any]:
        return json.loads(self.body)


@dataclass(frozen=True)
class Target:
    base_url: str
    authorization: str | None = None


class PrometheusClient:
    def __init__(self, *, timeout: float = 15) -> None:
        self.timeout = timeout

    def request(
        self,
        method: str,
        target: Target,
        path: str,
        *,
        parameters: list[tuple[str, str]] | None = None,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        authorization: str | None | object = _DEFAULT_AUTH,
    ) -> Response:
        url = f"{target.base_url}{path}"
        if parameters:
            url = f"{url}?{urllib.parse.urlencode(parameters)}"
        request_headers = dict(headers or {})
        credential = (
            target.authorization if authorization is _DEFAULT_AUTH else authorization
        )
        if credential:
            assert isinstance(credential, str)
            request_headers["Authorization"] = credential
        request = urllib.request.Request(
            url,
            data=body,
            headers=request_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return Response(response.status, response.read())
        except urllib.error.HTTPError as error:
            return Response(error.code, error.read())

    def query(
        self,
        target: Target,
        path: str,
        parameters: list[tuple[str, str]],
    ) -> dict[str, Any]:
        response = self.request("GET", target, path, parameters=parameters)
        assert 200 <= response.status < 300, (
            f"{target.base_url}{path}: {response.status} "
            f"{response.body.decode(errors='replace')}"
        )
        return response.json()

    def remote_write(
        self,
        target: Target,
        body: bytes,
        *,
        request_id: str | None = None,
        authorization: str | None | object = _DEFAULT_AUTH,
    ) -> Response:
        headers = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
        }
        if request_id:
            headers["X-Request-Id"] = request_id
        return self.request(
            "POST",
            target,
            "/api/v1/write",
            body=body,
            headers=headers,
            authorization=authorization,
        )

    def otlp_write(
        self,
        target: Target,
        body: bytes,
        *,
        path: str = "/v1/metrics",
        request_id: str | None = None,
    ) -> Response:
        headers = {"Content-Type": "application/x-protobuf"}
        if request_id:
            headers["X-Request-Id"] = request_id
        return self.request("POST", target, path, body=body, headers=headers)
