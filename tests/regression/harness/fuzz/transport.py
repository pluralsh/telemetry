"""Timed HTTP exchanges that record failures instead of raising them."""

from __future__ import annotations

import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from .config import Budget, Endpoint

SUCCESS = frozenset(range(200, 300))


@dataclass(frozen=True)
class Request:
    method: str
    path: str
    params: tuple[tuple[str, str], ...] = ()
    body: bytes | None = None
    headers: tuple[tuple[str, str], ...] = ()

    def url(self, endpoint: Endpoint) -> str:
        url = f"{endpoint.url}{self.path}"
        if self.params:
            url = f"{url}?{urllib.parse.urlencode(self.params)}"
        return url

    def describe(self) -> dict[str, object]:
        value: dict[str, object] = {
            "method": self.method,
            "path": self.path,
            "params": [list(item) for item in self.params],
        }
        if self.body is not None:
            value["body_bytes"] = len(self.body)
        return value


@dataclass(frozen=True)
class Exchange:
    """One request's outcome. `status` is `None` for transport failures."""

    status: int | None
    body: bytes
    latency_ms: float
    error: str | None = None

    def ok(self, statuses: Collection[int] = SUCCESS) -> bool:
        return self.status is not None and self.status in statuses

    @property
    def timed_out(self) -> bool:
        return self.error == "timeout"

    def json(self) -> Any:
        return json.loads(self.body)

    def describe(self) -> dict[str, object]:
        return {
            "status": self.status,
            "latency_ms": round(self.latency_ms, 3),
            "bytes": len(self.body),
            "error": self.error,
        }


class Transport:
    def __init__(self, *, timeout_s: float) -> None:
        self.timeout_s = timeout_s

    def send(self, endpoint: Endpoint, request: Request) -> Exchange:
        headers = dict(endpoint.headers)
        headers.update(request.headers)
        if endpoint.authorization:
            headers["Authorization"] = endpoint.authorization
        prepared = urllib.request.Request(
            request.url(endpoint),
            data=request.body,
            headers=headers,
            method=request.method,
        )
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(prepared, timeout=self.timeout_s) as response:
                body = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            body = error.read()
            return Exchange(error.code, body, _elapsed_ms(started))
        except TimeoutError:
            return Exchange(None, b"", _elapsed_ms(started), "timeout")
        except urllib.error.URLError as error:
            reason = error.reason
            if isinstance(reason, TimeoutError):
                return Exchange(None, b"", _elapsed_ms(started), "timeout")
            return Exchange(None, b"", _elapsed_ms(started), f"transport: {reason}")
        except (OSError, http.client.HTTPException) as error:
            # `HTTPException` covers a body cut short, as Loki does to
            # responses tens of MiB long.
            return Exchange(None, b"", _elapsed_ms(started), f"transport: {error!r}")
        return Exchange(status, body, _elapsed_ms(started))

    def send_with_retry(
        self,
        endpoint: Endpoint,
        request: Request,
        *,
        budget: Budget,
        attempts: int = 4,
        retry_statuses: Collection[int] = (429, 502, 503, 504),
    ) -> Exchange:
        """Retry throttling and transient failures for writes, never for reads:
        read latency and errors are measurements, not noise."""
        exchange = self.send(endpoint, request)
        for attempt in range(1, attempts):
            transient = exchange.status in retry_statuses or (
                exchange.status is None and not exchange.timed_out
            )
            if not transient or budget.expired():
                break
            time.sleep(min(0.25 * 2**attempt, budget.remaining()))
            exchange = self.send(endpoint, request)
        return exchange


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000
