"""Environment-driven configuration and time budget for differential fuzz runs."""

from __future__ import annotations

import json
import os
import re
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]

# Outcomes that fail a run unless `FUZZ_FAIL_ON` overrides them. `ingest` covers
# writes the oracle accepted but the implementation rejected and rounds that
# never became visible. `oracle_error` and `latency` are opt-in.
DEFAULT_FAIL_ON = frozenset(
    {"mismatch", "impl_error", "impl_timeout", "unstable_impl", "ingest"}
)

SCENARIOS = ("recent", "historical")
# MinIO listens here in the historical scenario, behind the latency proxy.
HISTORICAL_ENVIRONMENT = {"MINIO_ADDRESS": ":9010"}

_DURATION = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h)")
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(value: str) -> float:
    """Parse `90`, `90s`, `30m`, `1h30m`, or `250ms` into seconds."""
    text = value.strip().lower()
    if not text:
        raise ValueError("empty duration")
    try:
        return float(text)
    except ValueError:
        pass
    position = 0
    total = 0.0
    for match in _DURATION.finditer(text):
        if match.start() != position:
            break
        total += float(match.group(1)) * _UNITS[match.group(2)]
        position = match.end()
    if position != len(text):
        raise ValueError(f"invalid duration {value!r}")
    return total


def parse_range(value: str) -> tuple[int, int]:
    """Parse `40-160` or `100` into an inclusive integer range."""
    low, _, high = value.partition("-")
    bounds = (int(low), int(high or low))
    if bounds[0] < 0 or bounds[1] < bounds[0]:
        raise ValueError(f"invalid range {value!r}")
    return bounds


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ("", "0", "false", "no", "off")


@dataclass(frozen=True)
class Endpoint:
    """A base URL (including any namespace prefix) plus fixed request headers."""

    url: str
    authorization: str | None = None
    headers: tuple[tuple[str, str], ...] = ()

    def describe(self) -> dict[str, object]:
        return {
            "url": self.url,
            "authorization": self.authorization is not None,
            "headers": sorted(name for name, _ in self.headers),
        }


def endpoint_from_env(product: str, role: str, default: Endpoint) -> Endpoint:
    """Override an endpoint with `FUZZ_<PRODUCT>_<ROLE>_{URL,AUTHORIZATION,HEADERS}`.

    `HEADERS` is a JSON object. An empty `AUTHORIZATION` removes the default
    credential, which is how remote oracles without auth are configured.
    """
    prefix = f"FUZZ_{product.upper()}_{role.upper()}_"
    url = os.environ.get(f"{prefix}URL", default.url).rstrip("/")
    authorization = os.environ.get(f"{prefix}AUTHORIZATION", default.authorization)
    headers = dict(default.headers)
    raw_headers = os.environ.get(f"{prefix}HEADERS")
    if raw_headers:
        parsed = json.loads(raw_headers)
        if not isinstance(parsed, dict):
            raise ValueError(f"{prefix}HEADERS must be a JSON object")
        headers.update({str(key): str(value) for key, value in parsed.items()})
    return Endpoint(
        url=url,
        authorization=authorization or None,
        headers=tuple(sorted(headers.items())),
    )


@dataclass(frozen=True)
class FuzzConfig:
    product: str
    duration_s: float
    seed: int
    run_id: str
    stack: str
    isolated: bool
    output_dir: Path
    scale: float
    window_s: float
    queries_per_round: tuple[int, int]
    max_rounds: int
    max_cases: int
    request_timeout_s: float
    visibility_timeout_s: float
    settle_s: float
    latency_ratio: float
    latency_floor_ms: float
    recheck: bool
    fail_on: frozenset[str]
    max_artifacts: int
    artifact_bytes: int
    record_data: bool
    # `recent` reads what production serves from memory and fresh storage;
    # `historical` forces every oracle onto object-store reads and puts a
    # first-byte delay in front of MinIO for both sides.
    scenario: str = "recent"

    @classmethod
    def from_env(cls, product: str, *, default_window: str) -> FuzzConfig:
        seed = int(_env("FUZZ_SEED", str(secrets.randbits(32))))
        started = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        run_id = _env("FUZZ_RUN_ID", f"{started}-{seed:08x}")
        stack = _env("FUZZ_STACK", "compose")
        if stack not in ("compose", "external"):
            raise ValueError("FUZZ_STACK must be 'compose' or 'external'")
        scenario = _env("FUZZ_SCENARIO", "recent")
        if scenario not in SCENARIOS:
            raise ValueError(f"FUZZ_SCENARIO must be one of {SCENARIOS}")
        fail_on = _env("FUZZ_FAIL_ON", ",".join(sorted(DEFAULT_FAIL_ON)))
        output = _env(
            "FUZZ_OUTPUT_DIR",
            str(REPO_ROOT / "target" / "fuzz" / f"{product}-{run_id}"),
        )
        return cls(
            product=product,
            duration_s=parse_duration(_env("FUZZ_DURATION", "30m")),
            seed=seed,
            run_id=run_id,
            stack=stack,
            # A fresh compose stack only holds this run's data, so unscoped
            # metadata endpoints are comparable. Long-lived remote stacks
            # accumulate history that the two sides do not share.
            isolated=_flag("FUZZ_ISOLATED", stack == "compose"),
            output_dir=Path(output),
            scale=float(_env("FUZZ_SCALE", "1")),
            window_s=parse_duration(_env("FUZZ_WINDOW", default_window)),
            queries_per_round=parse_range(_env("FUZZ_QUERIES_PER_ROUND", "40-160")),
            max_rounds=int(_env("FUZZ_MAX_ROUNDS", "0")),
            max_cases=int(_env("FUZZ_MAX_CASES", "0")),
            request_timeout_s=parse_duration(_env("FUZZ_REQUEST_TIMEOUT", "30s")),
            visibility_timeout_s=parse_duration(_env("FUZZ_VISIBILITY_TIMEOUT", "90s")),
            settle_s=parse_duration(_env("FUZZ_SETTLE", "1s")),
            latency_ratio=float(_env("FUZZ_LATENCY_RATIO", "10")),
            latency_floor_ms=float(_env("FUZZ_LATENCY_FLOOR_MS", "250")),
            recheck=_flag("FUZZ_RECHECK", True),
            fail_on=frozenset(item.strip() for item in fail_on.split(",") if item),
            max_artifacts=int(_env("FUZZ_MAX_ARTIFACTS", "200")),
            artifact_bytes=int(_env("FUZZ_ARTIFACT_BYTES", str(256 * 1024))),
            record_data=_flag("FUZZ_RECORD_DATA", True),
            scenario=scenario,
        )

    @property
    def historical(self) -> bool:
        return self.scenario == "historical"

    def describe(self) -> dict[str, object]:
        return {
            "product": self.product,
            "duration_s": self.duration_s,
            "seed": self.seed,
            "run_id": self.run_id,
            "stack": self.stack,
            "scenario": self.scenario,
            "isolated": self.isolated,
            "scale": self.scale,
            "window_s": self.window_s,
            "queries_per_round": list(self.queries_per_round),
            "max_rounds": self.max_rounds,
            "max_cases": self.max_cases,
            "request_timeout_s": self.request_timeout_s,
            "visibility_timeout_s": self.visibility_timeout_s,
            "latency_ratio": self.latency_ratio,
            "latency_floor_ms": self.latency_floor_ms,
            "recheck": self.recheck,
            "fail_on": sorted(self.fail_on),
        }


class Budget:
    """Monotonic wall-clock budget shared by loading, querying, and reporting."""

    def __init__(self, seconds: float, *, clock=time.monotonic) -> None:
        self.seconds = seconds
        self._clock = clock
        self._started = clock()

    def elapsed(self) -> float:
        return self._clock() - self._started

    def remaining(self) -> float:
        return max(0.0, self.seconds - self.elapsed())

    def expired(self, reserve: float = 0.0) -> bool:
        return self.remaining() <= reserve
