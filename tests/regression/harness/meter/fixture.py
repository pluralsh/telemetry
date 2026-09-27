"""Deterministic Meter fixtures and virtual-shard calculations."""

from __future__ import annotations

from dataclasses import dataclass

import blake3

NAMESPACE = "regression"
VIRTUAL_SHARDS = 16


@dataclass(frozen=True)
class Sample:
    timestamp_ms: int
    value: float


@dataclass(frozen=True)
class Series:
    labels: tuple[tuple[str, str], ...]
    samples: tuple[Sample, ...]


def series(
    name: str,
    labels: tuple[tuple[str, str], ...],
    samples: tuple[tuple[int, float], ...],
) -> Series:
    return Series(
        labels=(("__name__", name), *labels),
        samples=tuple(Sample(timestamp, value) for timestamp, value in samples),
    )


def regression_fixture(base_ms: int) -> tuple[Series, ...]:
    times = tuple(base_ms + index * 60_000 for index in range(10))
    result: list[Series] = []
    for instance, zone, bias in (("a", "east", 0.0), ("b", "west", 10.0)):
        result.extend(
            (
                series(
                    "regression_gauge",
                    (
                        ("instance", instance),
                        ("job", NAMESPACE),
                        ("zone", zone),
                    ),
                    tuple(
                        (timestamp, float(index) + bias)
                        for index, timestamp in enumerate(times)
                    ),
                ),
                series(
                    "regression_counter_total",
                    (("instance", instance), ("job", NAMESPACE)),
                    tuple(
                        (timestamp, float(index) * 100.0 + bias)
                        for index, timestamp in enumerate(times)
                    ),
                ),
                series(
                    "regression_left",
                    (("instance", instance),),
                    ((base_ms + 540_000, bias + 2.0),),
                ),
                series(
                    "regression_right",
                    (("instance", instance), ("zone", zone)),
                    ((base_ms + 540_000, bias + 3.0),),
                ),
            )
        )
    return tuple(result)


def fixture_shard(value: Series) -> int:
    hasher = blake3.blake3()
    hasher.update(NAMESPACE.encode())
    for name, label_value in sorted(value.labels):
        hasher.update(b"\0")
        hasher.update(name.encode())
        hasher.update(b"\0")
        hasher.update(label_value.encode())
    return int.from_bytes(hasher.digest(length=8), "big") % VIRTUAL_SHARDS


def assert_spans_writer_ranges(value: tuple[Series, ...]) -> None:
    shards = {fixture_shard(item) for item in value}
    assert any(shard < 8 for shard in shards)
    assert any(shard >= 8 for shard in shards), (
        "posting to writer-0 must exercise forwarding to writer-1"
    )
