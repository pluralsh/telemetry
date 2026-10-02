"""Deterministic Metrics fixtures and shard calculations."""

from __future__ import annotations

from dataclasses import dataclass

import blake3

NAMESPACE = "regression"
SHARDS = 2
"""Storage shards in the regression configs: one per writer."""


@dataclass(frozen=True)
class Sample:
    timestamp_ms: int
    value: float


Spans = tuple[tuple[int, tuple[int, ...]], ...]
"""Sparse buckets as `(offset, absolute counts)` spans, as in remote write."""

CUSTOM_BUCKETS_SCHEMA = -53


@dataclass(frozen=True)
class NativeHistogram:
    timestamp_ms: int
    schema: int
    zero_threshold: float
    zero_count: int
    sum: float
    positive: Spans = ()
    negative: Spans = ()
    custom_values: tuple[float, ...] = ()

    @property
    def count(self) -> int:
        return self.zero_count + sum(
            sum(counts) for _, counts in (*self.positive, *self.negative)
        )


@dataclass(frozen=True)
class Series:
    labels: tuple[tuple[str, str], ...]
    samples: tuple[Sample, ...]
    histograms: tuple[NativeHistogram, ...] = ()


def series(
    name: str,
    labels: tuple[tuple[str, str], ...],
    samples: tuple[tuple[int, float], ...],
) -> Series:
    return Series(
        labels=(("__name__", name), *labels),
        samples=tuple(Sample(timestamp, value) for timestamp, value in samples),
    )


def histogram_series(
    name: str,
    labels: tuple[tuple[str, str], ...],
    histograms: tuple[NativeHistogram, ...],
) -> Series:
    return Series(
        labels=(("__name__", name), *labels),
        samples=(),
        histograms=histograms,
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


def native_histogram_fixture(base_ms: int) -> tuple[Series, ...]:
    """Cumulative native histograms sharing the float fixture's timestamps.

    Instance `a` grows monotonically at schema 1. Instance `b` uses schema 0
    with negative buckets and resets at index 6 so `rate` exercises counter
    reset handling. The custom-bucket series covers schema -53.
    """
    times = tuple(base_ms + index * 60_000 for index in range(10))
    job = (("job", NAMESPACE),)

    def grow(index: int, timestamp: int) -> NativeHistogram:
        return NativeHistogram(
            timestamp_ms=timestamp,
            schema=1,
            zero_threshold=0.001,
            zero_count=index,
            sum=1.5 * index,
            positive=(
                (0, (index + 1, 2 * index + 1, 3 * index)),
                (2, (index, index + 2)),
            ),
        )

    def reset(index: int, timestamp: int) -> NativeHistogram:
        value = index if index < 6 else index - 6
        return NativeHistogram(
            timestamp_ms=timestamp,
            schema=0,
            zero_threshold=0.001,
            zero_count=value,
            sum=1.75 * value,
            positive=((-1, (2 * value + 1, value)), (3, (value + 1,))),
            negative=((0, (value,)),),
        )

    def custom(index: int, timestamp: int) -> NativeHistogram:
        return NativeHistogram(
            timestamp_ms=timestamp,
            schema=CUSTOM_BUCKETS_SCHEMA,
            zero_threshold=0.0,
            zero_count=0,
            sum=0.75 * index,
            positive=((0, (index, 2 * index, index + 1, index, 1)),),
            custom_values=(0.1, 0.5, 1.0, 5.0),
        )

    return (
        histogram_series(
            "regression_native_seconds",
            (("instance", "a"), *job),
            tuple(grow(index, timestamp) for index, timestamp in enumerate(times)),
        ),
        histogram_series(
            "regression_native_seconds",
            (("instance", "b"), *job),
            tuple(reset(index, timestamp) for index, timestamp in enumerate(times)),
        ),
        histogram_series(
            "regression_nhcb_seconds",
            (("instance", "a"), *job),
            tuple(custom(index, timestamp) for index, timestamp in enumerate(times)),
        ),
    )


CLASSIC_BOUNDS = ("0.1", "0.5", "1", "+Inf")


def classic_histogram_fixture(base_ms: int) -> tuple[Series, ...]:
    """Classic `le`-labelled histograms: cumulative `_bucket` series plus
    `_sum` and `_count`, sharing the float fixture's timestamps.

    Instance `a` grows monotonically; instance `b` resets at index 6.
    """
    times = tuple(base_ms + index * 60_000 for index in range(10))
    result: list[Series] = []
    for instance, resets in (("a", False), ("b", True)):
        labels = (("instance", instance), ("job", NAMESPACE))
        values = tuple(
            index - 6 if resets and index >= 6 else index for index in range(10)
        )
        per_bucket = tuple((v, 3 * v + 1, 2 * v, v + 2) for v in values)
        for position, bound in enumerate(CLASSIC_BOUNDS):
            result.append(
                series(
                    "regression_classic_seconds_bucket",
                    (*labels, ("le", bound)),
                    tuple(
                        (timestamp, float(sum(counts[: position + 1])))
                        for timestamp, counts in zip(times, per_bucket, strict=True)
                    ),
                )
            )
        result.append(
            series(
                "regression_classic_seconds_count",
                labels,
                tuple(
                    (timestamp, float(sum(counts)))
                    for timestamp, counts in zip(times, per_bucket, strict=True)
                ),
            )
        )
        result.append(
            series(
                "regression_classic_seconds_sum",
                labels,
                tuple(
                    (timestamp, 0.37 * v)
                    for timestamp, v in zip(times, values, strict=True)
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
    hash_value = int.from_bytes(hasher.digest(length=16), "big")
    return (hash_value * SHARDS) >> 128


def assert_spans_writer_ranges(value: tuple[Series, ...]) -> None:
    shards = {fixture_shard(item) for item in value}
    assert 0 in shards
    assert 1 in shards, "posting to writer-0 must exercise forwarding to writer-1"
