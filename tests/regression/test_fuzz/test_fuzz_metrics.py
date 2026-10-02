import pytest
from harness.fuzz.metrics import MetricsFuzz


@pytest.mark.fuzz
def test_metrics_matches_prometheus_under_randomized_load_and_queries(fuzz) -> None:
    fuzz(MetricsFuzz)
