import pytest
from harness.fuzz.logs import LogsFuzz


@pytest.mark.fuzz
def test_logs_matches_loki_under_randomized_load_and_queries(fuzz) -> None:
    fuzz(LogsFuzz)
