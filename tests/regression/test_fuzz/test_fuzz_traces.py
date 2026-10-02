import pytest
from harness.fuzz.traces import TracesFuzz


@pytest.mark.fuzz
def test_traces_matches_tempo_under_randomized_load_and_queries(fuzz) -> None:
    fuzz(TracesFuzz)
