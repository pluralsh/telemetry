import pytest
from harness.fuzz.pseudofs import PseudofsFuzz


@pytest.mark.fuzz
def test_pseudofs_matches_reference_model_under_randomized_load_races_and_crashes(
    fuzz,
) -> None:
    fuzz(PseudofsFuzz)
