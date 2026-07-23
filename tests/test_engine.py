from ingest import segment_profile, detect_double_distribution_split, classify_day_type, compute_structures
import pytest

def test_tail_is_not_double_distribution():
    counts = {100: 8, 99: 9, 98: 10, 97: 8, 96: 1, 95: 1, 94: 1, 93: 1,
              92: 1, 91: 1, 90: 1, 89: 1, 88: 1, 87: 1, 86: 1, 85: 1}
    levels = sorted(counts.keys())
    assert detect_double_distribution_split(segment_profile(levels, counts, 1)) is False

def test_genuine_double_distribution():
    counts = {100: 7, 99: 8, 98: 9, 97: 7, 96: 1, 95: 1, 94: 1,
              93: 8, 92: 9, 91: 8, 90: 7}
    levels = sorted(counts.keys())
    assert detect_double_distribution_split(segment_profile(levels, counts, 1)) is True

def test_classifies_directional_when_one_sided_no_trend():
    label, reason = classify_day_type(0, 64300, 63975, 64300, 62650, False, 0.43, 0.60)
    assert label == "Directional (unclassified)"

def test_raises_on_missing_data():
    with pytest.raises(ValueError):
        compute_structures(9999999999999) 