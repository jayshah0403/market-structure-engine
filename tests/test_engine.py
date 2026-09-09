import os

from ingest import segment_profile, detect_double_distribution_split, classify_day_type, compute_structures
import ingest
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

@pytest.mark.skipif(
    "CONNECTION_STRING" not in os.environ,
    reason="integration test: needs a live database",
)
def test_raises_on_missing_data():
    with pytest.raises(ValueError):
        compute_structures(9999999999999)


# --- PR 1 acceptance tests -------------------------------------------------
#
# compute_structures() reaches the database twice: through compute_profile ->
# get_profile_grid, and through detect_trend. Both are patched out so these stay
# pure unit tests (V2_SPEC 4: "pure engine tests need no network or DB"). Every
# detector in between runs for real on the synthetic profile.

def _stub_profile(monkeypatch, profile, trend=(0.5, 0.5)):
    """Make compute_structures see `profile` ({bucket: [periods]}) and no DB.

    get_profile_grid returns (price_bucket, period, trade_count, volume) rows;
    compute_profile only reads the first two, so the last two are filler.
    """
    rows = [(bucket, period, 1, 1.0)
            for bucket, periods in profile.items()
            for period in periods]
    monkeypatch.setattr(ingest, "get_profile_grid", lambda _start_ts: rows)
    monkeypatch.setattr(ingest, "detect_trend", lambda _start_ts: trend)


def test_poor_high_derives_from_day_high_and_poor_low_from_day_low(monkeypatch):
    # day_high (105) is touched in 2 periods -> poor high.
    # day_low  (100) is touched in 1 period  -> not a poor low.
    _stub_profile(monkeypatch, {
        100: [0],
        101: [0, 1, 2],
        102: [0, 1, 2, 3],
        103: [1, 2, 3],
        104: [2, 3],
        105: [2, 3],
    })

    s = compute_structures(0)

    assert s["day_high"] == 105.0
    assert s["day_low"] == 100.0
    assert s["poor_high"] == s["day_high"]
    assert s["poor_low"] is None


def test_tails_are_none_when_no_single_print_buckets(monkeypatch):
    # Every bucket is touched in >= 2 periods, so arr_single_tpo is empty.
    _stub_profile(monkeypatch, {
        100: [0, 1],
        101: [0, 1, 2],
        102: [1, 2, 3],
        103: [2, 3],
    })

    s = compute_structures(0)

    assert s["arr_single_tpo"] == []
    assert s["buying_tail"] is None
    assert s["selling_tail"] is None
