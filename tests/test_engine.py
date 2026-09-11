import datetime

from ingest import segment_profile, detect_double_distribution_split, classify_day_type, compute_structures

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

# --- PR 1 acceptance tests -------------------------------------------------
#
# Unchanged in what they assert. PR 3 turned compute_structures into a pure
# function over an in-memory profile, so these no longer have to patch
# get_profile_grid and detect_trend out of the way (both of which reached the
# database) — the synthetic profile is now simply passed in.

# Three periods, one violated low and one violated high, so the classifier is
# not steered into a Trend branch by the trend confidences. Irrelevant to what
# these tests assert, but it keeps the fixtures honest.
FLAT_PERIOD_RANGES = {0: (100.0, 90.0), 1: (101.0, 89.0), 2: (99.0, 91.0)}


def _structures(profile, period_ranges=FLAT_PERIOD_RANGES):
    """Run every detector for real over `profile` ({bucket: [periods]})."""
    return compute_structures(
        {bucket: set(periods) for bucket, periods in profile.items()},
        period_ranges,
        symbol="TESTUSDT",
        session_date=datetime.date(2026, 1, 1),
        bucket_size=1,
        ib_periods=2,
    )


def test_poor_high_derives_from_day_high_and_poor_low_from_day_low():
    # day_high (105) is touched in 2 periods -> poor high.
    # day_low  (100) is touched in 1 period  -> not a poor low.
    s = _structures({
        100: [0],
        101: [0, 1, 2],
        102: [0, 1, 2, 3],
        103: [1, 2, 3],
        104: [2, 3],
        105: [2, 3],
    })

    assert s["day_high"] == 105.0
    assert s["day_low"] == 100.0
    assert s["poor_high"] == s["day_high"]
    assert s["poor_low"] is None


def test_tails_are_none_when_no_single_print_buckets():
    # Every bucket is touched in >= 2 periods, so there are no single prints.
    s = _structures({
        100: [0, 1],
        101: [0, 1, 2],
        102: [1, 2, 3],
        103: [2, 3],
    })

    assert s["single_prints"] == []
    assert s["buying_tail"] is None
    assert s["selling_tail"] is None
