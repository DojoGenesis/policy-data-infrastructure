"""Hold the cost-burden cells to their official Census labels.

The 2026-09-30 bug was a comment ("renter cost burdened") attached to a cell
whose real label is "Renter-occupied housing units:" — the renter total. These
tests make the label, not the comment, the thing the code is checked against.

    pytest ingest/tests/test_cost_burden.py            # offline + live metadata
    PDI_OFFLINE=1 pytest ingest/tests/test_cost_burden.py   # skip the network check
"""
import json
import os
import sys
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib import cost_burden as cb  # noqa: E402


# --- offline: the constants agree with the pinned labels ---------------------

def test_every_30plus_cell_is_labelled_30_percent_or_more():
    cells = cb.B25106_OWNER_30PLUS + cb.B25106_RENTER_30PLUS
    assert len(cells) == len(set(cells)) == 10
    for code in cells:
        assert cb.EXPECTED_LABELS[code].endswith("!!30 percent or more"), code


def test_owner_and_renter_cells_are_on_the_right_side():
    for code in cb.B25106_OWNER_30PLUS + cb.B25106_OWNER_NOT_COMPUTED:
        assert cb.EXPECTED_LABELS[code].startswith("Owner-occupied"), code
    for code in cb.B25106_RENTER_30PLUS + cb.B25106_RENTER_NOT_COMPUTED:
        assert cb.EXPECTED_LABELS[code].startswith("Renter-occupied"), code


def test_totals_are_never_counted_as_burdened():
    # The original bug: B25106_024E (all renters) summed as "renter burdened".
    burden_cells = set(cb.B25106_OWNER_30PLUS + cb.B25106_RENTER_30PLUS)
    for total in (cb.B25106_TOTAL, cb.B25106_OWNER_TOTAL, cb.B25106_RENTER_TOTAL):
        assert total not in burden_cells
    assert cb.EXPECTED_LABELS[cb.B25106_RENTER_TOTAL] == "Renter-occupied housing units:"


def test_severe_cells_are_labelled_50_percent_or_more():
    for code in [cb.B25070_50PLUS] + cb.B25091_50PLUS:
        assert cb.EXPECTED_LABELS[code].endswith("50.0 percent or more"), code
    # B25106 has no 50% cut — nothing from it may feed the severe measure.
    assert not any("50 percent" in cb.EXPECTED_LABELS[c] or "50.0 percent" in cb.EXPECTED_LABELS[c]
                   for c in cb.EXPECTED_LABELS if c.startswith("B25106"))


def test_not_computed_cells():
    assert all("Zero or negative income" in cb.EXPECTED_LABELS[c] or "No cash rent" in cb.EXPECTED_LABELS[c]
               for c in cb.B25106_OWNER_NOT_COMPUTED + cb.B25106_RENTER_NOT_COMPUTED)
    assert cb.EXPECTED_LABELS[cb.B25070_NOT_COMPUTED] == "Not computed"
    assert all(cb.EXPECTED_LABELS[c].endswith("Not computed") for c in cb.B25091_NOT_COMPUTED)


# --- offline: arithmetic on real published values ---------------------------

def _fixture():
    """Dane County, ACS 2020-2024 5-Year, pulled from the API 2026-09-30."""
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "cost_burden_dane_2024.json")) as f:
        return json.load(f)


def test_compute_matches_hand_calculation_for_dane():
    fx = _fixture()
    row = fx["row"]
    out = cb.compute_cost_burden(row)
    burdened = sum(row[c] for c in cb.B25106_OWNER_30PLUS + cb.B25106_RENTER_30PLUS)
    denom = row["B25106_001E"] - row["B25106_023E"] - row["B25106_045E"] - row["B25106_046E"]
    assert out["cost_burdened"] == burdened
    assert out["households_computed"] == denom
    assert out["pct_cost_burdened"] == round(burdened / denom * 100, 1)
    assert out["pct_cost_burdened"] == fx["expected"]["pct_cost_burdened"]
    assert out["pct_severely_cost_burdened"] == fx["expected"]["pct_severely_cost_burdened"]
    # Severe is a subset of burdened; the old formula could invert this.
    assert out["severely_cost_burdened"] < out["cost_burdened"]
    # Renter share is not burden: the old number was ~44.6 for Dane.
    assert abs(out["pct_cost_burdened"] - 44.6) > 5


def test_missing_cell_propagates_none_not_a_partial_sum():
    row = dict(_fixture()["row"])
    row["B25106_044E"] = None
    out = cb.compute_cost_burden(row)
    assert out["cost_burdened"] is None and out["pct_cost_burdened"] is None


# --- live: the pinned labels are still the Census's labels -------------------

@pytest.mark.skipif(os.environ.get("PDI_OFFLINE") == "1", reason="PDI_OFFLINE=1")
@pytest.mark.parametrize("year", [2023, 2024])
def test_pinned_labels_match_census_metadata(year):
    by_table: dict[str, list[str]] = {}
    for code in cb.EXPECTED_LABELS:
        by_table.setdefault(code.split("_")[0], []).append(code)
    for table, codes in by_table.items():
        url = f"https://api.census.gov/data/{year}/acs/acs5/groups/{table}.json"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                meta = json.load(resp)["variables"]
        except OSError as e:  # no network: don't fail the offline suite
            pytest.skip(f"metadata endpoint unreachable: {e}")
        for code in codes:
            assert cb.normalize_label(meta[code]["label"]) == cb.EXPECTED_LABELS[code], (year, code)
