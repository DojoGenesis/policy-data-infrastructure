"""Housing cost burden from ACS detail tables — the one place the cells are named.

Why this module exists (2026-09-30): the previous code read B25106 as if its
cells were "owner burdened / owner severe / renter burdened / renter severe".
They are not. B25106 is broken out by *income bracket*, so

    B25106_006E  owner,  <$20K,          30 percent or more
    B25106_010E  owner,  $20K-$34,999,   30 percent or more
    B25106_024E  renter  TOTAL           (not a burden cell at all)
    B25106_028E  renter, <$20K,          30 percent or more

and B25106 has no 50% cut anywhere. The old "pct_cost_burdened" was low-income
burdened owners plus *every* renter household (it tracked renter share), and
the old "severe" figure was two unrelated income-bracket cells.

Definitions used here:

  cost burdened (30%+)  = sum of all ten "30 percent or more" cells in B25106
                          (five owner income brackets + five renter brackets)
  severely burdened     = 50.0 percent or more, from B25070 (renters,
  (50%+)                  gross rent) + B25091 (owners, with and without a
                          mortgage). B25106 cannot provide this.
  denominator           = households for which the ratio is computed:
                          B25106_001E minus the zero/negative-income and
                          no-cash-rent cells (_023, _045, _046). This matches
                          the Census "not computed" exclusion, and it makes
                          B25106 and B25070+B25091 reconcile exactly (checked
                          for WI, Dane, Milwaukee, Florence, 2023 and 2024).

Every variable code below is pinned to its official label in EXPECTED_LABELS,
verified 2026-09-30 against
https://api.census.gov/data/{2023,2024}/acs/acs5/groups/{B25106,B25070,B25091}.json
(labels identical in both vintages). ingest/tests/test_cost_burden.py asserts
that mapping offline and, when the network is available, against the live API.
"""
from __future__ import annotations

# --- B25106: tenure by housing costs as a percentage of household income -----
B25106_TOTAL = "B25106_001E"
B25106_OWNER_TOTAL = "B25106_002E"
B25106_RENTER_TOTAL = "B25106_024E"
B25106_OWNER_30PLUS = [
    "B25106_006E",  # < $20,000
    "B25106_010E",  # $20,000 to $34,999
    "B25106_014E",  # $35,000 to $49,999
    "B25106_018E",  # $50,000 to $74,999
    "B25106_022E",  # $75,000 or more
]
B25106_RENTER_30PLUS = [
    "B25106_028E",  # < $20,000
    "B25106_032E",  # $20,000 to $34,999
    "B25106_036E",  # $35,000 to $49,999
    "B25106_040E",  # $50,000 to $74,999
    "B25106_044E",  # $75,000 or more
]
B25106_OWNER_NOT_COMPUTED = ["B25106_023E"]                  # zero or negative income
B25106_RENTER_NOT_COMPUTED = ["B25106_045E", "B25106_046E"]  # zero/neg income, no cash rent

# --- B25070: gross rent as a percentage of household income (renters) --------
B25070_TOTAL = "B25070_001E"
B25070_50PLUS = "B25070_010E"
B25070_NOT_COMPUTED = "B25070_011E"

# --- B25091: owner costs as a percentage of household income -----------------
B25091_TOTAL = "B25091_001E"
B25091_50PLUS = ["B25091_011E", "B25091_022E"]         # with / without mortgage
B25091_NOT_COMPUTED = ["B25091_012E", "B25091_023E"]   # with / without mortgage

# Official labels as the metadata endpoint returns them, minus the leading
# "Estimate!!Total:!!" (the _001E totals keep "Estimate!!Total:").
EXPECTED_LABELS = {
    "B25106_001E": "Estimate!!Total:",
    "B25106_002E": "Owner-occupied housing units:",
    "B25106_006E": "Owner-occupied housing units:!!Less than $20,000:!!30 percent or more",
    "B25106_010E": "Owner-occupied housing units:!!$20,000 to $34,999:!!30 percent or more",
    "B25106_014E": "Owner-occupied housing units:!!$35,000 to $49,999:!!30 percent or more",
    "B25106_018E": "Owner-occupied housing units:!!$50,000 to $74,999:!!30 percent or more",
    "B25106_022E": "Owner-occupied housing units:!!$75,000 or more:!!30 percent or more",
    "B25106_023E": "Owner-occupied housing units:!!Zero or negative income",
    "B25106_024E": "Renter-occupied housing units:",
    "B25106_028E": "Renter-occupied housing units:!!Less than $20,000:!!30 percent or more",
    "B25106_032E": "Renter-occupied housing units:!!$20,000 to $34,999:!!30 percent or more",
    "B25106_036E": "Renter-occupied housing units:!!$35,000 to $49,999:!!30 percent or more",
    "B25106_040E": "Renter-occupied housing units:!!$50,000 to $74,999:!!30 percent or more",
    "B25106_044E": "Renter-occupied housing units:!!$75,000 or more:!!30 percent or more",
    "B25106_045E": "Renter-occupied housing units:!!Zero or negative income",
    "B25106_046E": "Renter-occupied housing units:!!No cash rent",
    "B25070_001E": "Estimate!!Total:",
    "B25070_010E": "50.0 percent or more",
    "B25070_011E": "Not computed",
    "B25091_001E": "Estimate!!Total:",
    "B25091_011E": "Housing units with a mortgage:!!50.0 percent or more",
    "B25091_012E": "Housing units with a mortgage:!!Not computed",
    "B25091_022E": "Housing units without a mortgage:!!50.0 percent or more",
    "B25091_023E": "Housing units without a mortgage:!!Not computed",
}

# Everything a caller must request from the API to use compute_cost_burden().
COST_BURDEN_VARS = list(EXPECTED_LABELS)


def normalize_label(label: str) -> str:
    """Strip the 'Estimate!!Total:!!' prefix the metadata endpoint puts on every cell."""
    return label.removeprefix("Estimate!!Total:!!")


def _sum(row: dict, codes: list[str]) -> int | None:
    """Sum nullable ints; None if any cell is missing (a partial sum is wrong, not small)."""
    total = 0
    for c in codes:
        v = row.get(c)
        if v is None:
            return None
        total += v
    return total


def _pct(num: int | None, denom: int | None) -> float | None:
    if num is None or denom is None or denom <= 0:
        return None
    return round(num / denom * 100, 1)


def compute_cost_burden(row: dict) -> dict:
    """Derive cost-burden counts and percents from one geography's ACS row.

    `row` maps variable codes (COST_BURDEN_VARS) to ints, with None for
    missing/suppressed values — i.e. already passed through safe_int().
    """
    owner_30 = _sum(row, B25106_OWNER_30PLUS)
    renter_30 = _sum(row, B25106_RENTER_30PLUS)
    owner_nc = _sum(row, B25106_OWNER_NOT_COMPUTED)
    renter_nc = _sum(row, B25106_RENTER_NOT_COMPUTED)

    owner_total = row.get(B25106_OWNER_TOTAL)
    renter_total = row.get(B25106_RENTER_TOTAL)
    owner_computed = None if owner_total is None or owner_nc is None else owner_total - owner_nc
    renter_computed = None if renter_total is None or renter_nc is None else renter_total - renter_nc

    total = row.get(B25106_TOTAL)
    nc = None if owner_nc is None or renter_nc is None else owner_nc + renter_nc
    hh_computed = None if total is None or nc is None else total - nc
    burdened = None if owner_30 is None or renter_30 is None else owner_30 + renter_30

    renter_50 = row.get(B25070_50PLUS)
    owner_50 = _sum(row, B25091_50PLUS)
    severe = None if renter_50 is None or owner_50 is None else renter_50 + owner_50
    r_tot, r_nc = row.get(B25070_TOTAL), row.get(B25070_NOT_COMPUTED)
    o_tot, o_nc = row.get(B25091_TOTAL), _sum(row, B25091_NOT_COMPUTED)
    severe_denom = (
        None if None in (r_tot, r_nc, o_tot, o_nc) else (r_tot - r_nc) + (o_tot - o_nc)
    )

    return {
        "households_computed": hh_computed,
        "cost_burdened": burdened,
        "pct_cost_burdened": _pct(burdened, hh_computed),
        "owner_households_computed": owner_computed,
        "owner_cost_burdened": owner_30,
        "pct_owner_cost_burdened": _pct(owner_30, owner_computed),
        "renter_households_computed": renter_computed,
        "renter_cost_burdened": renter_30,
        "pct_renter_cost_burdened": _pct(renter_30, renter_computed),
        "severely_cost_burdened": severe,
        "pct_severely_cost_burdened": _pct(severe, severe_denom),
    }
