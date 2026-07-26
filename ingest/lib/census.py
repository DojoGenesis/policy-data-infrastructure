"""Census API client — generalized for any geography."""
import urllib.request
import urllib.parse
import json
import os
import time

CENSUS_BASE = "https://api.census.gov/data/{year}/acs/acs5"
API_KEY = os.environ.get("CENSUS_API_KEY", "")

# An API key is now REQUIRED (verified 2026-07-26). A keyless request no longer
# degrades to a rate-limited success — the API answers 302 -> /data/missing_key.html
# with an `X-DataWebAPI-KeyError: 1` header. urllib follows that redirect, so the
# caller gets an HTML page and a confusing JSONDecodeError far from the real cause.
# require_api_key() below converts that into one clear message at the boundary.
RATE_LIMIT_DELAY = 1.5  # seconds between requests (~40 req/min; safely under the keyed 500/min)

NULL_SENTINEL = -666666666


def require_api_key() -> str:
    """Return the Census API key, or raise with a fix-it message.

    Call this at the top of a fetch script's main() so a missing key fails on
    line one instead of mid-fetch as an unparseable-JSON error.
    """
    if not API_KEY:
        raise RuntimeError(
            "CENSUS_API_KEY is not set, and the Census API now rejects keyless "
            "requests (302 -> missing_key.html).\n"
            "Get a key at https://api.census.gov/data/key_signup.html, then:\n"
            "  export CENSUS_API_KEY=<key>"
        )
    return API_KEY


def _dataset_path(year: int, subject: bool = False) -> str:
    """Return the ACS 5-Year dataset path for the given year."""
    dataset = "acs5/subject" if subject else "acs5"
    return f"https://api.census.gov/data/{year}/acs/{dataset}"


def _build_url(base: str, variables: list[str], geo_clause: str) -> str:
    """Assemble a Census API URL with optional API key."""
    get_param = ",".join(["NAME"] + variables)
    url = f"{base}?get={urllib.parse.quote(get_param, safe=',')}&{geo_clause}"
    if API_KEY:
        url += f"&key={API_KEY}"
    return url


def _geo_clause(geo_level: str, state_fips: str, county_fips: str | None) -> str:
    """
    Build the Census API geography clause.

    geo_level options:
      'tract'       — all tracts in a county, or statewide if county_fips is None
      'block_group' — all block groups in a county (county_fips required)
      'county'      — all counties in a state
      'state'       — the state itself
    """
    state = state_fips.zfill(2)
    if geo_level == "tract":
        # Statewide tract queries are supported (verified against ACS 2024:
        # `for=tract:*&in=state:55` returns all 1,542 WI tracts in one call).
        # This used to raise when county_fips was absent; that restriction was
        # inherited from an older API vintage and cost 72x the requests.
        # block_group below still requires a county — its statewide response is
        # far larger and that path has not been verified.
        if not county_fips:
            return f"for=tract:*&in=state:{state}"
        county = county_fips.zfill(3)
        return f"for=tract:*&in=state:{state}%20county:{county}"
    elif geo_level == "block_group":
        if not county_fips:
            raise ValueError("county_fips is required for geo_level='block_group'")
        county = county_fips.zfill(3)
        return f"for=block+group:*&in=state:{state}%20county:{county}%20tract:*"
    elif geo_level == "county":
        return f"for=county:*&in=state:{state}"
    elif geo_level == "state":
        return f"for=state:{state}"
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level!r}. Use tract/block_group/county/state.")


def fetch_acs_table(
    year: int,
    variables: list[str],
    state_fips: str,
    county_fips: str | None = None,
    geo_level: str = "tract",
    subject: bool = False,
) -> list[dict]:
    """
    Fetch ACS 5-Year data for given variables at the specified geographic level.

    Parameters
    ----------
    year        : ACS release year (e.g. 2023 → 2019–2023 5-year estimates)
    variables   : list of ACS variable names, e.g. ["B19013_001E", "B19013_001M"]
    state_fips  : 2-digit state FIPS code (zero-padded string, e.g. "55")
    county_fips : 3-digit county FIPS code (zero-padded string, e.g. "025").
                  Required for tract/block_group; optional/ignored for county/state.
    geo_level   : "tract" | "block_group" | "county" | "state"
    subject     : True to use acs5/subject dataset (for S-prefix tables like S1701)

    Returns
    -------
    list of dicts: [{"geoid": "55025000100", "B19013_001E": 81738, ...}, ...]
    Null-sentinel values (-666666666) are converted to None.
    """
    base = _dataset_path(year, subject)
    geo = _geo_clause(geo_level, state_fips, county_fips)
    url = _build_url(base, variables, geo)

    req = urllib.request.Request(url, headers={"User-Agent": "policy-data-infrastructure/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Census API returned HTTP {resp.status} for {url}")
            # A keyless (or bad-key) request 302s to an HTML error page. urllib follows
            # it silently, so check where we actually landed before trying to parse JSON —
            # otherwise this surfaces as JSONDecodeError with no hint of the real cause.
            if "missing_key" in resp.url or "invalid_key" in resp.url:
                raise RuntimeError(
                    f"Census API rejected the key (redirected to {resp.url}). "
                    "Set a valid CENSUS_API_KEY — see lib.census.require_api_key()."
                )
            raw = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Census API HTTP error {exc.code}: {exc.reason}\nURL: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Census API network error: {exc.reason}\nURL: {url}") from exc

    if not raw or len(raw) < 2:
        return []

    header = raw[0]
    records = []
    for row in raw[1:]:
        geoid = build_geoid(row, header, geo_level)
        record: dict = {"geoid": geoid}
        # Include NAME field if present in response
        if "NAME" in header:
            record["NAME"] = row[header.index("NAME")]
        for var in variables:
            if var in header:
                raw_val = row[header.index(var)]
                # Preserve raw string; callers use safe_int/safe_float for conversion
                record[var] = _clean_sentinel(raw_val)
        records.append(record)

    time.sleep(RATE_LIMIT_DELAY)
    return records


def _clean_sentinel(val: str | None):
    """Replace null-sentinel strings with None; leave all other values as-is."""
    if val is None:
        return None
    try:
        if int(float(val)) == NULL_SENTINEL:
            return None
    except (ValueError, TypeError):
        pass
    return val


# ---------------------------------------------------------------------------
# Type-safe conversion helpers
# ---------------------------------------------------------------------------

def safe_int(val) -> int | None:
    """Convert a Census API value to int, returning None for null sentinel or non-parseable values."""
    if val is None:
        return None
    try:
        v = int(val)
        return None if v == NULL_SENTINEL else v
    except (ValueError, TypeError):
        return None


def safe_float(val) -> float | None:
    """Convert a Census API value to float, returning None for null sentinel or non-parseable values."""
    if val is None:
        return None
    try:
        v = float(val)
        return None if int(v) == NULL_SENTINEL else v
    except (ValueError, TypeError):
        return None


def safe_pct(num, denom) -> float | None:
    """
    Compute num/denom * 100, rounded to one decimal place.
    Returns None if either operand is None or denom is zero.
    """
    if num is None or denom is None or denom == 0:
        return None
    return round(num / denom * 100, 1)


def build_geoid(row: list, header: list[str], geo_level: str = "tract") -> str:
    """
    Concatenate FIPS component columns from a Census API row into a zero-padded GEOID.

    geo_level   expected GEOID length   FIPS columns used
    ---------   --------------------   -----------------
    state       2                      state
    county      5                      state + county
    tract       11                     state + county + tract
    block_group 12                     state + county + tract + block group
    """
    state = row[header.index("state")].zfill(2)
    if geo_level == "state":
        return state
    county = row[header.index("county")].zfill(3)
    if geo_level == "county":
        return state + county
    tract = row[header.index("tract")].zfill(6)
    if geo_level == "tract":
        return state + county + tract
    if geo_level == "block_group":
        bg = row[header.index("block group")].zfill(1)
        return state + county + tract + bg
    raise ValueError(f"Unsupported geo_level for GEOID construction: {geo_level!r}")
