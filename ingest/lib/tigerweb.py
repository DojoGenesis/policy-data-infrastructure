"""TIGERweb REST client — vintage-matched boundary geometry as GeoJSON.

Why this exists
---------------
The bulk cartographic-boundary GeoJSON files this repo used to download
(``https://www2.census.gov/geo/tiger/GENZ{year}/json/cb_{year}_{st}_{level}_500k.json``)
are gone. Verified 2026-07-26: ``GENZ2024/json/`` returns HTTP 404, while
``GENZ2024/shp/cb_2024_55_tract_500k.zip`` exists — the Census now publishes
these as shapefiles only, and this repo has no shapefile reader (no GDAL, no
pyshp) and a stdlib-only convention for ingest.

TIGERweb's ArcGIS REST service serves the same geometry as GeoJSON directly,
with two properties the flat files never had:

1. **Vintage-matched layers.** ``tigerWMS_ACS2024`` carries the exact tract
   boundaries the ACS 2020-2024 5-Year estimates are tabulated on, so the
   indicator join is guaranteed 1:1 rather than approximately-right. (Verified:
   1,542 WI tracts from the ACS API, 1,542 from this service.)
2. **Server-side generalization.** ``maxAllowableOffset`` trims coordinate
   density at the source. WI tracts at full resolution are ~21 MB; at 0.0005
   degrees (~55 m) they are ~750 KB with no visible difference in a choropleth.
   That difference is what makes a zero-build static Atlas viable.

Everything here is stdlib-only, per the ingest convention.
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

SERVICE_TMPL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
    "tigerWMS_ACS{year}/MapServer"
)

# Layer ids within tigerWMS_ACS{year}. Verified against the 2024 service
# 2026-07-26; the ids are stable across ACS vintages of this service.
LAYERS: dict[str, int] = {
    "state":        80,
    "county":       82,
    "tract":         8,
    "block_group":  10,
    "place":        28,
    # Political geography — used by the policy-impact overlays.
    "congressional": 54,   # 119th Congressional Districts
    "state_upper":   56,   # State Legislative Districts - Upper (WI Senate)
    "state_lower":   58,   # State Legislative Districts - Lower (WI Assembly)
    "school_unified": 14,
}

# Generalization in degrees. 0.0005 ~= 55 m — the level this repo ships at.
DEFAULT_SIMPLIFY = 0.0005

_TIMEOUT = 120
_RATE_DELAY = 0.5
_PAGE_SIZE = 500          # features per request; keeps each response well under
                          # the service transfer limit even at full resolution.

# Geometry-blob and bookkeeping fields TIGERweb returns that are never useful
# downstream and cost real bytes in a static-site payload.
_DROP_FIELDS = {"STGEOMETRY", "STGEOMETRY.AREA", "STGEOMETRY.LEN", "OBJECTID", "OID"}


def service_url(year: int) -> str:
    return SERVICE_TMPL.format(year=year)


def layer_id(level: str) -> int:
    try:
        return LAYERS[level]
    except KeyError:
        raise ValueError(
            f"Unsupported level: {level!r}. Choose from: {sorted(LAYERS)}"
        ) from None


def _where(state_fips: str, county_fips: str | None) -> str:
    clause = f"STATE='{state_fips.zfill(2)}'"
    if county_fips:
        clause += f" AND COUNTY='{county_fips.zfill(3)}'"
    return clause


def _get(url: str, params: dict) -> dict:
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        url,
        data=body,  # POST — where-clauses and field lists overflow a GET query string
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            raw = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"TIGERweb HTTP {exc.code}: {exc.reason} ({url})") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"TIGERweb network error: {exc.reason} ({url})") from exc

    # ArcGIS reports failures in a 200 body — check before trusting the payload.
    if isinstance(raw, dict) and "error" in raw:
        err = raw["error"]
        raise RuntimeError(
            f"TIGERweb error {err.get('code')}: {err.get('message')} "
            f"{'; '.join(err.get('details') or [])}"
        )
    return raw


def count_features(year: int, level: str, state_fips: str,
                   county_fips: str | None = None) -> int:
    """Return how many features the query matches, without fetching geometry.

    Cheap enough to call before every fetch as an expectation check.
    """
    url = f"{service_url(year)}/{layer_id(level)}/query"
    raw = _get(url, {
        "where": _where(state_fips, county_fips),
        "returnCountOnly": "true",
        "f": "json",
    })
    return int(raw.get("count", 0))


def fetch_features(
    year: int,
    level: str,
    state_fips: str,
    county_fips: str | None = None,
    simplify: float | None = DEFAULT_SIMPLIFY,
    out_fields: str = "*",
    verbose: bool = True,
) -> list[dict]:
    """Fetch boundary features as a list of GeoJSON Feature dicts.

    Pages through the service so the caller never has to think about the
    transfer limit. ``simplify`` is ``maxAllowableOffset`` in degrees; pass
    ``None`` for full resolution.
    """
    url = f"{service_url(year)}/{layer_id(level)}/query"
    where = _where(state_fips, county_fips)

    expected = count_features(year, level, state_fips, county_fips)
    if verbose:
        print(f"  TIGERweb ACS{year} layer {layer_id(level)} ({level}): "
              f"{expected} features expected")
    if expected == 0:
        return []

    features: list[dict] = []
    offset = 0
    while offset < expected:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(_PAGE_SIZE),
        }
        if simplify:
            params["maxAllowableOffset"] = str(simplify)

        raw = _get(url, params)
        page = raw.get("features") or []
        if not page:
            # Defensive: a service that stops returning features before the
            # count is reached would otherwise spin forever.
            if verbose:
                print(f"  warning: page at offset {offset} was empty; stopping "
                      f"with {len(features)}/{expected}")
            break

        for feat in page:
            props = {k: v for k, v in (feat.get("properties") or {}).items()
                     if k not in _DROP_FIELDS}
            features.append({
                "type": "Feature",
                "properties": props,
                "geometry": feat.get("geometry"),
            })

        offset += len(page)
        if verbose:
            print(f"    fetched {len(features)}/{expected}")
        if offset < expected:
            time.sleep(_RATE_DELAY)

    if len(features) != expected:
        print(f"  warning: got {len(features)} features, service reported "
              f"{expected} — treat the join as incomplete")
    return features


def feature_collection(features: list[dict], **metadata) -> dict:
    """Wrap features in a FeatureCollection, carrying provenance metadata."""
    fc = {"type": "FeatureCollection", "features": features}
    if metadata:
        fc["metadata"] = metadata
    return fc
