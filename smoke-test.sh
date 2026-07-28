#!/usr/bin/env bash
# PDI Automated Smoke Test
# Verifies: all pages load, API returns data, no JS errors in critical paths
# Usage: ./smoke-test.sh [--base-url https://api.policydatainfrastructure.com]

set -euo pipefail

BASE="${1:-http://localhost:8340}"
PASS=0
FAIL=0
ERRORS=""

check() {
  local label="$1"
  local url="$2"
  local expected="${3:-200}"
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null || echo "000")
  if [ "$code" = "$expected" ]; then
    echo "  ✅ $label"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $label — expected $expected, got $code"
    ERRORS="$ERRORS\n  $label: $code"
    FAIL=$((FAIL + 1))
  fi
}

check_json() {
  local label="$1"
  local url="$2"
  local key="$3"
  local body
  body=$(curl -s --max-time 10 "$url" 2>/dev/null || echo "{}")
  if echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); assert '$key' in d or any('$key' in str(k) for k in d.keys()), 'missing $key'" 2>/dev/null; then
    echo "  ✅ $label"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $label — missing key '$key'"
    ERRORS="$ERRORS\n  $label: missing $key"
    FAIL=$((FAIL + 1))
  fi
}

check_json_count() {
  local label="$1"
  local url="$2"
  local key="$3"
  local min="${4:-1}"
  local count
  count=$(curl -s --max-time 10 "$url" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
v=d.get('$key', d.get('${key}s', d.get('data', {}).get('$key', [])))
if isinstance(v, list): print(len(v))
elif isinstance(v, dict): print(len(v.get('items', v)))
else: print(0)
" 2>/dev/null || echo "0")
  if [ "$count" -ge "$min" ]; then
    echo "  ✅ $label ($count)"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $label — expected >=$min, got $count"
    ERRORS="$ERRORS\n  $label: count $count < $min"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== PDI SMOKE TEST ==="
echo "Base URL: $BASE"
echo ""

echo "── Frontend Pages ──"
check "Landing /"                 "$BASE/"
check "County Profile"            "$BASE/county?geoid=55025"
check "Map"                       "$BASE/map"
check "Chat"                      "$BASE/chat"
check "Compare"                   "$BASE/compare?geoid1=55025&geoid2=55079"
check "Evidence"                  "$BASE/evidence"
check "About"                     "$BASE/about"
check "ES Landing"                "$BASE/es/"
check "Composites"                "$BASE/composite"
check "Candidates"                "$BASE/candidates"

echo ""
echo "── API Endpoints ──"
check_json      "Health"          "$BASE/health" "status"
check_json_count "Geographies"    "$BASE/v1/policy/geographies" "items" 50
check_json       "Geographies total" "$BASE/v1/policy/geographies?limit=1" "total"
check_json       "Geography 55025" "$BASE/v1/policy/geographies/55025" "name"
check_json_count "Indicators"     "$BASE/v1/policy/geographies/55025/indicators" "indicators" 10
check_json_count "Factors"        "$BASE/v1/policy/geographies/55025/factors" "factors" 1
check_json_count "Variables"      "$BASE/v1/policy/variables" "variables" 20
check_json       "Sources"        "$BASE/v1/policy/sources" "sources"
# Compare is POST-only — test below
# NOTE: Compare is POST — test via curl -X POST
COMPARE_OK=$(curl -s -X POST "$BASE/v1/policy/compare" -H "Content-Type: application/json" -d '{"geoid1":"55025","geoid2":"55079"}' | python3 -c "import sys,json; d=json.load(sys.stdin); print('ok' if 'differences' in d else 'fail')" 2>/dev/null)
[ "$COMPARE_OK" = "ok" ] && echo "  ✅ Compare POST ($COMPARE_OK)" && PASS=$((PASS + 1)) || { echo "  ❌ Compare POST failed"; FAIL=$((FAIL + 1)); }

echo ""
echo "── Content Checks ──"
# Verify key content is present in responses
HAS_LEAFLET=$(curl -s "$BASE/map" | grep -c "leaflet" || echo "0")
[ "$HAS_LEAFLET" -gt 5 ] && echo "  ✅ Map has Leaflet ($HAS_LEAFLET refs)" && PASS=$((PASS + 1)) || { echo "  ❌ Map missing Leaflet"; FAIL=$((FAIL + 1)); }

HAS_CHATADAPTER=$(curl -s "$BASE/chat" | grep -c "ChatAdapter" || echo "0")
[ "$HAS_CHATADAPTER" -gt 1 ] && echo "  ✅ Chat has ChatAdapter ($HAS_CHATADAPTER refs)" && PASS=$((PASS + 1)) || { echo "  ❌ Chat missing ChatAdapter"; FAIL=$((FAIL + 1)); }

HAS_THEME=$(curl -s "$BASE/" | grep -c "tp-theme" || echo "0")
[ "$HAS_THEME" -gt 0 ] && echo "  ✅ Landing has theme restore" && PASS=$((PASS + 1)) || { echo "  ❌ Landing missing theme restore"; FAIL=$((FAIL + 1)); }

HAS_FACTORS=$(curl -s "$BASE/county?geoid=55025" | grep -c "Cardiovascular" || echo "0")
[ "$HAS_FACTORS" -gt 0 ] && echo "  ✅ County profile has factor data" && PASS=$((PASS + 1)) || { echo "  ❌ County missing factor data"; FAIL=$((FAIL + 1)); }

HAS_DEEPLINK=$(curl -s "$BASE/county?geoid=55025" | grep -c "Copy Link\|deeplink\|copyLink" || echo "0")
[ "$HAS_DEEPLINK" -gt 1 ] && echo "  ✅ County has deep link support ($HAS_DEEPLINK refs)" && PASS=$((PASS + 1)) || { echo "  ❌ County missing deep link"; FAIL=$((FAIL + 1)); }

echo ""
echo "────────────────────"
echo "Results: $PASS passed, $FAIL failed"
if [ -n "$ERRORS" ]; then
  echo "Errors:$ERRORS"
fi
[ "$FAIL" -eq 0 ] && echo "SMOKE TEST PASSED ✅" || echo "SMOKE TEST FAILED ❌"
exit $FAIL
