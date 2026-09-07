#!/usr/bin/env bash
# End-to-end check against a RUNNING stack, over HTTP.
#
# `scripts/e2e_health_data.py` exercises the read authority in-process. This
# one goes through the wire: the login flow, the REST endpoint the web client
# uses, the MCP surface an external client sees, and — last — the PHI sentinel,
# which is the only check that can tell you whether the redaction holds in a
# real process rather than in a unit test.
#
#   ./deploy.sh                 # or docker compose up -d
#   scripts/e2e_docker.sh       # BASE=http://host:port scripts/e2e_docker.sh
#
# Exit status is the number of failed checks.

set -uo pipefail

BASE="${BASE:-http://127.0.0.1:18060}"
EMAIL="${EMAIL:-caregiver@mirobody.ai}"
CODE="${CODE:-111111}"
CONTAINER="${CONTAINER:-mirobody-mirobody-1}"
FAILURES=0

# Everything logged from here on is THIS run's. `docker logs` is cumulative, so
# a check that greps the whole stream reports yesterday's fixed leak forever.
# The trailing Z is load-bearing: without it `docker logs --since` reads the
# stamp in the HOST's zone, which on a +08 machine reaches eight hours further
# back than intended and re-reports leaks that were fixed in between.
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

ok()   { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s%s\n' "$1" "${2:+ — $2}"; FAILURES=$((FAILURES + 1)); }
check(){ if [ "$1" = "0" ]; then ok "$2"; else fail "$2" "${3:-}"; fi; }

jqp() { python3 -c "import sys,json;d=json.load(sys.stdin);print(eval(sys.argv[1],{'d':d}))" "$1" 2>/dev/null; }

echo
echo "mirobody end-to-end — $BASE"
echo

# ── 1. the door ──────────────────────────────────────────────────────────────
curl -sf -o /dev/null "$BASE/" && ok "the server answers" || fail "the server answers"

curl -s -X POST "$BASE/email/login" -H 'Content-Type: application/json' \
     -d "{\"email\":\"$EMAIL\",\"code\":\"$CODE\"}" >/dev/null
TOKEN=$(curl -s -X POST "$BASE/email/verify" -H 'Content-Type: application/json' \
        -d "{\"email\":\"$EMAIL\",\"code\":\"$CODE\"}" | jqp "d['data']['access_token']")
[ -n "${TOKEN:-}" ] && ok "sign in" || { fail "sign in" "no access token"; echo; exit 1; }
AUTH="Authorization: Bearer $TOKEN"

# ── 2. the REST surface the web client uses ──────────────────────────────────
CAT=$(curl -s -H "$AUTH" "$BASE/api/v1/health-indicators")
check "$([ "$(echo "$CAT" | jqp "d['code']")" = "0" ] && echo 0 || echo 1)" \
      "GET /health-indicators (catalogue)" "$(echo "$CAT" | head -c 160)"

ROWS=$(echo "$CAT" | jqp "len(d['data']['rows'])")
check "$([ "${ROWS:-0}" -gt 0 ] && echo 0 || echo 1)" "the catalogue has rows" "rows=$ROWS"

SEM=$(echo "$CAT" | jqp "d['data']['window']['semantics']")
check "$([ "$SEM" = "tz_exact" ] || [ "$SEM" = "date_padded_naive" ] && echo 0 || echo 1)" \
      "the answer declares its window semantics" "semantics=$SEM"

FIRST=$(echo "$CAT" | jqp "d['data']['rows'][0]['indicator']")
READ=$(curl -s -H "$AUTH" --get --data-urlencode "indicators=$FIRST" --data-urlencode "limit=3" \
       "$BASE/api/v1/health-indicators")
check "$([ "$(echo "$READ" | jqp "d['code']")" = "0" ] && echo 0 || echo 1)" \
      "reading a named indicator" "$(echo "$READ" | head -c 160)"

# A name with a comma in it must round-trip: the catalogue is where the model
# and the browser both copy names FROM.
COMMA=$(echo "$CAT" | jqp "next((r['indicator'] for r in d['data']['rows'] if ',' in r['indicator']), '')")
if [ -n "$COMMA" ]; then
  N=$(curl -s -H "$AUTH" --get --data-urlencode "indicators=$COMMA" --data-urlencode "limit=2" \
      "$BASE/api/v1/health-indicators" | jqp "len(d['data']['rows'])")
  check "$([ "${N:-0}" -gt 0 ] && echo 0 || echo 1)" "an indicator whose NAME has a comma" "$COMMA -> $N rows"
fi

BAD=$(curl -s -H "$AUTH" --get --data-urlencode "resolution=day" "$BASE/api/v1/health-indicators")
check "$([ "$(echo "$BAD" | jqp "d['code']")" != "0" ] && echo 0 || echo 1)" \
      "a refused combination is refused, not defaulted"

# ── 3. the MCP surface an external client sees ───────────────────────────────
#
# TWO surfaces, and they are deliberately different.
#
# `POST /mcp` is capability discovery and does NOT identify the caller — the
# `tools/list` branch never reads the JWT, so a Bearer token here still gets the
# FULL advertised list. That is the surface an external client sees before it
# has a personal URL.
#
# `POST /personal/mcp` mints `/mcp/<secret>`, and THAT path resolves a user, so
# `_data_gated_tools` hides the tools this person has no data for. Checking only
# the first surface would have missed the gate entirely, which is what this
# script used to do — while also naming a tool that no longer exists
# (`query_health_data` became `query_health_indicators`, with medications split
# out into `query_medications`, CHANGELOG 1.4.0).
LIST=$(curl -s -X POST "$BASE/mcp" -H "$AUTH" -H 'Content-Type: application/json' \
       -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}')
NAMES=$(echo "$LIST" | jqp "','.join(sorted(t['name'] for t in d['result']['tools']))")
EXPECT="convert_unit,get_genetic_data,normalize_unit,query_health_indicators,query_medications,resolve_indicator"
check "$([ "$NAMES" = "$EXPECT" ] && echo 0 || echo 1)" \
      "tools/list advertises the whole surface to an unidentified caller" "got: $NAMES"

# The personal URL identifies the caller, so the data gate applies: a tool for
# data this person does not have must not be listed at all.
PERSONAL=$(curl -s -X POST "$BASE/personal/mcp" -H "$AUTH" -H 'Content-Type: application/json' -d '{}')
MCP_URL=$(echo "$PERSONAL" | jqp "d['data']['url']")
if [ -n "${MCP_URL:-}" ]; then
    GATED=$(curl -s -X POST "$MCP_URL" -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}')
    GNAMES=$(echo "$GATED" | jqp "','.join(sorted(t['name'] for t in d['result']['tools']))")
    check "$(echo "$GNAMES" | grep -q 'query_health_indicators' && echo 0 || echo 1)" \
          "the personal URL lists the tool this person HAS data for" "got: $GNAMES"
    check "$([ "$GNAMES" = "$EXPECT" ] && echo 1 || echo 0)" \
          "the personal URL hides what this person has no data for" "got: $GNAMES"
else
    fail "POST /personal/mcp returns a URL" "$(echo "$PERSONAL" | head -c 200)"
fi

CALL=$(curl -s -X POST "$BASE/mcp" -H "$AUTH" -H 'Content-Type: application/json' \
       -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_health_indicators","arguments":{"aggregate":"latest","keywords":["weight"]}}}')
check "$(echo "$CALL" | grep -q '"result"' && echo 0 || echo 1)" \
      "tools/call query_health_indicators" "$(echo "$CALL" | head -c 200)"

REFUSED=$(curl -s -X POST "$BASE/mcp" -H "$AUTH" -H 'Content-Type: application/json' \
          -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query_health_indicators","arguments":{"kind":"medications","resolution":"day"}}}')
# Refused at the MCP argument validator, BEFORE the tool runs, so the marker is
# the validator's text and not the tool envelope's `invalid_arguments` kind —
# this used to grep for the latter and reported a working refusal as a failure.
# What matters either way: the offending name is echoed, and the accepted set is
# spelled out, or the model has nothing to correct against.
check "$(echo "$REFUSED" | grep -q 'Unknown argument' && echo 0 || echo 1)" \
      "a wrong parameter comes back as a structured refusal" "$(echo "$REFUSED" | head -c 200)"
check "$(echo "$REFUSED" | grep -q 'kind' && echo 0 || echo 1)" \
      "the refusal names the offending parameter"
check "$(echo "$REFUSED" | grep -q 'user_info' && echo 1 || echo 0)" \
      "the refusal does not advertise the injected user_info"

# ── 4. the in-process suite, against this database ───────────────────────────
if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker exec "$CONTAINER" sh -c \
    'cd /app && /root/venv/bin/python scripts/e2e_health_data.py --user 2' >/tmp/e2e_inproc.txt 2>&1
  INNER=$?
  check "$INNER" "scripts/e2e_health_data.py inside the container" "see /tmp/e2e_inproc.txt"
fi

# ── 5. the PHI sentinel ──────────────────────────────────────────────────────
# Everything above put real health data through the process. If the redaction
# holds, none of it is in the log.
CANARY=$(docker exec "$CONTAINER" sh -c \
  'cd /app && /root/venv/bin/python -c "from mirobody.server.demo import PHI_CANARY; print(PHI_CANARY)"' 2>/dev/null \
  | tr -d '\r' | tail -1)
CANARY="${CANARY:-PHI-CANARY-3f9a}"
HITS=$(docker logs --since "$SINCE" "$CONTAINER" 2>&1 | grep -cF "${CANARY##* }")
check "$([ "$HITS" = "0" ] && echo 0 || echo 1)" "no PHI canary in the container's logs" "$HITS hit(s)"

# The canary tests ONE channel — a comment. A rendered result is another, and
# it is the one that actually leaked: the MCP handler logged the first hundred
# serialised characters of every result, which for `tools/call` are the
# person's readings. So check for the SHAPE of a leaked answer too. This is the
# same rule `testing.phi_lint` applies to the source, applied to the output.
SHAPES=$(docker logs --since "$SINCE" "$CONTAINER" 2>&1 \
  | grep -cE '\(constants:|indicator\|(date\|)?time\|value|"text\\":\\"\{' )
check "$([ "$SHAPES" = "0" ] && echo 0 || echo 1)" \
      "no rendered health data in the container's logs" "$SHAPES line(s)"

echo
echo "$FAILURES failed"
echo
exit "$FAILURES"
