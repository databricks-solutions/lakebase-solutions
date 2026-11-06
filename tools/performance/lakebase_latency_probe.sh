#!/usr/bin/env bash
#
# lakebase_latency_probe.sh — apples-to-apples latency decomposition for Lakebase
# (Postgres), the psql/shell twin of oracle_latency_probe.sh. Emits the SAME ledger
# so the two engines compare line-for-line from the SAME vantage point (your laptop).
#
# ============================================================================
# HOW TO RUN
# ============================================================================
#   psql does every measurement. The one thing psql CANNOT do is mint Lakebase's
#   short-lived OAuth token or resolve the endpoint host — only the Databricks CLI
#   can. So there are two ways to connect:
#
#   (A) Let the script resolve host + token from a Databricks CLI profile (easiest):
#
#         ./lakebase_latency_probe.sh --profile <databricks-cli-profile> \
#                                     --project-id <lakebase-project-id>
#
#       Host, username, and a ~1h OAuth token are fetched via `databricks postgres`
#       once at startup; psql does the rest. (branch/endpoint/database default to
#       production/primary/databricks_postgres.)
#
#   (B) Pure psql — you pre-export standard libpq vars (no Databricks dependency),
#       mirroring Oracle's ORA_CONN exactly:
#
#         export PGHOST=ep-xxxx.database.cloud.databricks.com PGUSER='you@databricks.com'
#         export PGPASSWORD='<oauth-token>' PGDATABASE=databricks_postgres PGSSLMODE=require
#         ./lakebase_latency_probe.sh
#
#   Requirements: `psql` on PATH (set PSQL=/path/to/psql otherwise). For path (A),
#   `databricks` CLI + a valid profile. Works on macOS (bash 3.2) and Linux.
#
# ----------------------------------------------------------------------------
# OPTIONS  (names mirror oracle_latency_probe.sh)
#   --sql FILE | --sql-inline "SELECT ..."   query to profile (default = built-in example)
#   --iters N        measured runs (default 30)      --warmup N    discarded warmups (default 5)
#   --rtt N          empty round trips (default 1000) --connect N   fresh-login samples (default 10)
#   --arraysize N    rows per fetch (default 0 = fetch-all; set = psql FETCH_COUNT cursor batch;
#                    MATCH Oracle's --arraysize for a fair fetch/round-trip comparison)
#   --pgss-match STR substring to find this query in pg_stat_statements (default: auto from SQL)
#   --no-result-cache  accepted for symmetry; Postgres has NO query result cache (no-op, see note)
#   --trace          also dump EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, WAL) per-node detail
#   --outdir DIR     where to write artifacts (default ./lakebase_probe_out)
#   --profile NAME   Databricks CLI profile (path A)  --project-id ID  Lakebase project
#   --branch NAME    default production               --endpoint NAME  default primary
#   --database NAME  default databricks_postgres
#
# PRODUCES a latency ledger (min/median over N runs) mirroring the Oracle probe:
#   Connect (TCP+TLS+auth) ....... fresh psql login wall-clock
#   Network RTT (empty) .......... bulk SELECT 1 / N  (resolution-independent)
#   Server planning .............. EXPLAIN (ANALYZE) -> Planning Time
#   Server execution ............. pg_stat_statements mean (clean, no instrumentation),
#                                  EXPLAIN exec as upper-bound fallback
#   Logical vs physical reads .... EXPLAIN (BUFFERS): shared hit vs read (+ I/O time)
#   CLIENT TOTAL (wall) .......... psql \timing
#   Transfer + client (residual) . total - RTT - (planning + exec)
#
# TIERS (auto-detected; degrade gracefully):
#   A  \timing + bulk RTT + connect        — needs only CONNECT
#   B  EXPLAIN (ANALYZE, BUFFERS)          — planning/exec + logical/physical reads (any user)
#   C  pg_stat_statements clean server ms  — needs the extension + SELECT on the view
#   D  EXPLAIN (... SETTINGS, WAL) detail   — optional, --trace (the 10046/tkprof analogue)
#
# NOTE ON CACHES: Oracle has a result cache you must bypass (--no-result-cache). Postgres
# has no such thing — the only cache dimension is the shared buffer pool, reported here as
# "shared hit" (warm) vs "shared read" (cold). You can't cheaply flush it on Lakebase;
# label the first run cold and read min/median of the warm runs. (checklist item 4.)
#
# SECURITY: never hardcode the token. In path (A) it is minted at runtime and only lives in
# this process's env. In path (B) keep PGPASSWORD in your shell env, not on disk.
# ============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------- defaults
PSQL="${PSQL:-psql}"
ITERS=30
WARMUP=5
RTT_ITERS=1000
CONNECT_ITERS=10
ARRAYSIZE=0                 # 0 = psql fetch-all. Set to match Oracle --arraysize (drives round trips).
NO_RESULT_CACHE=0
DO_TRACE=0
OUTDIR="./lakebase_probe_out"
SQL_FILE=""
SQL_INLINE=""
PGSS_MATCH=""
PROFILE=""
PROJECT_ID=""
BRANCH="production"
ENDPOINT="primary"
DATABASE="databricks_postgres"

DEFAULT_SQL="select count(1)
from demo.orders o,
     demo.order_lines l
where o.order_id = l.order_id
  and o.region_id = l.region_id
  and o.customer_id = '12345'"

# ---------------------------------------------------------------------------- args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql)             SQL_FILE="$2"; shift 2 ;;
    --sql-inline)      SQL_INLINE="$2"; shift 2 ;;
    --iters)           ITERS="$2"; shift 2 ;;
    --warmup)          WARMUP="$2"; shift 2 ;;
    --rtt)             RTT_ITERS="$2"; shift 2 ;;
    --connect)         CONNECT_ITERS="$2"; shift 2 ;;
    --arraysize)       ARRAYSIZE="$2"; shift 2 ;;
    --pgss-match)      PGSS_MATCH="$2"; shift 2 ;;
    --no-result-cache) NO_RESULT_CACHE=1; shift ;;
    --trace)           DO_TRACE=1; shift ;;
    --outdir)          OUTDIR="$2"; shift 2 ;;
    --profile)         PROFILE="$2"; shift 2 ;;
    --project-id)      PROJECT_ID="$2"; shift 2 ;;
    --branch)          BRANCH="$2"; shift 2 ;;
    --endpoint)        ENDPOINT="$2"; shift 2 ;;
    --database)        DATABASE="$2"; shift 2 ;;
    -h|--help)         grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

command -v "$PSQL" >/dev/null 2>&1 || { echo "psql not found on PATH (set PSQL=...)." >&2; exit 3; }

# ---------------------------------------------------------------------------- resolve connection
# Path (A): resolve host + user + token from a Databricks CLI profile (psql can't mint the token).
# Path (B): PGHOST/PGUSER/PGPASSWORD already exported -> use as-is (pure psql, like ORA_CONN).
if [[ -n "${PGHOST:-}" && -n "${PGPASSWORD:-}" ]]; then
  echo "[conn] using pre-set libpq env (PGHOST/PGUSER/PGPASSWORD) — pure psql path"
  : "${PGUSER:?set PGUSER (your workspace username) alongside PGHOST/PGPASSWORD}"
else
  : "${PROFILE:?Set --profile (and --project-id), or pre-export PGHOST/PGUSER/PGPASSWORD for pure-psql mode}"
  command -v databricks >/dev/null 2>&1 || { echo "databricks CLI not found; needed to mint the token (or use pure-psql mode)." >&2; exit 3; }
  if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID=$(databricks postgres list-projects --profile "$PROFILE" -o json \
      | python3 -I -c "import json,sys; d=json.load(sys.stdin); p=d if isinstance(d,list) else d.get('projects',d); print(p[0]['name'].split('/')[-1])")
    echo "[conn] no --project-id; auto-picked: $PROJECT_ID"
  fi
  EP="projects/${PROJECT_ID}/branches/${BRANCH}/endpoints/${ENDPOINT}"
  echo "[conn] resolving host + OAuth token from profile '$PROFILE' endpoint '$EP'..."
  PGHOST=$(databricks postgres get-endpoint "$EP" --profile "$PROFILE" -o json \
    | python3 -I -c "import json,sys; print(json.load(sys.stdin)['status']['hosts']['host'])")
  PGPASSWORD=$(databricks postgres generate-database-credential "$EP" --profile "$PROFILE" -o json \
    | python3 -I -c "import json,sys; print(json.load(sys.stdin)['token'])")
  PGUSER=$(databricks current-user me --profile "$PROFILE" -o json \
    | python3 -I -c "import json,sys; d=json.load(sys.stdin); print(d.get('userName'))")
  export PGHOST PGPASSWORD PGUSER
fi
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-$DATABASE}"
export PGSSLMODE="${PGSSLMODE:-require}"
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-30}"
export PGAPPNAME="lakebase_latency_probe"

# ---------------------------------------------------------------------------- high-res timer (copied from the Oracle probe for symmetry)
DATECMD="date"
if [[ -n "${EPOCHREALTIME:-}" ]]; then
  NOW_METHOD="epoch"
elif date +%N 2>/dev/null | grep -qE '^[0-9]+$'; then
  NOW_METHOD="gnudate"; DATECMD="date"
elif command -v gdate >/dev/null 2>&1 && gdate +%N 2>/dev/null | grep -qE '^[0-9]+$'; then
  NOW_METHOD="gnudate"; DATECMD="gdate"
elif command -v perl >/dev/null 2>&1; then
  NOW_METHOD="perl"
else
  NOW_METHOD="coarse"
  echo "WARN: only integer-second timing available; connect/RTT precision will be coarse." >&2
fi
echo "[timer] method=$NOW_METHOD"

now() {
  case "$NOW_METHOD" in
    epoch)   printf '%s' "$EPOCHREALTIME" ;;
    gnudate) "$DATECMD" +%s.%N ;;
    perl)    perl -MTime::HiRes=time -e 'printf "%.6f", time' ;;
    coarse)  date +%s ;;
  esac
}

mkdir -p "$OUTDIR"

if [[ -n "$SQL_FILE" ]]; then
  TARGET_SQL="$(cat "$SQL_FILE")"
elif [[ -n "$SQL_INLINE" ]]; then
  TARGET_SQL="$SQL_INLINE"
else
  TARGET_SQL="$DEFAULT_SQL"
fi
TARGET_SQL="${TARGET_SQL%;}"                      # strip a trailing semicolon if present

# Auto-derive a pg_stat_statements match token: first identifier >=5 chars that isn't a keyword.
if [[ -z "$PGSS_MATCH" ]]; then
  PGSS_MATCH=$(printf '%s' "$TARGET_SQL" | grep -oE '[A-Za-z_][A-Za-z0-9_.]{4,}' \
    | grep -viE '^(select|count|where|order|group|inner|outer|cross|limit|fetch|offset|having|distinct)$' \
    | head -1)
  PGSS_MATCH="${PGSS_MATCH:-count}"
fi

echo "=============================================================="
echo " Lakebase (Postgres) latency probe"
echo "  host=$PGHOST db=$PGDATABASE user=$PGUSER"
echo "  iters=$ITERS warmup=$WARMUP rtt=$RTT_ITERS connect=$CONNECT_ITERS arraysize=$ARRAYSIZE"
echo "  pgss_match='$PGSS_MATCH' trace=$DO_TRACE outdir=$OUTDIR"
echo "--------------------------------------------------------------"
echo "$TARGET_SQL"
echo "=============================================================="

# ---------------------------------------------------------------------------- helpers
# psql, hardened for scripting: no .psqlrc, quiet, unaligned/tuples-only, no pager.
PSQL_BASE=(-X -q -P pager=off -A -t -v ON_ERROR_STOP=0)
runpsql() { printf '%s\n' "$1" | "$PSQL" "${PSQL_BASE[@]}"; }

# Wall-clock a shell command group -> elapsed seconds (uses best clock from now()).
timeit() {
  local start end
  start=$(now)
  "$@" >/dev/null 2>&1 || true
  end=$(now)
  awk -v s="$start" -v e="$end" 'BEGIN{printf "%.6f", e - s}'
}

# min + median of newline-separated numbers on stdin -> "min median" (portable; sort -n).
minmed() {
  local sorted n min med a b
  sorted=$(grep -E '[0-9]' | sort -n)
  n=$(printf '%s\n' "$sorted" | grep -c .)
  if [[ "$n" -eq 0 ]]; then echo "0 0"; return; fi
  min=$(printf '%s\n' "$sorted" | head -1)
  if (( n % 2 )); then
    med=$(printf '%s\n' "$sorted" | sed -n "$(( (n + 1) / 2 ))p")
  else
    a=$(printf '%s\n' "$sorted" | sed -n "$(( n / 2 ))p")
    b=$(printf '%s\n' "$sorted" | sed -n "$(( n / 2 + 1 ))p")
    med=$(awk -v a="$a" -v b="$b" 'BEGIN{printf "%.3f", (a + b) / 2}')
  fi
  # trailing newline so `read -r` consuming this returns 0 under `set -e`
  printf "%s %s\n" "$min" "$med"
}

# ---------------------------------------------------------------------------- preflight: version, track_io_timing, pg_stat_statements
echo
echo "[preflight] version + capabilities..."
PRE_OUT="$(runpsql "SELECT 'VER|'||version();
SELECT 'IOT|'||current_setting('track_io_timing');
SELECT 'PGSS|'||CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements') THEN 'YES' ELSE 'NO' END;" 2>/dev/null || true)"
echo "$PRE_OUT" | grep -E '^(VER|IOT|PGSS)\|' | sed 's/^/    /' || true
HAVE_PGSS=$(echo "$PRE_OUT" | grep -q 'PGSS|YES' && echo 1 || echo 0)

# Resolve the endpoint IP + flag private vs public path (mirrors the notebook's PrivateLink check).
echo
echo "[vantage] resolving $PGHOST ..."
python3 -I - "$PGHOST" <<'PY' 2>/dev/null || echo "    (could not resolve host)"
import socket, ipaddress, sys
host = sys.argv[1]
try:
    ips = sorted({r[4][0] for r in socket.getaddrinfo(host, 5432, proto=socket.IPPROTO_TCP)})
    for ip in ips:
        try:
            kind = "PRIVATE (RFC1918 — PrivateLink/VPC path)" if ipaddress.ip_address(ip).is_private else "PUBLIC (internet-routable)"
        except ValueError:
            kind = "?"
        print(f"    {host} -> {ip}  [{kind}]")
    print("    (run from the SAME network your real client uses so this RTT is representative)")
except Exception as e:
    print(f"    could not resolve: {e}")
PY

# ---------------------------------------------------------------------------- 1) connect time + RTT baseline
# connect = fresh psql login (TCP+TLS+auth) wall-clock, running a trivial SELECT 1 (like the Oracle login).
# RTT     = one session, N× SELECT 1, minus a bare login baseline, / N -> immune to \timing resolution.
echo
echo "[1] connect time + network RTT..."
connect_ms_list=""
for _ in $(seq 1 "$CONNECT_ITERS"); do
  t=$(timeit runpsql "SELECT 1;")
  connect_ms_list+="$(awk -v x="$t" 'BEGIN{printf "%.3f", x*1000}')"$'\n'
done
read -r CONNECT_MIN CONNECT_MED < <(printf '%s' "$connect_ms_list" | minmed)

RTT_BODY=""
for _ in $(seq 1 "$RTT_ITERS"); do RTT_BODY+="SELECT 1;"$'\n'; done
BASE_WALL=$(timeit runpsql "")                  # connect + immediate EOF exit = login baseline
RTT_WALL=$(timeit runpsql "$RTT_BODY")
RTT_MS=$(awk -v r="$RTT_WALL" -v b="$BASE_WALL" -v n="$RTT_ITERS" 'BEGIN{d=(r-b)/n*1000; if(d<0)d=0; printf "%.3f", d}')
echo "    connect  min=${CONNECT_MIN}ms  median=${CONNECT_MED}ms"
echo "    rtt/stmt ~${RTT_MS}ms  (bulk ${RTT_ITERS}x: ${RTT_WALL}s minus baseline ${BASE_WALL}s)"

# ---------------------------------------------------------------------------- 2) warmup
echo
echo "[2] warmup (${WARMUP}x)..."
WARM_BODY=""
for _ in $(seq 1 "$WARMUP"); do WARM_BODY+="$TARGET_SQL;"$'\n'; done
runpsql "$WARM_BODY" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- 3) client total wall-clock (\timing)
echo
echo "[3] client total wall-clock (${ITERS}x, \\timing)..."
TIMING_BODY="\\timing on"$'\n'
[[ "$ARRAYSIZE" -gt 0 ]] && TIMING_BODY+="\\set FETCH_COUNT $ARRAYSIZE"$'\n'
for _ in $(seq 1 "$ITERS"); do TIMING_BODY+="$TARGET_SQL;"$'\n'; done
runpsql "$TIMING_BODY" > "$OUTDIR/timing.txt" 2>&1 || true
# psql prints "Time: 1.234 ms" after each statement.
total_ms_list=$( { grep -E '^Time:' "$OUTDIR/timing.txt" | awk '{print $2}'; } || true)
if [[ -n "$total_ms_list" ]]; then
  read -r TOTAL_MIN TOTAL_MED < <(printf '%s' "$total_ms_list" | minmed)
else
  TOTAL_MIN=0; TOTAL_MED=0
  echo "    WARN: no Time: lines parsed — check $OUTDIR/timing.txt"
fi
echo "    total   min=${TOTAL_MIN}ms  median=${TOTAL_MED}ms"

# ---------------------------------------------------------------------------- 4) EXPLAIN (ANALYZE, BUFFERS): planning, exec, logical/physical reads (Tier B)
echo
echo "[4] EXPLAIN (ANALYZE, BUFFERS) — planning, execution, buffers..."
runpsql "SET track_io_timing = on;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) $TARGET_SQL;" > "$OUTDIR/explain.txt" 2>&1 || true
# `|| true`: these greps may legitimately find nothing (set -e + pipefail would otherwise abort).
PLAN_MS=$( { grep -E 'Planning Time:' "$OUTDIR/explain.txt"  | tail -1 | awk '{print $(NF-1)}'; } || true)
EXEC_MS=$( { grep -E 'Execution Time:' "$OUTDIR/explain.txt" | tail -1 | awk '{print $(NF-1)}'; } || true)
PLAN_MS=${PLAN_MS:-0}; EXEC_MS=${EXEC_MS:-0}
# Sum shared hit/read across plan nodes (only lines that mention "shared").
read -r SHARED_HIT SHARED_READ < <(awk '/Buffers:/ && /shared/ {
    for (i=1;i<=NF;i++){ if($i ~ /^hit=/){sub("hit=","",$i); h+=$i} if($i ~ /^read=/){sub("read=","",$i); r+=$i} }
  } END { printf "%d %d\n", h+0, r+0 }' "$OUTDIR/explain.txt")
# I/O Timings only appear when track_io_timing=on; absent is normal -> tolerate the empty grep.
IO_READ_MS=$( { grep -oE 'I/O Timings: .*read=[0-9.]+' "$OUTDIR/explain.txt" | grep -oE 'read=[0-9.]+' | awk -F= '{s+=$2} END{printf "%.3f", s+0}'; } || true)
IO_READ_MS=${IO_READ_MS:-0.000}
echo "    planning=${PLAN_MS}ms  execution=${EXEC_MS}ms (EXPLAIN, TIMING overhead = upper bound)"
echo "    buffers: shared hit=${SHARED_HIT} (cache)  read=${SHARED_READ} (storage)  io_read=${IO_READ_MS}ms"

# ---------------------------------------------------------------------------- 5) pg_stat_statements clean server elapsed (Tier C)
SERVER_MS=""
if [[ "$HAVE_PGSS" -eq 1 ]]; then
  echo
  echo "[5] pg_stat_statements clean server time (no instrumentation)..."
  BEFORE=$( { runpsql "SELECT COALESCE(sum(calls),0)||'|'||COALESCE(sum(total_exec_time),0) FROM pg_stat_statements WHERE query LIKE '%${PGSS_MATCH}%';" 2>/dev/null | head -1; } || true)
  WL_BODY=""
  for _ in $(seq 1 "$ITERS"); do WL_BODY+="$TARGET_SQL;"$'\n'; done
  runpsql "$WL_BODY" >/dev/null 2>&1 || true
  AFTER=$( { runpsql "SELECT COALESCE(sum(calls),0)||'|'||COALESCE(sum(total_exec_time),0) FROM pg_stat_statements WHERE query LIKE '%${PGSS_MATCH}%';" 2>/dev/null | head -1; } || true)
  SERVER_MS=$(awk -F'|' -v b="$BEFORE" -v a="$AFTER" '
    BEGIN{ split(b,B,"|"); split(a,A,"|");
      dc=A[1]-B[1]; dt=A[2]-B[2];
      if(dc<=0){ print ""; } else { printf "%.3f", dt/dc } }')
  if [[ -z "$SERVER_MS" ]]; then
    echo "    (no pg_stat_statements rows matched '%${PGSS_MATCH}%' — set --pgss-match to a token in your query)"
  else
    echo "    server exec/call=${SERVER_MS}ms  (clean mean over ${ITERS} calls, match='%${PGSS_MATCH}%')"
  fi
else
  echo
  echo "[5] pg_stat_statements not installed — using EXPLAIN execution as the server number (upper bound)."
fi

# ---------------------------------------------------------------------------- 6) optional deep per-node detail (Tier D, the 10046/tkprof analogue)
if [[ "$DO_TRACE" -eq 1 ]]; then
  echo
  echo "[6] deep EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, WAL)..."
  runpsql "SET track_io_timing = on;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, WAL) $TARGET_SQL;" > "$OUTDIR/explain_deep.txt" 2>&1 || true
  echo "    -> $OUTDIR/explain_deep.txt  (per-node actual time/rows/loops, buffers, settings, WAL)"
fi

# ---------------------------------------------------------------------------- ledger
# Prefer the clean pg_stat_statements server time; fall back to EXPLAIN exec (upper bound).
SERVER_EXEC="${SERVER_MS:-$EXEC_MS}"
SERVER_TOTAL=$(awk -v s="$SERVER_EXEC" -v p="$PLAN_MS" 'BEGIN{printf "%.3f", s+p}')
RESID=$(awk -v t="$TOTAL_MED" -v n="$RTT_MS" -v s="$SERVER_TOTAL" 'BEGIN{v=t-n-s; if(v<0)v=0; printf "%.3f", v}')

echo
echo "=============================================================="
echo " LATENCY LEDGER (Lakebase / Postgres)   [all values ms]"
echo "--------------------------------------------------------------"
printf "  %-32s %10s %10s\n" "line_item" "min" "median"
printf "  %-32s %10s %10s\n" "Connect (TCP+TLS+auth)" "$CONNECT_MIN" "$CONNECT_MED"
printf "  %-32s %10s %10s\n" "Network RTT (empty)" "$RTT_MS" "$RTT_MS"
printf "  %-32s %10s %10s\n" "Server planning (EXPLAIN)" "$PLAN_MS" "$PLAN_MS"
printf "  %-32s %10s %10s   (%s)\n" "Server execution" "$SERVER_EXEC" "$SERVER_EXEC" \
  "$([[ -n "$SERVER_MS" ]] && echo 'pg_stat_statements, clean' || echo 'EXPLAIN exec = upper bound')"
printf "  %-32s %10s %10s\n" "Transfer + client (residual)" "$RESID" "$RESID"
printf "  %-32s %10s %10s\n" "CLIENT TOTAL (wall)" "$TOTAL_MIN" "$TOTAL_MED"
echo "--------------------------------------------------------------"
echo "  shared hit (logical / cache)     : $SHARED_HIT"
echo "  shared read (physical / storage) : $SHARED_READ   (0 = fully cached / warm)"
echo "  I/O read time                    : ${IO_READ_MS}ms"
echo "  fetch batch (FETCH_COUNT)        : $([[ $ARRAYSIZE -gt 0 ]] && echo "$ARRAYSIZE" || echo 'all (fetch-all)')   <-- match Oracle --arraysize"
echo "  result cache                     : N/A for Postgres (warm/cold = shared hit vs read above)"
echo "=============================================================="
echo "Artifacts in: $OUTDIR/  (timing.txt, explain.txt$([[ $DO_TRACE -eq 1 ]] && echo ', explain_deep.txt'))"
echo
echo "Compare to Oracle: line up 'Server planning + execution' first (vantage-independent);"
echo "then use 'Network RTT' to explain any CLIENT TOTAL gap. Keep SQL/predicates/indexes/volume equal."
