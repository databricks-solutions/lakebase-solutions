#!/usr/bin/env bash
#
# oracle_latency_probe.sh — apples-to-apples latency decomposition for Oracle,
# mirroring lakebase_latency_probe.py so the two engines compare line-for-line.
#
# ============================================================================
# HOW TO RUN  (you only need to supply ONE thing: the Oracle connection)
# ============================================================================
#   1. Point it at your Oracle. Easiest is the SAME TNS alias you use in SQL
#      Developer (e.g. MYDB), so no hostname/port/service needed:
#
#        export ORA_CONN='USERNAME/PASSWORD@MYDB'
#
#      Other accepted forms:
#        export ORA_CONN='USERNAME/PASSWORD@//host:1521/service'   # EZConnect
#        export ORA_CONN='/@MYDB_WALLET_ALIAS'                     # Oracle Wallet (no password on disk)
#
#   2. Run it. An example query is already built in as the default:
#
#        ./oracle_latency_probe.sh
#
#      To profile a different query, pass --sql-inline "..." or --sql file.sql.
#      To compare the engines fairly, add --no-result-cache (see note below).
#
#   Requirements: `sqlplus` on PATH (set SQLPLUS=/path/to/sqlplus otherwise).
#   Works on macOS (bash 3.2, BSD tools) and Linux with no extra installs.
#
# ----------------------------------------------------------------------------
# OPTIONS
#   --sql FILE | --sql-inline "SELECT ..."   query to profile (default = built-in example)
#   --iters N        measured runs (default 30)      --warmup N    discarded warmups (default 5)
#   --rtt N          empty round trips (default 1000) --connect N   fresh-login samples (default 10)
#   --arraysize N    rows per fetch (default 15; MATCH your real client — drives round trips)
#   --no-result-cache  inject NO_RESULT_CACHE hint (measure the engine, not Oracle's cache)
#   --trace          also capture a 10046 level-8 trace + tkprof (needs trace-file access)
#   --outdir DIR     where to write artifacts (default ./oracle_probe_out)
#
# PRODUCES a latency ledger (min/median over N runs):
#   Connect (TCP+TLS+auth) ....... bare sqlplus login wall-clock
#   Network RTT (empty) .......... bulk SELECT 1 FROM DUAL ÷ N  (resolution-independent)
#   Server planning + execution .. V$SQL ELAPSED_TIME / EXECUTIONS   (clean, no instrumentation)
#   Round trips / bytes on wire .. AUTOTRACE SQL*Net statistics
#   Logical vs physical reads .... AUTOTRACE consistent gets vs physical reads
#   CLIENT TOTAL (wall) .......... SET TIMING ON
#   Transfer + client (residual) . total − server − (roundtrips × RTT)
#
# TIERS (auto-detected by what your login can see; degrade gracefully):
#   A  SET TIMING + bulk RTT + connect        — needs only CONNECT privilege
#   B  AUTOTRACE statistics                   — needs PLUSTRACE role (or SELECT on V$ fixed views)
#   C  V$SQL server-side elapsed              — needs SELECT on V$SQL / V$SQLAREA
#   D  10046 trace + tkprof (optional, --trace) — needs ALTER SESSION + trace-file access
#
# SECURITY: never hardcode a password in this file. `user/pass@tns` is visible in the process
# list (ps); prefer an Oracle Wallet (/@alias), or keep ORA_CONN in your shell env only.
# ============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------- defaults
SQLPLUS="${SQLPLUS:-sqlplus}"
ITERS=30
WARMUP=5
RTT_ITERS=1000
CONNECT_ITERS=10
ARRAYSIZE=15                 # sqlplus default; MATCH this to the real client. Drives fetch round trips.
NO_RESULT_CACHE=0
DO_TRACE=0
OUTDIR="./oracle_probe_out"
SQL_FILE=""
SQL_INLINE=""

DEFAULT_SQL="select count(1)
from orders o,
     order_lines l
where o.order_id = l.order_id
  and o.region_id = l.region_id
  and o.customer_id = '12345'"

# ---------------------------------------------------------------------------- args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql)            SQL_FILE="$2"; shift 2 ;;
    --sql-inline)     SQL_INLINE="$2"; shift 2 ;;
    --iters)          ITERS="$2"; shift 2 ;;
    --warmup)         WARMUP="$2"; shift 2 ;;
    --rtt)            RTT_ITERS="$2"; shift 2 ;;
    --connect)        CONNECT_ITERS="$2"; shift 2 ;;
    --arraysize)      ARRAYSIZE="$2"; shift 2 ;;
    --no-result-cache) NO_RESULT_CACHE=1; shift ;;
    --trace)          DO_TRACE=1; shift ;;
    --outdir)         OUTDIR="$2"; shift 2 ;;
    -h|--help)        grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

: "${ORA_CONN:?Set ORA_CONN, e.g. export ORA_CONN='user/password@//host:1521/service' (or /@wallet_alias)}"
command -v "$SQLPLUS" >/dev/null 2>&1 || { echo "sqlplus not found on PATH (set SQLPLUS=...)." >&2; exit 3; }

# High-resolution wall clock, portable across macOS (bash 3.2, BSD date) and Linux.
# Priority: bash5 EPOCHREALTIME (zero overhead) -> GNU date %N / gdate -> perl Time::HiRes
# (always present on macOS) -> coarse integer seconds (last resort).
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

# Optionally inject a NO_RESULT_CACHE hint so we measure the engine, not Oracle's result cache.
TARGET_RUN_SQL="$TARGET_SQL"
if [[ "$NO_RESULT_CACHE" -eq 1 ]]; then
  TARGET_RUN_SQL="$(printf '%s' "$TARGET_SQL" | sed -E '0,/[Ss][Ee][Ll][Ee][Cc][Tt]/s//SELECT \/*+ NO_RESULT_CACHE *\//')"
fi

echo "=============================================================="
echo " Oracle latency probe"
echo "  iters=$ITERS warmup=$WARMUP rtt=$RTT_ITERS connect=$CONNECT_ITERS arraysize=$ARRAYSIZE"
echo "  no_result_cache=$NO_RESULT_CACHE trace=$DO_TRACE outdir=$OUTDIR"
echo "--------------------------------------------------------------"
echo "$TARGET_RUN_SQL"
echo "=============================================================="

# ---------------------------------------------------------------------------- helpers
# Run a sqlplus session. $1 = SQL body (statements). Login banner suppressed with -S.
runsql() { printf '%s\n' "$1" | "$SQLPLUS" -S -L "$ORA_CONN"; }

# Wall-clock a shell command group, echo elapsed seconds. Uses the best clock from now().
timeit() {
  local start end
  start=$(now)
  "$@" >/dev/null 2>&1 || true
  end=$(now)
  awk -v s="$start" -v e="$end" 'BEGIN{printf "%.6f", e - s}'
}

# median + min of newline-separated numbers on stdin -> "min median".
# Uses `sort -n` (not awk asort, which is gawk-only) for portability across BSD/mac/mawk.
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
  printf "%s %s" "$min" "$med"
}

SETTINGS="WHENEVER SQLERROR CONTINUE
WHENEVER OSERROR CONTINUE
SET HEADING OFF
SET FEEDBACK OFF
SET VERIFY OFF
SET ECHO OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET ARRAYSIZE $ARRAYSIZE"

# ---------------------------------------------------------------------------- preflight: version + privileges
echo
echo "[preflight] version + privilege detection..."
PRE="$SETTINGS
SET SERVEROUTPUT ON
SELECT 'VER|'||banner FROM v\$version WHERE ROWNUM=1;
DECLARE v NUMBER; s VARCHAR2(512);
BEGIN
  BEGIN EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM v\$sql WHERE ROWNUM=1' INTO v; DBMS_OUTPUT.PUT_LINE('PRIV|VSQL|YES'); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('PRIV|VSQL|NO'); END;
  BEGIN EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM v\$mystat WHERE ROWNUM=1' INTO v; DBMS_OUTPUT.PUT_LINE('PRIV|VMYSTAT|YES'); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('PRIV|VMYSTAT|NO'); END;
  BEGIN EXECUTE IMMEDIATE 'SELECT value FROM v\$diag_info WHERE name=''Default Trace File''' INTO s; DBMS_OUTPUT.PUT_LINE('PRIV|DIAG|YES'); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('PRIV|DIAG|NO'); END;
END;
/
EXIT"
PRE_OUT="$(runsql "$PRE" 2>/dev/null || true)"
echo "$PRE_OUT" | grep -E '^(VER|PRIV)\|' || true
HAVE_VSQL=$(echo "$PRE_OUT" | grep -q 'PRIV|VSQL|YES' && echo 1 || echo 0)

# ---------------------------------------------------------------------------- 1) connect time + RTT baseline
# baseline = login + immediate exit (this IS the connect cost). rtt session = login + N× SELECT 1 FROM DUAL.
# per-RTT = (rtt_wall - baseline_wall) / N  -> immune to sqlplus's coarse SET TIMING resolution.
echo
echo "[1] connect time + network RTT..."

connect_samples=""
for _ in $(seq 1 "$CONNECT_ITERS"); do
  t=$(timeit runsql "$SETTINGS
SELECT 1 FROM DUAL;
EXIT")
  connect_samples+="$t"$'\n'
done
# connect in ms
connect_ms_list=$(printf '%s' "$connect_samples" | awk 'NF{printf "%.3f\n", $1*1000}')
read -r CONNECT_MIN CONNECT_MED < <(printf '%s' "$connect_ms_list" | minmed)

# RTT: build N statements, run once, subtract one baseline login.
RTT_BODY="$SETTINGS"$'\n'
for _ in $(seq 1 "$RTT_ITERS"); do RTT_BODY+="SELECT 1 FROM DUAL;"$'\n'; done
RTT_BODY+="EXIT"
BASE_WALL=$(timeit runsql "$SETTINGS
EXIT")
RTT_WALL=$(timeit runsql "$RTT_BODY")
RTT_MS=$(awk -v r="$RTT_WALL" -v b="$BASE_WALL" -v n="$RTT_ITERS" 'BEGIN{d=(r-b)/n*1000; if(d<0)d=0; printf "%.3f", d}')
echo "    connect  min=${CONNECT_MIN}ms  median=${CONNECT_MED}ms"
echo "    rtt/stmt ~${RTT_MS}ms  (bulk ${RTT_ITERS}x: ${RTT_WALL}s minus baseline ${BASE_WALL}s)"

# ---------------------------------------------------------------------------- 2) warmup
echo
echo "[2] warmup (${WARMUP}x)..."
WARM_BODY="$SETTINGS"$'\n'
for _ in $(seq 1 "$WARMUP"); do WARM_BODY+="$TARGET_RUN_SQL;"$'\n'; done
WARM_BODY+="EXIT"
runsql "$WARM_BODY" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- 3) client total (SET TIMING, autotrace OFF = clean)
echo
echo "[3] client total wall-clock (${ITERS}x, SET TIMING)..."
TIMING_BODY="$SETTINGS
SET TIMING ON"$'\n'
for _ in $(seq 1 "$ITERS"); do TIMING_BODY+="$TARGET_RUN_SQL;"$'\n'; done
TIMING_BODY+="EXIT"
runsql "$TIMING_BODY" > "$OUTDIR/timing.txt" 2>&1 || true
# Elapsed lines look like: "Elapsed: 00:00:00.065" (SQL*Plus 12c+ prints milliseconds).
total_ms_list=$(grep -iE 'Elapsed:[[:space:]]*[0-9]' "$OUTDIR/timing.txt" | \
  sed -E 's/.*Elapsed:[[:space:]]*//' | \
  awk -F'[:.]' '{printf "%.3f\n", (($1*3600)+($2*60)+$3)*1000 + $4}')
if [[ -n "$total_ms_list" ]]; then
  read -r TOTAL_MIN TOTAL_MED < <(printf '%s' "$total_ms_list" | minmed)
else
  TOTAL_MIN=0; TOTAL_MED=0
  echo "    WARN: no Elapsed lines parsed — check $OUTDIR/timing.txt"
fi
echo "    total   min=${TOTAL_MIN}ms  median=${TOTAL_MED}ms"

# ---------------------------------------------------------------------------- 4) AUTOTRACE statistics (Tier B): roundtrips, bytes, gets, physical reads
echo
echo "[4] AUTOTRACE statistics (SQL*Net roundtrips, bytes, logical/physical reads)..."
AT_BODY="$SETTINGS
SET AUTOTRACE TRACEONLY STATISTICS
$TARGET_RUN_SQL;
SET AUTOTRACE OFF
EXIT"
runsql "$AT_BODY" > "$OUTDIR/autotrace.txt" 2>&1 || true
ROUNDTRIPS=$(grep -i 'SQL\*Net roundtrips to/from client' "$OUTDIR/autotrace.txt" | head -1 | awk '{print $1}' | tr -d ',')
BYTES_SENT=$(grep -i 'bytes sent via SQL\*Net to client' "$OUTDIR/autotrace.txt" | head -1 | awk '{print $1}' | tr -d ',')
CONSISTENT_GETS=$(grep -i 'consistent gets' "$OUTDIR/autotrace.txt" | head -1 | awk '{print $1}' | tr -d ',')
PHYS_READS=$(grep -i 'physical reads' "$OUTDIR/autotrace.txt" | head -1 | awk '{print $1}' | tr -d ',')
ROUNDTRIPS=${ROUNDTRIPS:-}; BYTES_SENT=${BYTES_SENT:-}; CONSISTENT_GETS=${CONSISTENT_GETS:-}; PHYS_READS=${PHYS_READS:-}
if [[ -z "$ROUNDTRIPS" ]]; then
  echo "    (AUTOTRACE unavailable — need PLUSTRACE role / V\$ access. See $OUTDIR/autotrace.txt)"
else
  echo "    roundtrips=$ROUNDTRIPS  bytes_sent=$BYTES_SENT  consistent_gets=$CONSISTENT_GETS  physical_reads=$PHYS_READS"
fi

# ---------------------------------------------------------------------------- 5) V$SQL clean server elapsed (Tier C)
SERVER_MS=""
if [[ "$HAVE_VSQL" -eq 1 ]]; then
  echo
  echo "[5] V\$SQL server-side elapsed per execution..."
  # Match our query by a distinctive substring; take the most-recently-active matching cursor.
  SIG=$(printf '%s' "$TARGET_SQL" | tr '[:lower:]' '[:upper:]' | grep -oE '[A-Z_][A-Z0-9_]{4,}' | head -1)
  VSQL_BODY="$SETTINGS
SET SERVEROUTPUT ON
DECLARE
  v_elapsed NUMBER; v_exec NUMBER; v_cpu NUMBER; v_bg NUMBER; v_dr NUMBER; v_rows NUMBER; v_parse NUMBER;
BEGIN
  SELECT elapsed_time, executions, cpu_time, buffer_gets, disk_reads, rows_processed, parse_calls
    INTO v_elapsed, v_exec, v_cpu, v_bg, v_dr, v_rows, v_parse
  FROM (SELECT * FROM v\$sql
        WHERE UPPER(sql_text) LIKE '%${SIG}%' AND executions > 0
        ORDER BY last_active_time DESC)
  WHERE ROWNUM = 1;
  DBMS_OUTPUT.PUT_LINE('VSQL|'||(v_elapsed/GREATEST(v_exec,1)/1000)||'|'||(v_cpu/GREATEST(v_exec,1)/1000)
    ||'|'||v_exec||'|'||ROUND(v_bg/GREATEST(v_exec,1))||'|'||ROUND(v_dr/GREATEST(v_exec,1))||'|'||ROUND(v_rows/GREATEST(v_exec,1)));
EXCEPTION WHEN NO_DATA_FOUND THEN DBMS_OUTPUT.PUT_LINE('VSQL|NODATA for %${SIG}%');
END;
/
EXIT"
  VSQL_OUT="$(runsql "$VSQL_BODY" 2>/dev/null || true)"
  VROW=$(echo "$VSQL_OUT" | grep '^VSQL|' | head -1)
  if [[ "$VROW" == VSQL\|NODATA* || -z "$VROW" ]]; then
    echo "    (no matching cursor for signature '%${SIG}%' — adjust the query signature)"
  else
    SERVER_MS=$(echo "$VROW" | awk -F'|' '{printf "%.3f", $2}')
    SERVER_CPU=$(echo "$VROW" | awk -F'|' '{printf "%.3f", $3}')
    echo "    server elapsed/exec=${SERVER_MS}ms  cpu/exec=${SERVER_CPU}ms  (sig=%${SIG}%)"
  fi
fi

# ---------------------------------------------------------------------------- 6) optional 10046 trace + tkprof (Tier D)
if [[ "$DO_TRACE" -eq 1 ]]; then
  echo
  echo "[6] 10046 level 8 trace + tkprof..."
  TR_BODY="$SETTINGS
SET SERVEROUTPUT ON
ALTER SESSION SET tracefile_identifier = 'latprobe';
ALTER SESSION SET events '10046 trace name context forever, level 8';
$TARGET_RUN_SQL;
SELECT value FROM v\$diag_info WHERE name='Default Trace File';
ALTER SESSION SET events '10046 trace name context off';
EXIT"
  runsql "$TR_BODY" > "$OUTDIR/trace_session.txt" 2>&1 || true
  TRACEFILE=$(grep -iE '\.trc$' "$OUTDIR/trace_session.txt" | tail -1 | tr -d '[:space:]')
  if [[ -n "$TRACEFILE" && -r "$TRACEFILE" ]]; then
    echo "    trace: $TRACEFILE"
    if command -v tkprof >/dev/null 2>&1; then
      tkprof "$TRACEFILE" "$OUTDIR/tkprof.txt" sys=no >/dev/null 2>&1 || true
      echo "    tkprof -> $OUTDIR/tkprof.txt  (parse/exec/fetch elapsed + CPU + SQL*Net waits)"
    else
      echo "    tkprof not on PATH; raw trace saved. Format later: tkprof $TRACEFILE out.txt"
    fi
  else
    echo "    trace file not reachable from this host (need read access to the diag trace dir)."
  fi
fi

# ---------------------------------------------------------------------------- ledger
echo
echo "=============================================================="
echo " LATENCY LEDGER (Oracle)   [all values ms]"
echo "--------------------------------------------------------------"
printf "  %-32s %10s %10s\n" "line_item" "min" "median"
printf "  %-32s %10s %10s\n" "Connect (TCP+TLS+auth)" "$CONNECT_MIN" "$CONNECT_MED"
printf "  %-32s %10s %10s\n" "Network RTT (empty)" "$RTT_MS" "$RTT_MS"
if [[ -n "$SERVER_MS" ]]; then
  printf "  %-32s %10s %10s\n" "Server elapsed/exec (V\$SQL)" "$SERVER_MS" "$SERVER_MS"
  if [[ -n "${ROUNDTRIPS:-}" ]]; then
    NET_MS=$(awk -v r="$ROUNDTRIPS" -v rtt="$RTT_MS" 'BEGIN{printf "%.3f", r*rtt}')
    RESID=$(awk -v t="$TOTAL_MED" -v s="$SERVER_MS" -v n="$NET_MS" 'BEGIN{v=t-s-n; if(v<0)v=0; printf "%.3f", v}')
    printf "  %-32s %10s %10s   (%s roundtrips x %sms)\n" "Network (roundtrips x RTT)" "$NET_MS" "$NET_MS" "$ROUNDTRIPS" "$RTT_MS"
    printf "  %-32s %10s %10s\n" "Transfer + client (residual)" "$RESID" "$RESID"
  fi
fi
printf "  %-32s %10s %10s\n" "CLIENT TOTAL (wall)" "$TOTAL_MIN" "$TOTAL_MED"
echo "--------------------------------------------------------------"
[[ -n "${BYTES_SENT:-}" ]]     && echo "  bytes sent via SQL*Net to client : $BYTES_SENT"
[[ -n "${CONSISTENT_GETS:-}" ]] && echo "  consistent gets (logical reads)  : $CONSISTENT_GETS"
[[ -n "${PHYS_READS:-}" ]]     && echo "  physical reads (disk)            : $PHYS_READS   (0 = fully cached / warm)"
echo "  arraysize (fetch batch)          : $ARRAYSIZE   <-- match the real client; drives roundtrips"
echo "  result cache                     : $([[ $NO_RESULT_CACHE -eq 1 ]] && echo 'bypassed (NO_RESULT_CACHE)' || echo 'DEFAULT (may serve cached count!)')"
echo "=============================================================="
echo "Artifacts in: $OUTDIR/  (timing.txt, autotrace.txt$([[ $DO_TRACE -eq 1 ]] && echo ', tkprof.txt'))"
