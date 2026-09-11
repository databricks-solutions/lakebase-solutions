-- =============================================================================
-- Lakebase Monitoring Schema
-- =============================================================================
-- Creates a 'monitoring' schema with 14 views that wrap PostgreSQL system
-- catalogs for database health monitoring. Designed to be queried through
-- Unity Catalog foreign tables via a Databricks SQL warehouse and Genie AI.
--
-- Target: PostgreSQL 16 (Databricks Lakebase)
-- Idempotent: Safe to run multiple times (CREATE OR REPLACE VIEW)
--
-- Notes:
--   - Column names are descriptive for natural language querying.
--   - Sizes are provided in both raw bytes and human-readable text.
--   - pg_stat_statements views gracefully handle the extension being absent.
--   - Views that may be restricted on Lakebase are wrapped in exception blocks.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema creation
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ===========================================================================
-- 1. monitoring.active_connections
--    Shows all currently active (non-idle) connections to the database.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.active_connections AS
  SELECT
    pid                                        AS process_id,
    usename                                    AS username,
    datname                                    AS database_name,
    client_addr                                AS client_address,
    client_port                                AS client_port,
    application_name                           AS application_name,
    backend_start                              AS connection_started_at,
    xact_start                                 AS transaction_started_at,
    query_start                                AS query_started_at,
    state_change                               AS state_changed_at,
    state                                      AS connection_state,
    wait_event_type                            AS wait_event_type,
    wait_event                                 AS wait_event,
    EXTRACT(EPOCH FROM (clock_timestamp() - backend_start))::numeric(12,1)
                                               AS connection_duration_seconds,
    EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::numeric(12,1)
                                               AS current_query_duration_seconds,
    query                                      AS current_query
  FROM pg_stat_activity
  WHERE state IS NOT NULL
    AND state <> 'idle'
    AND pid <> pg_backend_pid();
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.active_connections: %', SQLERRM;
END $$;

-- ===========================================================================
-- 2. monitoring.blocking_queries
--    Shows queries that are blocked and the queries that are blocking them.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.blocking_queries AS
  SELECT
    blocked.pid                                AS blocked_process_id,
    blocked.usename                            AS blocked_username,
    blocked.datname                            AS blocked_database_name,
    blocked.application_name                   AS blocked_application_name,
    blocked.query                              AS blocked_query,
    blocked.query_start                        AS blocked_query_started_at,
    EXTRACT(EPOCH FROM (clock_timestamp() - blocked.query_start))::numeric(12,1)
                                               AS blocked_duration_seconds,
    blocked.wait_event_type                    AS blocked_wait_event_type,
    blocked.wait_event                         AS blocked_wait_event,
    blocker.pid                                AS blocking_process_id,
    blocker.usename                            AS blocking_username,
    blocker.application_name                   AS blocking_application_name,
    blocker.state                              AS blocking_connection_state,
    blocker.query                              AS blocking_query,
    blocker.query_start                        AS blocking_query_started_at
  FROM pg_stat_activity AS blocked
  JOIN pg_locks AS blocked_locks
    ON blocked.pid = blocked_locks.pid AND NOT blocked_locks.granted
  JOIN pg_locks AS blocking_locks
    ON  blocked_locks.locktype    = blocking_locks.locktype
    AND blocked_locks.database   IS NOT DISTINCT FROM blocking_locks.database
    AND blocked_locks.relation   IS NOT DISTINCT FROM blocking_locks.relation
    AND blocked_locks.page       IS NOT DISTINCT FROM blocking_locks.page
    AND blocked_locks.tuple      IS NOT DISTINCT FROM blocking_locks.tuple
    AND blocked_locks.virtualxid IS NOT DISTINCT FROM blocking_locks.virtualxid
    AND blocked_locks.transactionid IS NOT DISTINCT FROM blocking_locks.transactionid
    AND blocked_locks.classid    IS NOT DISTINCT FROM blocking_locks.classid
    AND blocked_locks.objid      IS NOT DISTINCT FROM blocking_locks.objid
    AND blocked_locks.objsubid   IS NOT DISTINCT FROM blocking_locks.objsubid
    AND blocking_locks.granted
    AND blocked_locks.pid <> blocking_locks.pid
  JOIN pg_stat_activity AS blocker
    ON blocking_locks.pid = blocker.pid
  WHERE blocked.state = 'active';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.blocking_queries: %', SQLERRM;
END $$;

-- ===========================================================================
-- 3. monitoring.cache_hit_ratio
--    Shows buffer cache hit ratio per database. Higher is better (target >99%).
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.cache_hit_ratio AS
  SELECT
    datname                                    AS database_name,
    blks_read                                  AS disk_blocks_read,
    blks_hit                                   AS cache_blocks_hit,
    (blks_read + blks_hit)                     AS total_blocks_accessed,
    CASE
      WHEN (blks_read + blks_hit) = 0 THEN 0
      ELSE ROUND(blks_hit::numeric / (blks_read + blks_hit) * 100, 2)
    END                                        AS cache_hit_ratio_percent,
    CASE
      WHEN (blks_read + blks_hit) = 0 THEN 'No activity'
      WHEN ROUND(blks_hit::numeric / (blks_read + blks_hit) * 100, 2) >= 99 THEN 'Excellent'
      WHEN ROUND(blks_hit::numeric / (blks_read + blks_hit) * 100, 2) >= 95 THEN 'Good'
      WHEN ROUND(blks_hit::numeric / (blks_read + blks_hit) * 100, 2) >= 90 THEN 'Fair'
      ELSE 'Poor - consider increasing shared_buffers'
    END                                        AS cache_health,
    stats_reset                                AS stats_last_reset_at
  FROM pg_stat_database
  WHERE datname IS NOT NULL
  ORDER BY database_name;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.cache_hit_ratio: %', SQLERRM;
END $$;

-- ===========================================================================
-- 4. monitoring.connection_stats
--    Aggregated connection statistics grouped by database, user, and state.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.connection_stats AS
  SELECT
    datname                                    AS database_name,
    usename                                    AS username,
    state                                      AS connection_state,
    COUNT(*)                                   AS connection_count,
    COUNT(*) FILTER (WHERE wait_event IS NOT NULL)
                                               AS waiting_connection_count,
    MIN(backend_start)                         AS oldest_connection_started_at,
    MAX(backend_start)                         AS newest_connection_started_at,
    MAX(EXTRACT(EPOCH FROM (clock_timestamp() - backend_start)))::numeric(12,1)
                                               AS longest_connection_duration_seconds,
    COUNT(*) FILTER (WHERE state = 'active')   AS active_count,
    COUNT(*) FILTER (WHERE state = 'idle')     AS idle_count,
    COUNT(*) FILTER (WHERE state = 'idle in transaction')
                                               AS idle_in_transaction_count
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
    AND datname IS NOT NULL
  GROUP BY datname, usename, state
  ORDER BY connection_count DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.connection_stats: %', SQLERRM;
END $$;

-- ===========================================================================
-- 5. monitoring.database_size_stats
--    Shows size of each database with human-readable formatting.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.database_size_stats AS
  SELECT
    d.datname                                  AS database_name,
    pg_database_size(d.datname)                AS database_size_bytes,
    pg_size_pretty(pg_database_size(d.datname))
                                               AS database_size_pretty,
    ROUND(pg_database_size(d.datname)::numeric / (1024 * 1024), 2)
                                               AS database_size_mb,
    ROUND(pg_database_size(d.datname)::numeric / (1024 * 1024 * 1024), 4)
                                               AS database_size_gb,
    s.numbackends                              AS active_backend_count,
    s.xact_commit                              AS total_transactions_committed,
    s.xact_rollback                            AS total_transactions_rolled_back,
    CASE
      WHEN (s.xact_commit + s.xact_rollback) = 0 THEN 0
      ELSE ROUND(s.xact_rollback::numeric / (s.xact_commit + s.xact_rollback) * 100, 2)
    END                                        AS rollback_ratio_percent,
    s.tup_returned                             AS total_rows_returned,
    s.tup_fetched                              AS total_rows_fetched,
    s.tup_inserted                             AS total_rows_inserted,
    s.tup_updated                              AS total_rows_updated,
    s.tup_deleted                              AS total_rows_deleted,
    s.temp_files                               AS temp_files_created,
    s.temp_bytes                               AS temp_bytes_written,
    pg_size_pretty(s.temp_bytes)               AS temp_bytes_written_pretty,
    s.stats_reset                              AS stats_last_reset_at
  FROM pg_database d
  LEFT JOIN pg_stat_database s ON d.datname = s.datname
  WHERE d.datistemplate = false
  ORDER BY pg_database_size(d.datname) DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.database_size_stats: %', SQLERRM;
END $$;

-- ===========================================================================
-- 6. monitoring.long_running_queries
--    Shows queries running longer than 30 seconds.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.long_running_queries AS
  SELECT
    pid                                        AS process_id,
    usename                                    AS username,
    datname                                    AS database_name,
    client_addr                                AS client_address,
    application_name                           AS application_name,
    backend_start                              AS connection_started_at,
    query_start                                AS query_started_at,
    state                                      AS connection_state,
    wait_event_type                            AS wait_event_type,
    wait_event                                 AS wait_event,
    EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::numeric(12,1)
                                               AS query_duration_seconds,
    ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::numeric / 60, 1)
                                               AS query_duration_minutes,
    CASE
      WHEN EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) < 60
        THEN 'Warning (30s-1m)'
      WHEN EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) < 300
        THEN 'Elevated (1-5m)'
      WHEN EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) < 3600
        THEN 'High (5-60m)'
      ELSE 'Critical (>1h)'
    END                                        AS severity,
    query                                      AS current_query
  FROM pg_stat_activity
  WHERE state = 'active'
    AND pid <> pg_backend_pid()
    AND query_start IS NOT NULL
    AND EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) > 30
  ORDER BY query_start ASC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.long_running_queries: %', SQLERRM;
END $$;

-- ===========================================================================
-- 7. monitoring.maintenance_recommendations
--    Identifies tables that may need VACUUM, ANALYZE, or have bloat issues.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.maintenance_recommendations AS
  SELECT
    schemaname                                 AS table_schema,
    relname                                    AS table_name,
    schemaname || '.' || relname               AS fully_qualified_table_name,
    n_live_tup                                 AS live_row_count,
    n_dead_tup                                 AS dead_row_count,
    CASE
      WHEN n_live_tup = 0 THEN 0
      ELSE ROUND(n_dead_tup::numeric / GREATEST(n_live_tup, 1) * 100, 2)
    END                                        AS dead_row_ratio_percent,
    last_vacuum                                AS last_manual_vacuum_at,
    last_autovacuum                            AS last_autovacuum_at,
    last_analyze                               AS last_manual_analyze_at,
    last_autoanalyze                           AS last_autoanalyze_at,
    vacuum_count                               AS manual_vacuum_count,
    autovacuum_count                           AS autovacuum_count,
    analyze_count                              AS manual_analyze_count,
    autoanalyze_count                          AS autoanalyze_count,
    n_mod_since_analyze                        AS rows_modified_since_last_analyze,
    n_ins_since_vacuum                         AS rows_inserted_since_last_vacuum,
    CASE
      WHEN n_dead_tup > 10000
        AND (n_live_tup = 0 OR n_dead_tup::numeric / GREATEST(n_live_tup, 1) > 0.1)
        THEN 'VACUUM recommended - high dead row ratio'
      WHEN last_autovacuum IS NULL AND last_vacuum IS NULL AND n_dead_tup > 1000
        THEN 'VACUUM recommended - never vacuumed with dead rows'
      WHEN last_autovacuum < (now() - interval '7 days')
        AND n_dead_tup > 5000
        THEN 'VACUUM recommended - not vacuumed in over 7 days'
      ELSE NULL
    END                                        AS vacuum_recommendation,
    CASE
      WHEN last_analyze IS NULL AND last_autoanalyze IS NULL AND n_live_tup > 1000
        THEN 'ANALYZE recommended - never analyzed'
      WHEN n_mod_since_analyze > GREATEST(n_live_tup * 0.1, 10000)
        THEN 'ANALYZE recommended - significant modifications since last analyze'
      ELSE NULL
    END                                        AS analyze_recommendation,
    CASE
      WHEN n_dead_tup > 10000
        AND (n_live_tup = 0 OR n_dead_tup::numeric / GREATEST(n_live_tup, 1) > 0.2)
        THEN 'High'
      WHEN n_dead_tup > 5000
        OR (last_autovacuum IS NULL AND last_vacuum IS NULL AND n_live_tup > 10000)
        THEN 'Medium'
      WHEN n_mod_since_analyze > GREATEST(n_live_tup * 0.1, 10000)
        THEN 'Low'
      ELSE 'None'
    END                                        AS maintenance_urgency
  FROM pg_stat_user_tables
  ORDER BY n_dead_tup DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.maintenance_recommendations: %', SQLERRM;
END $$;

-- ===========================================================================
-- 8. monitoring.pg_stat_statements
--    Wraps the pg_stat_statements extension. Falls back to an empty view
--    if the extension is not installed.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.pg_stat_statements AS
  SELECT
    userid                                     AS user_id,
    dbid                                       AS database_id,
    toplevel                                   AS is_top_level,
    queryid                                    AS query_id,
    query                                      AS query_text,
    plans                                      AS plan_count,
    total_plan_time                            AS total_plan_time_ms,
    min_plan_time                              AS min_plan_time_ms,
    max_plan_time                              AS max_plan_time_ms,
    mean_plan_time                             AS mean_plan_time_ms,
    calls                                      AS execution_count,
    total_exec_time                            AS total_exec_time_ms,
    min_exec_time                              AS min_exec_time_ms,
    max_exec_time                              AS max_exec_time_ms,
    mean_exec_time                             AS mean_exec_time_ms,
    rows                                       AS total_rows_returned,
    shared_blks_hit                            AS shared_cache_hits,
    shared_blks_read                           AS shared_disk_reads,
    shared_blks_dirtied                        AS shared_blocks_dirtied,
    shared_blks_written                        AS shared_blocks_written,
    local_blks_hit                             AS local_cache_hits,
    local_blks_read                            AS local_disk_reads,
    temp_blks_read                             AS temp_blocks_read,
    temp_blks_written                          AS temp_blocks_written
  FROM pg_stat_statements;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_stat_statements not available, creating empty fallback: %', SQLERRM;
  CREATE OR REPLACE VIEW monitoring.pg_stat_statements AS
  SELECT
    NULL::oid       AS user_id,
    NULL::oid       AS database_id,
    NULL::boolean   AS is_top_level,
    NULL::bigint    AS query_id,
    'pg_stat_statements extension is not enabled'::text AS query_text,
    NULL::bigint    AS plan_count,
    NULL::double precision AS total_plan_time_ms,
    NULL::double precision AS min_plan_time_ms,
    NULL::double precision AS max_plan_time_ms,
    NULL::double precision AS mean_plan_time_ms,
    NULL::bigint    AS execution_count,
    NULL::double precision AS total_exec_time_ms,
    NULL::double precision AS min_exec_time_ms,
    NULL::double precision AS max_exec_time_ms,
    NULL::double precision AS mean_exec_time_ms,
    NULL::bigint    AS total_rows_returned,
    NULL::bigint    AS shared_cache_hits,
    NULL::bigint    AS shared_disk_reads,
    NULL::bigint    AS shared_blocks_dirtied,
    NULL::bigint    AS shared_blocks_written,
    NULL::bigint    AS local_cache_hits,
    NULL::bigint    AS local_disk_reads,
    NULL::bigint    AS temp_blocks_read,
    NULL::bigint    AS temp_blocks_written
  WHERE false;
END $$;

-- ===========================================================================
-- 9. monitoring.pg_stat_statements_info
--    Metadata about the pg_stat_statements collection.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.pg_stat_statements_info AS
  SELECT
    dealloc                                    AS times_entries_deallocated,
    stats_reset                                AS stats_last_reset_at
  FROM pg_stat_statements_info;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_stat_statements_info not available, creating empty fallback: %', SQLERRM;
  CREATE OR REPLACE VIEW monitoring.pg_stat_statements_info AS
  SELECT
    NULL::bigint       AS times_entries_deallocated,
    NULL::timestamptz  AS stats_last_reset_at
  WHERE false;
END $$;

-- ===========================================================================
-- 10. monitoring.query_performance_log
--     Top queries by total execution time from pg_stat_statements.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.query_performance_log AS
  SELECT
    pss.queryid                                AS query_id,
    d.datname                                  AS database_name,
    u.usename                                  AS username,
    pss.query                                  AS query_text,
    pss.calls                                  AS execution_count,
    ROUND(pss.total_exec_time::numeric, 2)     AS total_exec_time_ms,
    ROUND(pss.mean_exec_time::numeric, 2)      AS avg_exec_time_ms,
    ROUND(pss.min_exec_time::numeric, 2)       AS min_exec_time_ms,
    ROUND(pss.max_exec_time::numeric, 2)       AS max_exec_time_ms,
    ROUND(pss.total_plan_time::numeric, 2)     AS total_plan_time_ms,
    ROUND(pss.mean_plan_time::numeric, 2)      AS avg_plan_time_ms,
    pss.rows                                   AS total_rows_returned,
    CASE
      WHEN pss.calls = 0 THEN 0
      ELSE ROUND((pss.rows / pss.calls)::numeric, 1)
    END                                        AS avg_rows_per_execution,
    pss.shared_blks_hit                        AS shared_cache_hits,
    pss.shared_blks_read                       AS shared_disk_reads,
    CASE
      WHEN (pss.shared_blks_hit + pss.shared_blks_read) = 0 THEN 0
      ELSE ROUND(pss.shared_blks_hit::numeric
                 / (pss.shared_blks_hit + pss.shared_blks_read) * 100, 2)
    END                                        AS query_cache_hit_ratio_percent,
    pss.temp_blks_read + pss.temp_blks_written AS temp_blocks_total,
    CASE
      WHEN pss.mean_exec_time > 10000 THEN 'Very Slow (>10s avg)'
      WHEN pss.mean_exec_time > 1000  THEN 'Slow (1-10s avg)'
      WHEN pss.mean_exec_time > 100   THEN 'Moderate (100ms-1s avg)'
      ELSE 'Fast (<100ms avg)'
    END                                        AS performance_category
  FROM pg_stat_statements pss
  LEFT JOIN pg_database d ON pss.dbid = d.oid
  LEFT JOIN pg_roles u ON pss.userid = u.oid
  ORDER BY pss.total_exec_time DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'query_performance_log: pg_stat_statements may not be available: %', SQLERRM;
  CREATE OR REPLACE VIEW monitoring.query_performance_log AS
  SELECT
    NULL::bigint    AS query_id,
    NULL::text      AS database_name,
    NULL::text      AS username,
    'pg_stat_statements extension is not enabled'::text AS query_text,
    NULL::bigint    AS execution_count,
    NULL::numeric   AS total_exec_time_ms,
    NULL::numeric   AS avg_exec_time_ms,
    NULL::numeric   AS min_exec_time_ms,
    NULL::numeric   AS max_exec_time_ms,
    NULL::numeric   AS total_plan_time_ms,
    NULL::numeric   AS avg_plan_time_ms,
    NULL::bigint    AS total_rows_returned,
    NULL::numeric   AS avg_rows_per_execution,
    NULL::bigint    AS shared_cache_hits,
    NULL::bigint    AS shared_disk_reads,
    NULL::numeric   AS query_cache_hit_ratio_percent,
    NULL::bigint    AS temp_blocks_total,
    NULL::text      AS performance_category
  WHERE false;
END $$;

-- ===========================================================================
-- 11. monitoring.replication_lag_history
--     Shows current replication lag details for each standby.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.replication_lag_history AS
  SELECT
    pid                                        AS replication_process_id,
    usename                                    AS replication_username,
    application_name                           AS standby_application_name,
    client_addr                                AS standby_address,
    state                                      AS replication_state,
    sent_lsn                                   AS sent_log_position,
    write_lsn                                  AS written_log_position,
    flush_lsn                                  AS flushed_log_position,
    replay_lsn                                 AS replayed_log_position,
    CASE
      WHEN sent_lsn IS NOT NULL AND replay_lsn IS NOT NULL
        THEN (sent_lsn - replay_lsn)
      ELSE NULL
    END                                        AS total_replication_lag_bytes,
    CASE
      WHEN sent_lsn IS NOT NULL AND write_lsn IS NOT NULL
        THEN (sent_lsn - write_lsn)
      ELSE NULL
    END                                        AS send_lag_bytes,
    CASE
      WHEN write_lsn IS NOT NULL AND flush_lsn IS NOT NULL
        THEN (write_lsn - flush_lsn)
      ELSE NULL
    END                                        AS write_lag_bytes,
    CASE
      WHEN flush_lsn IS NOT NULL AND replay_lsn IS NOT NULL
        THEN (flush_lsn - replay_lsn)
      ELSE NULL
    END                                        AS replay_lag_bytes,
    write_lag                                  AS write_lag_interval,
    flush_lag                                  AS flush_lag_interval,
    replay_lag                                 AS replay_lag_interval,
    EXTRACT(EPOCH FROM write_lag)::numeric(12,3)
                                               AS write_lag_seconds,
    EXTRACT(EPOCH FROM flush_lag)::numeric(12,3)
                                               AS flush_lag_seconds,
    EXTRACT(EPOCH FROM replay_lag)::numeric(12,3)
                                               AS replay_lag_seconds,
    now()                                      AS snapshot_taken_at
  FROM pg_stat_replication
  ORDER BY replay_lag_seconds DESC NULLS LAST;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.replication_lag_history: %', SQLERRM;
END $$;

-- ===========================================================================
-- 12. monitoring.replication_status
--     High-level replication health for each standby connection.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.replication_status AS
  SELECT
    pid                                        AS replication_process_id,
    usename                                    AS replication_username,
    application_name                           AS standby_application_name,
    client_addr                                AS standby_address,
    client_port                                AS standby_port,
    backend_start                              AS replication_started_at,
    state                                      AS replication_state,
    sync_state                                 AS synchronization_mode,
    sync_priority                              AS synchronization_priority,
    sent_lsn                                   AS sent_log_position,
    replay_lsn                                 AS replayed_log_position,
    CASE
      WHEN sent_lsn IS NOT NULL AND replay_lsn IS NOT NULL
        THEN (sent_lsn - replay_lsn)
      ELSE NULL
    END                                        AS replication_lag_bytes,
    CASE
      WHEN sent_lsn IS NOT NULL AND replay_lsn IS NOT NULL
        THEN pg_size_pretty((sent_lsn - replay_lsn)::bigint)
      ELSE 'N/A'
    END                                        AS replication_lag_pretty,
    EXTRACT(EPOCH FROM replay_lag)::numeric(12,3)
                                               AS replay_lag_seconds,
    CASE
      WHEN state = 'streaming' AND (replay_lag IS NULL OR replay_lag < interval '5 seconds')
        THEN 'Healthy'
      WHEN state = 'streaming' AND replay_lag < interval '30 seconds'
        THEN 'Warning - minor lag'
      WHEN state = 'streaming'
        THEN 'Critical - significant lag'
      WHEN state = 'catchup'
        THEN 'Catching up'
      ELSE 'Unhealthy - state: ' || COALESCE(state, 'unknown')
    END                                        AS replication_health
  FROM pg_stat_replication
  ORDER BY standby_application_name;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.replication_status: %', SQLERRM;
END $$;

-- ===========================================================================
-- 13. monitoring.table_size_stats
--     Shows the size of every user table including indexes and TOAST data.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.table_size_stats AS
  SELECT
    n.nspname                                  AS table_schema,
    c.relname                                  AS table_name,
    n.nspname || '.' || c.relname              AS fully_qualified_table_name,
    pg_total_relation_size(c.oid)              AS total_size_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid))
                                               AS total_size_pretty,
    pg_relation_size(c.oid)                    AS table_size_bytes,
    pg_size_pretty(pg_relation_size(c.oid))    AS table_size_pretty,
    pg_indexes_size(c.oid)                     AS indexes_size_bytes,
    pg_size_pretty(pg_indexes_size(c.oid))     AS indexes_size_pretty,
    pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid)
                                               AS toast_and_other_size_bytes,
    ROUND(pg_total_relation_size(c.oid)::numeric / (1024 * 1024), 2)
                                               AS total_size_mb,
    CASE
      WHEN pg_total_relation_size(c.oid) = 0 THEN 0
      ELSE ROUND(pg_relation_size(c.oid)::numeric
                 / pg_total_relation_size(c.oid) * 100, 1)
    END                                        AS table_data_percent_of_total,
    CASE
      WHEN pg_total_relation_size(c.oid) = 0 THEN 0
      ELSE ROUND(pg_indexes_size(c.oid)::numeric
                 / pg_total_relation_size(c.oid) * 100, 1)
    END                                        AS indexes_percent_of_total,
    c.reltuples::bigint                        AS estimated_row_count,
    CASE
      WHEN c.reltuples = 0 THEN 0
      ELSE ROUND((pg_relation_size(c.oid) / GREATEST(c.reltuples, 1))::numeric, 0)
    END                                        AS estimated_bytes_per_row
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  ORDER BY pg_total_relation_size(c.oid) DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.table_size_stats: %', SQLERRM;
END $$;

-- ===========================================================================
-- 14. monitoring.table_stats
--     Comprehensive table-level statistics from pg_stat_user_tables.
-- ===========================================================================
DO $$ BEGIN
  CREATE OR REPLACE VIEW monitoring.table_stats AS
  SELECT
    schemaname                                 AS table_schema,
    relname                                    AS table_name,
    schemaname || '.' || relname               AS fully_qualified_table_name,
    seq_scan                                   AS sequential_scans,
    seq_tup_read                               AS rows_read_by_sequential_scans,
    idx_scan                                   AS index_scans,
    idx_tup_fetch                              AS rows_fetched_by_index_scans,
    CASE
      WHEN (seq_scan + COALESCE(idx_scan, 0)) = 0 THEN 0
      ELSE ROUND(COALESCE(idx_scan, 0)::numeric
                 / (seq_scan + COALESCE(idx_scan, 0)) * 100, 2)
    END                                        AS index_usage_ratio_percent,
    CASE
      WHEN seq_scan > COALESCE(idx_scan, 0) AND COALESCE(idx_scan, 0) > 0
        THEN 'Low - more sequential scans than index scans'
      WHEN COALESCE(idx_scan, 0) = 0 AND seq_scan > 100
        THEN 'Missing - no index scans detected'
      WHEN COALESCE(idx_scan, 0) >= seq_scan
        THEN 'Good'
      ELSE 'N/A'
    END                                        AS index_effectiveness,
    n_tup_ins                                  AS rows_inserted,
    n_tup_upd                                  AS rows_updated,
    n_tup_del                                  AS rows_deleted,
    n_tup_hot_upd                              AS rows_hot_updated,
    n_live_tup                                 AS live_row_count,
    n_dead_tup                                 AS dead_row_count,
    CASE
      WHEN n_live_tup = 0 THEN 0
      ELSE ROUND(n_dead_tup::numeric / GREATEST(n_live_tup, 1) * 100, 2)
    END                                        AS dead_row_ratio_percent,
    n_mod_since_analyze                        AS rows_modified_since_last_analyze,
    n_ins_since_vacuum                         AS rows_inserted_since_last_vacuum,
    last_vacuum                                AS last_manual_vacuum_at,
    last_autovacuum                            AS last_autovacuum_at,
    last_analyze                               AS last_manual_analyze_at,
    last_autoanalyze                           AS last_autoanalyze_at,
    vacuum_count                               AS manual_vacuum_count,
    autovacuum_count                           AS autovacuum_count,
    analyze_count                              AS manual_analyze_count,
    autoanalyze_count                          AS autoanalyze_count
  FROM pg_stat_user_tables
  ORDER BY (seq_scan + COALESCE(idx_scan, 0)) DESC;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not create monitoring.table_stats: %', SQLERRM;
END $$;
