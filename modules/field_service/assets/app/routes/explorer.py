"""
Data Explorer API — Flask Blueprint
=====================================

All endpoints that power the Data Explorer (view-data) page:
  - /api/tables — list all Lakebase + Unity Catalog tables (cached)
  - /api/query/<schema>/<table> — simple query (first 100 rows)
  - /api/columns/<schema>/<table> — column metadata with auto-type detection
  - /api/recent/<schema>/<table> — last 10 rows ordered by PK desc
  - /api/insert/<schema>/<table> — insert a single row
  - /api/query/<schema>/<table>/paginated — paginated query with sort/search
  - /api/export/<schema>/<table>/csv — streaming CSV export (up to 10 000 rows)

Internal helper:
  - _query_uc_table_paginated() — routes paginated queries for UC tables
    through the SQL Warehouse Statement Execution API instead of Lakebase.

Security:
  All schema, table, and column names pass through validate_identifier()
  before being interpolated into SQL. This prevents SQL injection by
  rejecting anything that is not alphanumeric/underscore/hyphen.

Usage in app.py:
    from routes.explorer import explorer_bp
    app.register_blueprint(explorer_bp)
"""

import csv
import io
import logging
import math
import os
import time
from datetime import datetime

from flask import Blueprint, Response, jsonify, request

from shared import (
    get_pool,
    get_analytics_pool,
    log_error,
    validate_identifier,
    _get_or_refresh,
    GENIE_SPACES,
    get_workspace_client,
    _run_sql,
    get_cached_tables,
)

log = logging.getLogger(__name__)

explorer_bp = Blueprint("explorer", __name__)


# ── UC table paginated query helper ────────────────────────────────────

def _query_uc_table_paginated(catalog, schema, table):
    """Paginated query for Unity Catalog tables via SQL Warehouse.

    UC tables live outside Lakebase and must be queried through the
    Statement Execution API. This helper mirrors the Lakebase paginated
    query but uses the workspace client instead of the PG connection pool.

    Polls with 2 s intervals (up to 60 iterations) for long-running
    statements.
    """
    # Security: validate all identifiers to prevent SQL injection
    validate_identifier(catalog, 'catalog')
    validate_identifier(schema, 'schema')
    validate_identifier(table, 'table')

    page = max(int(request.args.get('page', 1)), 1)
    page_size = min(max(int(request.args.get('page_size', 50)), 1), 500)
    sort_col = request.args.get('sort_col', '')
    sort_dir = request.args.get('sort_dir', 'asc').lower()
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'asc'

    fqn = f'`{catalog}`.`{schema}`.`{table}`'
    wh_id = os.environ.get('SQL_WAREHOUSE_ID', '')
    if not wh_id:
        return jsonify({'error': 'SQL_WAREHOUSE_ID not configured'}), 500

    w = get_workspace_client()

    def _exec(sql):
        """Execute a SQL statement via the Statement Execution API and wait for completion."""
        body = {'warehouse_id': wh_id, 'statement': sql,
                'wait_timeout': '50s', 'catalog': catalog}
        resp = w.api_client.do('POST', '/api/2.0/sql/statements', body=body)
        stmt_id = resp.get('statement_id', '')
        state = resp.get('status', {}).get('state', '')
        polls = 0
        while state in ('PENDING', 'RUNNING') and polls < 60:
            time.sleep(2)
            resp = w.api_client.do('GET', f'/api/2.0/sql/statements/{stmt_id}')
            state = resp.get('status', {}).get('state', '')
            polls += 1
        if state == 'FAILED':
            raise RuntimeError(resp.get('status', {}).get('error', {}).get('message', 'Query failed'))
        return resp

    try:
        # ── Total count ──
        count_resp = _exec(f'SELECT COUNT(*) FROM {fqn}')
        count_data = count_resp.get('result', {}).get('data_array', [])
        total_count = int(count_data[0][0]) if count_data else 0
        total_pages = max(math.ceil(total_count / page_size), 1)

        # ── Main query with sorting and pagination ──
        order_clause = ''
        if sort_col:
            validate_identifier(sort_col, 'sort column')
            order_clause = f'ORDER BY `{sort_col}` {sort_dir} NULLS LAST'
        else:
            order_clause = 'ORDER BY 1 DESC'
        offset = (page - 1) * page_size
        data_resp = _exec(f'SELECT * FROM {fqn} {order_clause} LIMIT {page_size} OFFSET {offset}')

        # Parse columns from the response manifest
        manifest_cols = data_resp.get('manifest', {}).get('schema', {}).get('columns', [])
        columns = [{'name': c.get('name', ''), 'type': c.get('type_text', '')}
                   for c in manifest_cols]
        col_meta = [{'name': c.get('name', ''), 'type': c.get('type_text', ''),
                     'nullable': True, 'auto_type': None}
                    for c in manifest_cols]

        # Parse rows from data_array
        raw_rows = data_resp.get('result', {}).get('data_array', [])
        data = []
        for row in raw_rows:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col['name']] = row[i] if i < len(row) else None
            data.append(row_dict)

        return jsonify({
            'columns': columns, 'data': data,
            'total_count': total_count, 'page': page,
            'page_size': page_size, 'total_pages': total_pages,
            'column_meta': col_meta
        })
    except Exception as e:
        log_error("query_uc_table_paginated", e)
        return jsonify({'error': str(e)}), 500


# ═════════════════════════════════════════════════════════════════════════
# Routes
# ═════════════════════════════════════════════════════════════════════════


@explorer_bp.route('/api/tables')
def list_tables():
    """List all tables from Lakebase and Unity Catalog (cached 5 min).

    Returns a JSON object with:
      - tables: list of {schema, table, source, catalog?}
      - uc_loading: true if Unity Catalog tables are still being fetched
                    (SQL Warehouse may be cold-starting)
    """
    try:
        tables = get_cached_tables()
        from shared import _table_cache
        uc_loading = _table_cache.get("uc_loading", False)
        return jsonify({"tables": tables, "uc_loading": uc_loading})
    except Exception as e:
        log_error("list_tables", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/query/<schema>/<table>')
def query_table(schema, table):
    """Simple query returning the first 100 rows of a Lakebase table.

    Used for quick previews; for full pagination use the /paginated endpoint.
    """
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'/* page:view_data/query */ SELECT * FROM "{schema}"."{table}" LIMIT 100')
                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                columns = [{'name': n} for n in col_names]
                data = [dict(zip(col_names, row)) for row in rows]
        return jsonify({'columns': columns, 'data': data})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("query_table", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/columns/<schema>/<table>')
def get_columns(schema, table):
    """Return column metadata for a Lakebase table.

    Detects auto-populated columns (serial, timestamp defaults, date
    defaults) so the insert form can skip them.
    """
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    /* page:view_data/columns */
                    SELECT column_name, data_type, column_default, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                cols = []
                for name, dtype, default, nullable in cur.fetchall():
                    # Detect auto-populated column types from defaults
                    auto_type = None
                    if default:
                        default_lower = str(default).lower()
                        if 'nextval' in default_lower:
                            auto_type = 'serial'
                        elif 'current_timestamp' in default_lower:
                            auto_type = 'timestamp_default'
                        elif 'current_date' in default_lower:
                            auto_type = 'date_default'
                    cols.append({
                        'name': name,
                        'type': dtype,
                        'default': default,
                        'nullable': nullable == 'YES',
                        'auto_type': auto_type
                    })
        return jsonify(cols)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("get_columns", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/recent/<schema>/<table>')
def get_recent(schema, table):
    """Return the 10 most recent rows (ordered by first column descending)."""
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Single query -- get columns from cursor.description
                cur.execute(f'/* page:view_data/recent */ SELECT * FROM "{schema}"."{table}" ORDER BY 1 DESC LIMIT 10')
                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                data = [
                    dict(zip(col_names, [str(v) if v is not None else None for v in row]))
                    for row in rows
                ]
        return jsonify({'columns': col_names, 'data': data})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("get_recent", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/insert/<schema>/<table>', methods=['POST'])
def insert_data_api(schema, table):
    """Insert a single row into a Lakebase table.

    JSON body: {column_name: value, ...}
    Null/empty values are skipped. All column names are validated.
    Returns the inserted row via RETURNING *.
    """
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')
        data = request.json
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Filter out null/empty values
                columns = [k for k, v in data.items() if v is not None and v != '']
                if not columns:
                    return jsonify({'error': 'No data provided'}), 400

                # Validate every column name to prevent injection
                for col in columns:
                    validate_identifier(col, 'column')

                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join([f'"{col}"' for col in columns])
                query = f'/* page:insert_data */ INSERT INTO "{schema}"."{table}" ({column_names}) VALUES ({placeholders}) RETURNING *'

                values = [data[col] for col in columns]
                cur.execute(query, values)
                returned_row = cur.fetchone()
                conn.commit()

                col_names = [desc[0] for desc in cur.description]
                row_dict = dict(zip(col_names, [str(v) if v is not None else None for v in returned_row]))

        return jsonify({'success': True, 'message': 'Data inserted successfully', 'row': row_dict})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("insert_data", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/query/<schema>/<table>/paginated')
def query_table_paginated(schema, table):
    """Paginated query with sorting and search. Routes to Lakebase or UC.

    Query params:
      - page (int, default 1)
      - page_size (int, default 50, max 500)
      - sort_col (str, optional)
      - sort_dir (str, 'asc' or 'desc', default 'asc')
      - search (str, optional — ILIKE against first 3 text columns)
      - source (str, 'lakebase' or 'uc')
      - catalog (str, required if source='uc')

    Performance notes:
      - Uses reltuples from pg_class for instant row count estimates on
        large tables (avoids COUNT(*) full scan on 5M+ rows)
      - Truncates large text/jsonb columns to 500 chars to avoid
        transferring megabytes of data per page
    """
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')

        # Check if this is a UC table — route to the UC helper if so
        source = request.args.get('source', 'lakebase')
        uc_catalog = request.args.get('catalog', '')
        if source == 'uc' and uc_catalog:
            return _query_uc_table_paginated(uc_catalog, schema, table)

        page = max(int(request.args.get('page', 1)), 1)
        page_size = min(max(int(request.args.get('page_size', 50)), 1), 500)
        sort_col = request.args.get('sort_col', '')
        sort_dir = request.args.get('sort_dir', 'asc').lower()
        search = request.args.get('search', '').strip()

        if sort_dir not in ('asc', 'desc'):
            sort_dir = 'asc'

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # ── Total count ──
                # Use reltuples estimate for non-search queries (instant, no full scan)
                if search:
                    # Get text columns for ILIKE search
                    cur.execute("""
                        /* page:view_data/paginated:text_cols */
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND data_type IN ('text', 'character varying', 'varchar', 'char', 'character')
                        ORDER BY ordinal_position
                    """, (schema, table))
                    text_cols = [r[0] for r in cur.fetchall()]
                    if text_cols:
                        # Search across up to 3 text columns for performance
                        where_clauses = ' OR '.join([f'"{c}"::text ILIKE %s' for c in text_cols[:3]])
                        search_params = [f'%{search}%'] * min(len(text_cols), 3)
                        # Cap the count scan at 10 000 for responsiveness
                        cur.execute(f'/* page:view_data/paginated:count */ SELECT COUNT(*) FROM (SELECT 1 FROM "{schema}"."{table}" WHERE {where_clauses} LIMIT 10000) _c', search_params)
                    else:
                        # No text columns — fall back to reltuples estimate
                        cur.execute("""
                            /* page:view_data/paginated:count_estimate */
                            SELECT GREATEST(reltuples::bigint, 0) FROM pg_class c
                            JOIN pg_namespace n ON c.relnamespace = n.oid
                            WHERE n.nspname = %s AND c.relname = %s
                        """, (schema, table))
                        search = ''  # disable search since no text columns
                else:
                    # Use reltuples estimate — instant, avoids COUNT(*) full scan
                    cur.execute("""
                        /* page:view_data/paginated:count_estimate */
                        SELECT GREATEST(reltuples::bigint, 0) FROM pg_class c
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = %s AND c.relname = %s
                    """, (schema, table))
                total_count = cur.fetchone()[0]
                total_pages = max(math.ceil(total_count / page_size), 1)

                # ── Sort clause ──
                order_clause = ''
                if sort_col:
                    validate_identifier(sort_col, 'sort column')
                    order_clause = f'ORDER BY "{sort_col}" {sort_dir} NULLS LAST'
                else:
                    order_clause = 'ORDER BY 1 DESC'

                offset = (page - 1) * page_size

                # ── Column list — truncate large text/jsonb to 500 chars ──
                cur.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                col_defs = cur.fetchall()
                select_cols = []
                for cname, dtype, max_len in col_defs:
                    if dtype in ('text', 'character varying') and (max_len is None or max_len > 500):
                        select_cols.append(f'LEFT("{cname}", 500) AS "{cname}"')
                    elif dtype == 'jsonb':
                        select_cols.append(f'LEFT("{cname}"::text, 500) AS "{cname}"')
                    else:
                        select_cols.append(f'"{cname}"')
                select_list = ', '.join(select_cols) if select_cols else '*'

                # ── Execute main query ──
                if search and text_cols:
                    where_clauses = ' OR '.join([f'"{c}"::text ILIKE %s' for c in text_cols[:3]])
                    search_params = [f'%{search}%'] * min(len(text_cols), 3)
                    cur.execute(
                        f'/* page:view_data/paginated */ SELECT {select_list} FROM "{schema}"."{table}" WHERE {where_clauses} {order_clause} LIMIT %s OFFSET %s',
                        search_params + [page_size, offset]
                    )
                else:
                    cur.execute(
                        f'/* page:view_data/paginated */ SELECT {select_list} FROM "{schema}"."{table}" {order_clause} LIMIT %s OFFSET %s',
                        [page_size, offset]
                    )

                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                # ── Fetch column type metadata for the UI ──
                cur.execute("""
                    /* page:view_data/paginated:col_types */
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                col_type_map = {}
                col_meta_list = []
                for cname, ctype, cnull, cdefault in cur.fetchall():
                    col_type_map[cname] = ctype
                    auto_type = None
                    if cdefault:
                        dl = str(cdefault).lower()
                        if 'nextval' in dl:
                            auto_type = 'serial'
                        elif 'current_timestamp' in dl:
                            auto_type = 'timestamp_default'
                        elif 'current_date' in dl:
                            auto_type = 'date_default'
                    col_meta_list.append({
                        'name': cname, 'type': ctype,
                        'nullable': cnull == 'YES',
                        'default': cdefault,
                        'auto_type': auto_type
                    })

                columns = [{'name': n, 'type': col_type_map.get(n, '')} for n in col_names]
                data = []
                for row in rows:
                    row_dict = {}
                    for i, val in enumerate(row):
                        if val is None:
                            row_dict[col_names[i]] = None
                        elif isinstance(val, (datetime,)):
                            row_dict[col_names[i]] = str(val)
                        else:
                            row_dict[col_names[i]] = val
                    data.append(row_dict)

        return jsonify({
            'columns': columns, 'data': data,
            'total_count': total_count, 'page': page,
            'page_size': page_size, 'total_pages': total_pages,
            'column_meta': col_meta_list
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("query_table_paginated", e)
        return jsonify({'error': str(e)}), 500


@explorer_bp.route('/api/export/<schema>/<table>/csv')
def export_csv(schema, table):
    """Export table data as a streaming CSV download (up to 10 000 rows).

    Uses a generator to stream rows as they are fetched from the cursor,
    keeping memory usage constant regardless of table size.
    """
    try:
        validate_identifier(schema, 'schema')
        validate_identifier(table, 'table')
        pool = get_pool()

        def generate():
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'/* page:view_data/export */ SELECT * FROM "{schema}"."{table}" ORDER BY 1 DESC LIMIT 10000')
                    col_names = [desc[0] for desc in cur.description]
                    output = io.StringIO()
                    writer = csv.writer(output)
                    # Header row
                    writer.writerow(col_names)
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)
                    # Data rows — streamed one at a time
                    for row in cur:
                        writer.writerow([str(v) if v is not None else '' for v in row])
                        yield output.getvalue()
                        output.seek(0)
                        output.truncate(0)

        filename = f"{schema}_{table}.csv"
        return Response(
            generate(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        log_error("export_csv", e)
        return jsonify({'error': str(e)}), 500
