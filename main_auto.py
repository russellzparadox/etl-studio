#!/usr/bin/env python3
"""
SQL Server → PostgreSQL schema migration script.

- Discovers all tables in a source schema
- Exports each to a human-readable CSV
- Loads each CSV into a manually-created PostgreSQL table
- Auto-detects load order from FK constraints in the target schema
- Handles type mismatches (float->int, bit->bool, varbinary->bytea, etc.)
- Tolerates schema drift (renamed/dropped columns via COLUMN_RENAMES / SKIP_COLUMNS)
- Reports per-table success/failure with a final summary

Requires: pandas, sqlalchemy, psycopg2-binary, pyodbc, ODBC Driver 18 for SQL Server.
"""

import os
import io
import pandas as pd
from collections import defaultdict, deque
from sqlalchemy import create_engine, text
import psycopg2

# ==================================================================
# 1. CONFIG
# ==================================================================

# --- SQL Server source ---
SQLSERVER_USER     = 'sa'
SQLSERVER_PASSWORD = 'YourPassword'
SQLSERVER_HOST     = 'localhost'
SQLSERVER_DB       = 'MyDB'
SOURCE_SCHEMA      = 'dbo'

# --- PostgreSQL target ---
PG_USER     = 'postgres'
PG_PASSWORD = 'YourPgPassword'
PG_HOST     = 'localhost'
PG_DB       = 'MyPgDB'
TARGET_SCHEMA = 'md'

# --- Behavior ---
OUTPUT_DIR      = 'csv_export'
LOWERCASE_NAMES = True               # Postgres folds unquoted names to lowercase
SKIP_TABLES     = {'sysdiagrams'}    # System tables etc. to skip entirely

# Load order: set to None to auto-detect from FK constraints in the target schema.
# Provide an explicit list to override (source-case table names).
LOAD_ORDER = None

# Per-table column renames: SQL Server name -> Postgres name (already lowercase)
COLUMN_RENAMES = {
    'EntityColumn':                         {'size':   'length'},
    'ScheduledTaskProcedureParameterValue': {'isnull': 'is_null'},
}

# Per-table columns present in SQL Server but not in Postgres (dropped silently)
SKIP_COLUMNS = {
    # 'SomeTable': {'legacy_col'},
}

# Wrap the whole load in SET session_replication_role = 'replica' to disable FK
# checks. Requires superuser. If you prefer strict FK enforcement, set to False
# and make sure the detected load order is complete.
DISABLE_FK_DURING_LOAD = True

# ==================================================================
# 2. CONNECTIONS
# ==================================================================

sql_engine = create_engine(
    f"mssql+pyodbc://{SQLSERVER_USER}:{SQLSERVER_PASSWORD}"
    f"@{SQLSERVER_HOST}/{SQLSERVER_DB}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&TrustServerCertificate=yes"
)

pg_conn = psycopg2.connect(
    dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST
)
pg_conn.autocommit = True
pg_cursor = pg_conn.cursor()

# ==================================================================
# 3. SCHEMA INTROSPECTION
# ==================================================================

def to_target_name(src_name: str) -> str:
    return src_name.lower() if LOWERCASE_NAMES else src_name


def get_source_tables(schema: str):
    q = text("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema
        ORDER BY TABLE_NAME
    """)
    with sql_engine.connect() as c:
        return [r[0] for r in c.execute(q, {"schema": schema})]


def get_target_columns(table: str, schema: str):
    pg_cursor.execute("""
        SELECT column_name, data_type, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return {
        r[0]: {'type': r[1].lower(), 'udt': r[2].lower(), 'nullable': r[3] == 'YES'}
        for r in pg_cursor.fetchall()
    }


# ------------------------------------------------------------------
# 3a. AUTO-DETECT LOAD ORDER FROM FK CONSTRAINTS
# ------------------------------------------------------------------

def get_load_order(source_tables, target_schema: str):
    """
    Return a list of source-case table names, sorted so that a table's FK
    parents are always loaded before the table itself.

    Reads FKs from the target Postgres schema (that's where enforcement
    happens). Self-references are ignored. Cycles are reported and their
    members are appended at the end (they'll still load if
    DISABLE_FK_DURING_LOAD is True).
    """
    # Map target-name (lowercased) -> source-name, so we can translate back
    name_map = {to_target_name(t): t for t in source_tables}
    target_names = set(name_map.keys())

    # Query FKs: child depends on parent
    pg_cursor.execute("""
        SELECT DISTINCT
            child.relname  AS child_table,
            parent.relname AS parent_table
        FROM pg_constraint c
        JOIN pg_class     child  ON child.oid  = c.conrelid
        JOIN pg_class     parent ON parent.oid = c.confrelid
        JOIN pg_namespace n      ON n.oid      = c.connamespace
        WHERE c.contype = 'f'
          AND n.nspname = %s
    """, (target_schema,))
    fk_rows = pg_cursor.fetchall()

    # Build graph, ignoring self-references and FKs to tables outside our set
    parents_of = defaultdict(set)  # child -> {parents}
    for child, parent in fk_rows:
        child_l, parent_l = child.lower(), parent.lower()
        if child_l == parent_l:
            continue  # self-reference: safe to ignore
        if child_l in target_names and parent_l in target_names:
            parents_of[child_l].add(parent_l)

    # Kahn's algorithm
    in_degree = {t: len(parents_of.get(t, ())) for t in target_names}
    children_of = defaultdict(set)
    for child, parents in parents_of.items():
        for p in parents:
            children_of[p].add(child)

    ready = deque(sorted(t for t in target_names if in_degree[t] == 0))
    ordered_targets = []
    while ready:
        t = ready.popleft()
        ordered_targets.append(t)
        for c in sorted(children_of[t]):
            in_degree[c] -= 1
            if in_degree[c] == 0:
                ready.append(c)

    # Anything left is part of a cycle
    remaining = sorted(target_names - set(ordered_targets))
    if remaining:
        print(f"    ! circular FK dependency among: {remaining}")
        print(f"    ! these will load last; rely on DISABLE_FK_DURING_LOAD=True")
        ordered_targets.extend(remaining)

    # Translate back to source-case names
    return [name_map[t] for t in ordered_targets]


# ==================================================================
# 4. TYPE COERCION (SQL Server -> PostgreSQL)
# ==================================================================

INT_TARGET_TYPES = {'smallint', 'integer', 'bigint'}
INT_TARGET_UDTS  = {'int2', 'int4', 'int8'}

BOOL_TARGET_TYPES = {'boolean'}
BOOL_TARGET_UDTS  = {'bool'}

TIMESTAMP_TYPES = {
    'date', 'timestamp', 'timestamp with time zone', 'timestamp without time zone',
    'time', 'time with time zone', 'time without time zone',
}


def _to_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return 't' if v else 'f'
    try:
        return 't' if int(v) != 0 else 'f'
    except (ValueError, TypeError):
        pass
    s = str(v).strip().lower()
    if s in ('true', 't', 'yes', 'y', 'on', '1'):
        return 't'
    if s in ('false', 'f', 'no', 'n', 'off', '0'):
        return 'f'
    return None


def _to_bytea(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return '\\x' + bytes(v).hex()
    s = str(v)
    return s if s.startswith('\\x') else '\\x' + s


def coerce_for_target(df: pd.DataFrame, target_cols: dict) -> pd.DataFrame:
    """Mutate df so values are parseable by the target Postgres column types."""
    for col in list(df.columns):
        if col not in target_cols:
            continue
        t = target_cols[col]
        pg_type, pg_udt = t['type'], t['udt']
        s = df[col]

        if pg_type in INT_TARGET_TYPES or pg_udt in INT_TARGET_UDTS:
            if pd.api.types.is_float_dtype(s):
                non_null = s.dropna()
                if len(non_null) == 0 or (non_null % 1 == 0).all():
                    df[col] = s.astype('Int64')

        elif pg_type in BOOL_TARGET_TYPES or pg_udt in BOOL_TARGET_UDTS:
            if not pd.api.types.is_bool_dtype(s):
                df[col] = s.map(_to_bool)

        elif pg_type == 'bytea' or pg_udt == 'bytea':
            df[col] = s.map(_to_bytea)

        elif pg_type in TIMESTAMP_TYPES:
            if s.dtype == object:
                try:
                    df[col] = pd.to_datetime(s, errors='coerce')
                except Exception:
                    pass

        elif pg_type == 'uuid':
            df[col] = s.map(lambda v: str(v).lower()
                            if v is not None and not (isinstance(v, float) and pd.isna(v))
                            else None)

    return df


# ==================================================================
# 5. EXTRACT (SQL Server -> CSV)
# ==================================================================

NA_REP = '\\N'

def extract_table(table: str, csv_path: str, target_cols: dict):
    query = f"SELECT * FROM [{SOURCE_SCHEMA}].[{table}]"
    df = pd.read_sql(query, sql_engine)

    if LOWERCASE_NAMES:
        df.columns = [c.lower() for c in df.columns]

    renames = COLUMN_RENAMES.get(table, {})
    if renames:
        df = df.rename(columns=renames)

    skips = SKIP_COLUMNS.get(table, set())
    keep, dropped = [], []
    for c in df.columns:
        if c in skips:
            dropped.append(c)
        elif c in target_cols:
            keep.append(c)
        else:
            dropped.append(c)
    if dropped:
        print(f"    ! dropping columns not present in target: {dropped}")
    df = df[keep]

    if not list(df.columns):
        raise RuntimeError(f"No columns of {table} match the target table")

    df = coerce_for_target(df, target_cols)
    df.to_csv(csv_path, index=False, encoding='utf-8', na_rep=NA_REP)
    return len(df), list(df.columns)


# ==================================================================
# 6. LOAD (CSV -> PostgreSQL)
# ==================================================================

def load_table(table: str, csv_path: str, columns: list):
    target_name = table.lower() if LOWERCASE_NAMES else table
    qualified   = f'"{TARGET_SCHEMA}"."{target_name}"'
    col_list    = ", ".join(f'"{c}"' for c in columns)

    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        pg_cursor.copy_expert(
            f"COPY {qualified} ({col_list}) "
            f"FROM STDIN WITH CSV HEADER NULL '{NA_REP}'",
            f
        )

    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        return sum(1 for _ in f) - 1


# ==================================================================
# 7. ORCHESTRATION
# ==================================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_tables = get_source_tables(SOURCE_SCHEMA)
    tables = [t for t in all_tables if t not in SKIP_TABLES]

    # ---- Determine load order ----
    if LOAD_ORDER:
        # Manual override: keep only tables that exist in the source
        source_set = set(tables)
        order_idx = {t: i for i, t in enumerate(LOAD_ORDER)}
        tables = [t for t in LOAD_ORDER if t in source_set]
        tables += sorted(t for t in source_set if t not in order_idx)
        print("Using manual LOAD_ORDER.")
    else:
        print("Auto-detecting load order from FK constraints...")
        tables = get_load_order(tables, TARGET_SCHEMA)

    print(f"\nMigrating {len(tables)} tables "
          f"from {SQLSERVER_DB}.{SOURCE_SCHEMA} → {TARGET_SCHEMA}")
    print(f"Load order:\n  " + " → ".join(tables) + "\n")

    if DISABLE_FK_DURING_LOAD:
        try:
            pg_cursor.execute("SET session_replication_role = 'replica'")
            print("FK checks disabled for this session.\n")
        except Exception as e:
            print(f"! Could not disable FK checks "
                  f"(need superuser?): {e}\n")

    succeeded, failed = [], []

    for table in tables:
        print(f"--- {table} ---")
        csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")
        try:
            target_name = to_target_name(table)
            target_cols = get_target_columns(target_name, TARGET_SCHEMA)
            if not target_cols:
                raise RuntimeError(
                    f"target table {TARGET_SCHEMA}.{target_name} "
                    f"not found or has no columns"
                )

            exported, columns = extract_table(table, csv_path, target_cols)
            loaded = load_table(table, csv_path, columns)
            print(f"  ✓ exported {exported} rows, loaded {loaded} rows\n")
            succeeded.append((table, loaded))
        except Exception as e:
            msg = str(e).strip().replace('\n', ' | ')
            print(f"  ✗ FAILED: {msg}\n")
            failed.append((table, msg))

    if DISABLE_FK_DURING_LOAD:
        try:
            pg_cursor.execute("SET session_replication_role = 'origin'")
        except Exception:
            pass

    print("=" * 60)
    print(f"Done. {len(succeeded)} succeeded, {len(failed)} failed.")
    if succeeded:
        print("\nSucceeded:")
        for t, n in succeeded:
            print(f"  ✓ {t} ({n} rows)")
    if failed:
        print("\nFailed:")
        for t, err in failed:
            print(f"  ✗ {t}: {err[:200]}")

    pg_cursor.close()
    pg_conn.close()


# ==================================================================
# 8. OPTIONAL: FK INTEGRITY CHECK
# ==================================================================

def check_fk_integrity():
    pg_cursor.execute("""
        SELECT
            tc.table_name, kcu.column_name,
            ccu.table_name AS ref_table, ccu.column_name AS ref_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s
    """, (TARGET_SCHEMA,))

    broken = []
    for (tbl, col, rtbl, rcol) in pg_cursor.fetchall():
        pg_cursor.execute(f"""
            SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{tbl}" c
            LEFT JOIN "{TARGET_SCHEMA}"."{rtbl}" p ON c."{col}" = p."{rcol}"
            WHERE c."{col}" IS NOT NULL AND p."{rcol}" IS NULL
        """)
        cnt = pg_cursor.fetchone()[0]
        if cnt:
            broken.append((tbl, col, rtbl, cnt))

    if not broken:
        print("✓ All FKs satisfied.")
    else:
        print("✗ Orphaned rows detected:")
        for tbl, col, rtbl, cnt in broken:
            print(f"  {tbl}.{col} -> {rtbl}: {cnt} orphan row(s)")


if __name__ == "__main__":
    main()
    # Uncomment to verify no orphaned rows after loading with FKs disabled:
    # check_fk_integrity()