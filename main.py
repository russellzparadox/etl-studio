#!/usr/bin/env python3
"""
SQL Server → PostgreSQL schema migration script.

- Reads all tables in a source schema
- Exports each to a human-readable CSV
- Loads each CSV into a manually-created PostgreSQL table
- Handles type mismatches (float->int, bit->bool, varbinary->bytea, etc.)
- Tolerates schema drift (renamed/dropped columns via COLUMN_RENAMES / SKIP_COLUMNS)
- Loads parents before children (LOAD_ORDER) and can disable FKs during load
- Reports per-table success/failure with a final summary

Requires: pandas, sqlalchemy, psycopg2-binary, pyodbc, ODBC Driver 18 for SQL Server.
"""

import os
import io
import csv
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# ==================================================================
# 1. CONFIG — edit these for your environment
# ==================================================================

# --- SQL Server source ---
SQLSERVER_USER     = 'sa'
SQLSERVER_PASSWORD = 'AHm59wtu'
SQLSERVER_HOST     = '127.0.0.1:1433'
SQLSERVER_DB       = 'raes_system'
SOURCE_SCHEMA      = 'MD'

# --- PostgreSQL target ---
PG_USER     = 'postgres'
PG_PASSWORD = ''
PG_HOST     = 'localhost'
PG_DB       = 'systemtest'
TARGET_SCHEMA = 'md'

# --- Behavior ---
OUTPUT_DIR       = 'csv_export'
LOWERCASE_NAMES  = True              # Postgres folds unquoted names to lowercase
SKIP_TABLES      = {'sysdiagrams'}   # System tables etc. to skip entirely

# Parent-before-child load order. Tables not listed run afterwards (alphabetically).
LOAD_ORDER = [
    'StoredProcedure', 'StoredProcedureParameter',
    'Company', 'Entity', 'EntityColumn',
    'DataSource', 'DataSourceModule', 'DataSourceTypeEntity',
    'Category', 'CategoryProperty', 'CategoryPropertyColumn', 'CategoryMember',
    'GatheringConfig', 'ManualScript', 'ScheduledTask',
    'ScheduledTaskProcedureParameterValue', 'SystemConfig', 'UserAccess',
]

# Per-table column renames: SQL Server name -> Postgres name (after lowercase)
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
# and make sure LOAD_ORDER is complete.
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
    """Return {column_name: {type, udt, nullable}} for a Postgres table."""
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
    """
    Mutate df so its values are parseable by the target Postgres column types.
    Handles: float->int, bit/int->boolean, varbinary->bytea, uuid lowercasing.
    """
    for col in list(df.columns):
        if col not in target_cols:
            continue
        t = target_cols[col]
        pg_type, pg_udt = t['type'], t['udt']
        s = df[col]

        # --- Integer columns ---
        if pg_type in INT_TARGET_TYPES or pg_udt in INT_TARGET_UDTS:
            if pd.api.types.is_float_dtype(s):
                non_null = s.dropna()
                if len(non_null) == 0 or (non_null % 1 == 0).all():
                    df[col] = s.astype('Int64')
            # bool dtype -> int is handled implicitly by pandas writing 1/0

        # --- Boolean columns ---
        elif pg_type in BOOL_TARGET_TYPES or pg_udt in BOOL_TARGET_UDTS:
            if not pd.api.types.is_bool_dtype(s):
                df[col] = s.map(_to_bool)

        # --- bytea columns ---
        elif pg_type == 'bytea' or pg_udt == 'bytea':
            df[col] = s.map(_to_bytea)

        # --- Timestamps / dates ---
        elif pg_type in TIMESTAMP_TYPES:
            # Pandas already returns datetime64 for datetime columns; NaT writes
            # as na_rep (NULL). If the column is object dtype due to mixed types,
            # try to coerce.
            if s.dtype == object:
                try:
                    df[col] = pd.to_datetime(s, errors='coerce')
                except Exception:
                    pass

        # --- UUID ---
        elif pg_type == 'uuid':
            df[col] = s.map(lambda v: str(v).lower()
                            if v is not None and not (isinstance(v, float) and pd.isna(v))
                            else None)

    return df

# ==================================================================
# 5. EXTRACT (SQL Server -> CSV)
# ==================================================================

NA_REP = '\\N'   # Written for NULL; matched by NULL '\N' on COPY

def extract_table(table: str, csv_path: str, target_cols: dict):
    """Read source table, apply renames/skips/coercions, write CSV."""
    query = f"SELECT * FROM [{SOURCE_SCHEMA}].[{table}]"
    df = pd.read_sql(query, sql_engine)

    if LOWERCASE_NAMES:
        df.columns = [c.lower() for c in df.columns]

    # Apply renames
    renames = COLUMN_RENAMES.get(table, {})
    if renames:
        df = df.rename(columns=renames)

    # Drop columns that don't exist in the target (or are explicitly skipped)
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

    # Row count (excluding header)
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        return sum(1 for _ in f) - 1

# ==================================================================
# 7. ORCHESTRATION
# ==================================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_tables = get_source_tables(SOURCE_SCHEMA)
    tables = [t for t in all_tables if t not in SKIP_TABLES]

    # Order: LOAD_ORDER first, then the rest alphabetically
    order_idx = {t: i for i, t in enumerate(LOAD_ORDER)}
    tables.sort(key=lambda t: (order_idx.get(t, 10**6), t))

    print(f"Migrating {len(tables)} tables from "
          f"{SQLSERVER_DB}.{SOURCE_SCHEMA} → {TARGET_SCHEMA}\n")

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
            target_name = table.lower() if LOWERCASE_NAMES else table
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
# 8. OPTIONAL: FK INTEGRITY CHECK (run after migration)
# ==================================================================

def check_fk_integrity():
    """
    After loading with FK checks disabled, verify no orphaned rows exist.
    Run this manually after main() succeeds.
    """
    pg_cursor.execute("""
        SELECT
            tc.table_schema, tc.table_name, kcu.column_name,
            ccu.table_schema AS ref_schema, ccu.table_name AS ref_table,
            ccu.column_name AS ref_column, tc.constraint_name
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
    for (sch, tbl, col, rsch, rtbl, rcol, cname) in pg_cursor.fetchall():
        pg_cursor.execute(f"""
            SELECT COUNT(*) FROM "{sch}"."{tbl}" c
            LEFT JOIN "{rsch}"."{rtbl}" p ON c."{col}" = p."{rcol}"
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
    # Uncomment to run integrity check after:
    # check_fk_integrity()