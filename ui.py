#!/usr/bin/env python3
"""
ETL Studio — bidirectional SQL Server <-> PostgreSQL migration tool with a
native Tkinter UI.

Run:  python etl_studio.py
"""

import os
import re
import sys
import json
import threading
import queue
import traceback
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from collections import defaultdict, deque
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import psycopg2


# ==================================================================
# DEFAULTS
# ==================================================================

DEFAULT_CONFIG = {
    "direction": "mssql_to_pg",     # or "pg_to_mssql"
    "sqlserver": {
        "host": "localhost",
        "port": 1433,
        "user": "sa",
        "password": "",
        "database": "MyDB",
        "trust_cert": True,
    },
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "",
        "database": "MyPgDB",
    },
    "migration": {
        "convert_snake_case": True,
        "lowercase_names": True,
        "create_missing_tables": True,
        "disable_fk_during_load": True,
        "skip_tables": [],
        "output_dir": "csv_export",
    },
    # source schema -> target schema
    "schema_map": {
        "MD":   "md",
        "GNR":  "gnr",
    },
    # Column-level transformations
    "table_merges": {
        # "CategoryProperty": {
        #     "merge_into":  "Category",
        #     "join_on":     [["CategoryID", "id"]],
        #     "on_conflict": "keep_target",
        #     "on_multi":    "error",
        # }
    },
    "tree_inheritance": {
        # "Category": {
        #     "id_col":     "id",
        #     "parent_col": "parentid",
        #     "columns":    ["entityid"],
        # }
    },
    "fk_redirects": {
        # "CategoryPropertyColumn": [{
        #     "from_column":  "CategoryPropertyID",
        #     "via_table":    "CategoryProperty",
        #     "via_key":      "id",
        #     "value_column": "CategoryID",
        #     "to_column":    "CategoryID",
        # }]
    },
    "column_from_related": {
        # "CategoryMember": [{
        #     "from_table":  "Category",
        #     "from_column": "entityid",
        #     "via_key":     "id",
        #     "via_fk":      "CategoryID",
        #     "to_column":   "entityid",
        # }]
    },
    "column_renames": {},
    "skip_columns": {},
    "post_create_ddl": [],
    "post_load_ddl": [],
}


# ==================================================================
# TYPE MAPPINGS
# ==================================================================

MSSQL_TO_PG_TYPES = {
    'int': 'integer', 'bigint': 'bigint', 'smallint': 'smallint',
    'tinyint': 'smallint', 'bit': 'boolean',
    'decimal': 'numeric', 'numeric': 'numeric',
    'money': 'numeric(19,4)', 'smallmoney': 'numeric(10,4)',
    'float': 'double precision', 'real': 'real',
    'char': 'char', 'nchar': 'char', 'varchar': 'varchar', 'nvarchar': 'varchar',
    'text': 'text', 'ntext': 'text', 'xml': 'xml', 'sysname': 'varchar(128)',
    'binary': 'bytea', 'varbinary': 'bytea', 'image': 'bytea',
    'date': 'date', 'time': 'time',
    'datetime': 'timestamp', 'datetime2': 'timestamp',
    'smalldatetime': 'timestamp', 'datetimeoffset': 'timestamptz',
    'timestamp': 'bytea', 'rowversion': 'bytea',
    'uniqueidentifier': 'uuid', 'sql_variant': 'text',
}

PG_TO_MSSQL_TYPES = {
    'integer': 'int', 'int4': 'int',
    'bigint': 'bigint', 'int8': 'bigint',
    'smallint': 'smallint', 'int2': 'smallint',
    'boolean': 'bit', 'bool': 'bit',
    'numeric': 'decimal', 'decimal': 'decimal',
    'real': 'real', 'float4': 'real',
    'double precision': 'float', 'float8': 'float',
    'money': 'money',
    'character varying': 'nvarchar', 'varchar': 'nvarchar',
    'character': 'nchar', 'char': 'nchar', 'bpchar': 'nchar',
    'text': 'nvarchar(max)',
    'bytea': 'varbinary(max)',
    'uuid': 'uniqueidentifier',
    'date': 'date',
    'time': 'time',
    'time without time zone': 'time',
    'time with time zone': 'time',
    'timestamp': 'datetime2',
    'timestamp without time zone': 'datetime2',
    'timestamp with time zone': 'datetimeoffset',
    'timestamptz': 'datetimeoffset',
    'xml': 'xml',
    'json': 'nvarchar(max)',
    'jsonb': 'nvarchar(max)',
    'inet': 'nvarchar(64)',
    'cidr': 'nvarchar(64)',
    'macaddr': 'nvarchar(32)',
}

PG_TYPES_NO_PARAMS = {
    'integer', 'bigint', 'smallint', 'boolean', 'double precision', 'real',
    'text', 'xml', 'bytea', 'date', 'time', 'timestamp', 'timestamptz', 'uuid',
    'json', 'jsonb',
}

MSSQL_TYPES_NO_PARAMS = {
    'int', 'bigint', 'smallint', 'tinyint', 'bit', 'date', 'time',
    'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset',
    'text', 'ntext', 'image', 'uniqueidentifier', 'money', 'smallmoney',
    'float', 'real', 'xml', 'timestamp', 'rowversion', 'sql_variant',
    'nvarchar(max)', 'varchar(max)', 'varbinary(max)',
}


# ==================================================================
# NORMALIZATION
# ==================================================================

_SNAKE_1 = re.compile(r'(.)([A-Z][a-z]+)')
_SNAKE_2 = re.compile(r'([a-z0-9])([A-Z])')


def to_snake_case(name):
    s1 = _SNAKE_1.sub(r'\1_\2', name)
    s2 = _SNAKE_2.sub(r'\1_\2', s1)
    return s2.lower()


def make_normalizer(convert_snake_case, lowercase):
    def normalize(name):
        if convert_snake_case:
            return to_snake_case(name)
        if lowercase:
            return name.lower()
        return name
    return normalize


def qident_pg(name):
    return '"' + name.replace('"', '""') + '"'


def qident_mssql(name):
    return '[' + name.replace(']', ']]') + ']'


# ==================================================================
# SOURCE ADAPTERS
# ==================================================================

class MSSQLSource:
    dialect = 'mssql'

    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        trust = 'yes' if cfg.get('trust_cert') else 'no'
        self.engine = create_engine(
            f"mssql+pyodbc://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg.get('port', 1433)}/{cfg['database']}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
            f"&TrustServerCertificate={trust}"
        )

    def test(self):
        with self.engine.connect() as c:
            c.execute(text("SELECT 1"))

    def list_tables(self, schemas):
        q = text("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN :schemas
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """).bindparams(bindparam("schemas", expanding=True))
        with self.engine.connect() as c:
            rows = c.execute(q, {"schemas": list(schemas)}).fetchall()
        return [f'{r[0]}.{r[1]}' for r in rows]

    def get_columns(self, table_key):
        schema, table = table_key.split('.', 1)
        q = text("""
            SELECT c.name AS column_name, t.name AS data_type,
                   c.max_length AS max_length, c.precision AS precision,
                   c.scale AS scale, c.is_nullable AS is_nullable,
                   c.is_identity AS is_identity, c.is_computed AS is_computed,
                   dc.definition AS default_definition
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.default_constraints dc
              ON dc.parent_object_id = c.object_id
             AND dc.parent_column_id = c.column_id
            WHERE c.object_id = OBJECT_ID(:qualified)
            ORDER BY c.column_id
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"qualified": f'[{schema}].[{table}]'}).fetchall()
        return [dict(r._mapping) for r in rows]

    def get_constraints(self, table_key):
        schema, table = table_key.split('.', 1)
        qualified = f'[{schema}].[{table}]'
        pk_q = text("""
            SELECT i.name AS constraint_name, i.is_primary_key AS is_primary,
                   c.name AS column_name, ic.key_ordinal AS ordinal
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID(:qualified)
              AND (i.is_primary_key = 1 OR i.is_unique_constraint = 1)
              AND ic.is_included_column = 0
            ORDER BY i.name, ic.key_ordinal
        """)
        fk_q = text("""
            SELECT fk.name AS constraint_name,
                   OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
                   OBJECT_NAME(fk.referenced_object_id) AS ref_table,
                   cp.name AS parent_column, cr.name AS referenced_column,
                   fkc.constraint_column_id AS ordinal,
                   fk.delete_referential_action_desc AS on_delete,
                   fk.update_referential_action_desc AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id
                                AND fkc.parent_column_id = cp.column_id
            JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id
                                AND fkc.referenced_column_id = cr.column_id
            WHERE fk.parent_object_id = OBJECT_ID(:qualified)
            ORDER BY fk.name, fkc.constraint_column_id
        """)
        with self.engine.connect() as conn:
            pk_rows = conn.execute(pk_q, {"qualified": qualified}).fetchall()
            fk_rows = conn.execute(fk_q, {"qualified": qualified}).fetchall()

        primary_key = None
        uniques = defaultdict(list)
        for r in pk_rows:
            if r.is_primary:
                primary_key = primary_key or {'name': r.constraint_name, 'columns': []}
                primary_key['columns'].append((r.ordinal, r.column_name))
            else:
                uniques[r.constraint_name].append((r.ordinal, r.column_name))
        if primary_key:
            primary_key['columns'] = [c for _, c in sorted(primary_key['columns'])]
        uniques_list = [{'name': n, 'columns': [c for _, c in sorted(cols)]}
                        for n, cols in uniques.items()]

        fk_groups = defaultdict(lambda: {'ref_key': None, 'columns': [],
                                         'ref_columns': [], 'on_delete': None,
                                         'on_update': None})
        for r in fk_rows:
            g = fk_groups[r.constraint_name]
            g['ref_key'] = f'{r.ref_schema}.{r.ref_table}'
            g['on_delete'] = r.on_delete
            g['on_update'] = r.on_update
            g['columns'].append((r.ordinal, r.parent_column))
            g['ref_columns'].append((r.ordinal, r.referenced_column))
        fks = [{'name': n, 'ref_key': g['ref_key'],
                'columns': [c for _, c in sorted(g['columns'])],
                'ref_columns': [c for _, c in sorted(g['ref_columns'])],
                'on_delete': g['on_delete'], 'on_update': g['on_update']}
               for n, g in fk_groups.items()]
        return {'primary_key': primary_key, 'uniques': uniques_list,
                'foreign_keys': fks}

    def read_table(self, table_key):
        schema, table = table_key.split('.', 1)
        df = pd.read_sql(
            f"SELECT * FROM [{schema}].[{table}]", self.engine)
        df.columns = [self.normalize(c) for c in df.columns]
        return df


class PGSource:
    dialect = 'postgres'

    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        self.conn = psycopg2.connect(
            dbname=cfg['database'], user=cfg['user'],
            password=cfg['password'], host=cfg['host'],
            port=cfg.get('port', 5432)
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def test(self):
        self.cursor.execute("SELECT 1")

    def list_tables(self, schemas):
        self.cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema = ANY(%s)
            ORDER BY table_schema, table_name
        """, (list(schemas),))
        return [f'{r[0]}.{r[1]}' for r in self.cursor.fetchall()]

    def get_columns(self, table_key):
        schema, table = table_key.split('.', 1)
        self.cursor.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.udt_name,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.is_identity
            FROM information_schema.columns c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (schema, table))
        cols = []
        for r in self.cursor.fetchall():
            cols.append({
                'column_name': r[0],
                'data_type': r[1].lower(),
                'udt_name': r[2].lower(),
                'max_length': r[3] if r[3] is not None else -1,
                'precision': r[4] if r[4] is not None else 0,
                'scale': r[5] if r[5] is not None else 0,
                'is_nullable': (r[6] == 'YES'),
                'default_definition': r[7],
                'is_identity': r[8] == 'YES',
                'is_computed': False,
            })
        return cols

    def get_constraints(self, table_key):
        schema, table = table_key.split('.', 1)
        # Primary key / uniques
        self.cursor.execute("""
            SELECT tc.constraint_name, tc.constraint_type,
                   kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
              AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """, (schema, table))
        prim = None
        uniq = defaultdict(list)
        for cname, ctype, col, ordinal in self.cursor.fetchall():
            if ctype == 'PRIMARY KEY':
                prim = prim or {'name': cname, 'columns': []}
                prim['columns'].append((ordinal, col))
            else:
                uniq[cname].append((ordinal, col))
        if prim:
            prim['columns'] = [c for _, c in sorted(prim['columns'])]
        uniq_list = [{'name': n, 'columns': [c for _, c in sorted(cols)]}
                     for n, cols in uniq.items()]

        # Foreign keys
        self.cursor.execute("""
            SELECT
                tc.constraint_name,
                ccu.table_schema AS ref_schema,
                ccu.table_name   AS ref_table,
                kcu.column_name  AS parent_column,
                ccu.column_name  AS ref_column,
                kcu.ordinal_position AS ordinal,
                rc.delete_rule,
                rc.update_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints rc
              ON rc.constraint_name = tc.constraint_name
             AND rc.constraint_schema = tc.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """, (schema, table))
        groups = defaultdict(lambda: {'ref_key': None, 'columns': [],
                                      'ref_columns': [], 'on_delete': None,
                                      'on_update': None})
        for cname, rsch, rtbl, col, rcol, ord_, dr, ur in self.cursor.fetchall():
            g = groups[cname]
            g['ref_key'] = f'{rsch}.{rtbl}'
            g['on_delete'] = dr
            g['on_update'] = ur
            g['columns'].append((ord_, col))
            g['ref_columns'].append((ord_, rcol))
        fks = [{'name': n, 'ref_key': g['ref_key'],
                'columns': [c for _, c in sorted(g['columns'])],
                'ref_columns': [c for _, c in sorted(g['ref_columns'])],
                'on_delete': g['on_delete'], 'on_update': g['on_update']}
               for n, g in groups.items()]
        return {'primary_key': prim, 'uniques': uniq_list, 'foreign_keys': fks}

    def read_table(self, table_key):
        schema, table = table_key.split('.', 1)
        self.cursor.execute(
            f'SELECT * FROM {qident_pg(schema)}.{qident_pg(table)}')
        cols = [d[0] for d in self.cursor.description]
        rows = self.cursor.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        df.columns = [self.normalize(c) for c in df.columns]
        return df


# ==================================================================
# TARGET ADAPTERS
# ==================================================================

class PGTarget:
    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        self.conn = psycopg2.connect(
            dbname=cfg['database'], user=cfg['user'],
            password=cfg['password'], host=cfg['host'],
            port=cfg.get('port', 5432)
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def test(self):
        self.cursor.execute("SELECT 1")

    def ensure_schema(self, schema):
        self.cursor.execute(
            f'CREATE SCHEMA IF NOT EXISTS {qident_pg(schema)}')

    def table_exists(self, schema, table):
        self.cursor.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return self.cursor.fetchone() is not None

    def get_columns(self, schema, table):
        self.cursor.execute("""
            SELECT column_name, data_type, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return {
            r[0]: {'type': r[1].lower(), 'udt': r[2].lower(),
                   'nullable': r[3] == 'YES'}
            for r in self.cursor.fetchall()
        }

    def execute(self, sql):
        self.cursor.execute(sql)

    def render_type(self, col):
        """Translate a source column dict into a PG type string."""
        sql_type = col['data_type'].lower()
        if sql_type not in MSSQL_TO_PG_TYPES:
            raise ValueError(f"Unmapped source type: {sql_type}")
        base = MSSQL_TO_PG_TYPES[sql_type]
        if (base in PG_TYPES_NO_PARAMS
                or base.startswith('numeric(')
                or base.startswith('varchar(')):
            return base
        if base in ('varchar', 'char'):
            if col['max_length'] == -1:
                return 'text'
            length = (col['max_length'] // 2
                      if sql_type in ('nvarchar', 'nchar') else col['max_length'])
            return f'{base}({length})'
        if base == 'numeric':
            return f'numeric({col["precision"]},{col["scale"]})'
        return base

    def translate_default(self, sql_default, pg_type):
        if not sql_default:
            return None
        d = sql_default.strip()
        rules = [
            (r'^\(?getdate\(\)\)?$', 'CURRENT_TIMESTAMP'),
            (r'^\(?sysdatetime\(\)\)?$', 'CURRENT_TIMESTAMP'),
            (r'^\(?getutcdate\(\)\)?$', "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
            (r'^\(?newid\(\)\)?$', 'gen_random_uuid()'),
            (r'^\(?newsequentialid\(\)\)?$', 'gen_random_uuid()'),
        ]
        for pat, rep in rules:
            if re.match(pat, d, re.I):
                return rep
        if d.startswith('(') and d.endswith(')'):
            d = d[1:-1].strip()
        if re.fullmatch(r'-?\d+(\.\d+)?', d):
            return d
        m = re.fullmatch(r"N?'(.*)'", d)
        if m:
            return "'" + m.group(1).replace("'", "''") + "'"
        return None

    def load_dataframe(self, schema, table, columns, df):
        """Load via COPY for speed."""
        qualified = f'{qident_pg(schema)}.{qident_pg(table)}'
        col_list = ", ".join(qident_pg(c) for c in columns)
        buf = df[columns].to_csv(index=False, header=False,
                                 na_rep='\\N')
        import io
        self.cursor.copy_expert(
            f"COPY {qualified} ({col_list}) "
            f"FROM STDIN WITH CSV NULL '\\N'",
            io.StringIO(buf))
        return len(df)

    def set_fk_enforcement(self, enabled):
        try:
            self.cursor.execute(
                f"SET session_replication_role = "
                f"{'origin' if enabled else 'replica'}")
            return True
        except Exception:
            return False

    def add_fk_constraint(self, schema, table, fk_name, cols, ref_schema,
                          ref_table, ref_cols, on_delete, on_update):
        c = ', '.join(qident_pg(x) for x in cols)
        rc = ', '.join(qident_pg(x) for x in ref_cols)
        ddl = (f'ALTER TABLE {qident_pg(schema)}.{qident_pg(table)} '
               f'ADD CONSTRAINT {qident_pg(fk_name)} '
               f'FOREIGN KEY ({c}) '
               f'REFERENCES {qident_pg(ref_schema)}.{qident_pg(ref_table)} ({rc})')
        if on_delete and on_delete.upper() != 'NO ACTION':
            ddl += f' ON DELETE {on_delete.upper().replace("_", " ")}'
        if on_update and on_update.upper() != 'NO ACTION':
            ddl += f' ON UPDATE {on_update.upper().replace("_", " ")}'
        self.cursor.execute(ddl)


class MSSQLTarget:
    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        trust = 'yes' if cfg.get('trust_cert') else 'no'
        self.engine = create_engine(
            f"mssql+pyodbc://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg.get('port', 1433)}/{cfg['database']}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
            f"&TrustServerCertificate={trust}"
        )

    def test(self):
        with self.engine.connect() as c:
            c.execute(text("SELECT 1"))

    def ensure_schema(self, schema):
        with self.engine.begin() as c:
            c.execute(text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :n) "
                f"EXEC('CREATE SCHEMA {qident_mssql(schema)}')"
            ), {"n": schema})

    def table_exists(self, schema, table):
        with self.engine.connect() as c:
            r = c.execute(text("""
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t
            """), {"s": schema, "t": table}).fetchone()
        return r is not None

    def get_columns(self, schema, table):
        with self.engine.connect() as c:
            rows = c.execute(text("""
                SELECT c.name AS column_name, t.name AS data_type,
                       c.max_length AS max_length, c.precision AS precision,
                       c.scale AS scale, c.is_nullable AS is_nullable
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID(:q)
                ORDER BY c.column_id
            """), {"q": f'[{schema}].[{table}]'}).fetchall()
        return {
            r[0]: {'type': r[1].lower(), 'max_length': r[2],
                   'precision': r[3], 'scale': r[4],
                   'nullable': bool(r[5])}
            for r in rows
        }

    def execute(self, sql):
        with self.engine.begin() as c:
            c.execute(text(sql))

    def render_type(self, col):
        """Translate a source PG column dict into a T-SQL type string."""
        dt = col['data_type'].lower()
        udt = col.get('udt_name', '').lower()
        # Prefer udt_name when it's a known base type
        key = udt if udt in PG_TO_MSSQL_TYPES else dt
        if key not in PG_TO_MSSQL_TYPES:
            return 'nvarchar(max)'
        base = PG_TO_MSSQL_TYPES[key]
        if base in MSSQL_TYPES_NO_PARAMS or base.endswith('(max)'):
            return base
        if base in ('nvarchar', 'nchar'):
            ml = col.get('max_length', -1)
            if ml is None or ml == -1:
                return 'nvarchar(max)'
            return f'{base}({ml})'
        if base == 'varbinary':
            ml = col.get('max_length', -1)
            if ml is None or ml == -1:
                return 'varbinary(max)'
            return f'varbinary({ml})'
        if base == 'decimal':
            p = col.get('precision', 18) or 18
            s = col.get('scale', 0) or 0
            return f'decimal({p},{s})'
        return base

    def translate_default(self, sql_default, mssql_type):
        if not sql_default:
            return None
        d = str(sql_default).strip()
        rules = [
            (r'^CURRENT_TIMESTAMP$', 'GETDATE()'),
            (r"^\(?CURRENT_TIMESTAMP AT TIME ZONE 'UTC'\)?$", 'GETUTCDATE()'),
            (r'^now\(\)$', 'GETDATE()'),
            (r'^gen_random_uuid\(\)$', 'NEWID()'),
            (r'^uuid_generate_v4\(\)$', 'NEWID()'),
        ]
        for pat, rep in rules:
            if re.match(pat, d, re.I):
                return rep
        if re.fullmatch(r'-?\d+(\.\d+)?', d):
            return d
        m = re.fullmatch(r"'(.*)'::[\w\s]+", d) or re.fullmatch(r"'(.*)'", d)
        if m:
            return "N'" + m.group(1).replace("'", "''") + "'"
        return None

    def load_dataframe(self, schema, table, columns, df):
        """Load via fast_executemany."""
        col_names = ', '.join(qident_mssql(c) for c in columns)
        placeholders = ', '.join(['?'] * len(columns))
        sql = (f'INSERT INTO {qident_mssql(schema)}.{qident_mssql(table)} '
               f'({col_names}) VALUES ({placeholders})')

        def clean(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            if pd.isna(v):
                return None
            return v

        rows = [tuple(clean(v) for v in row)
                for row in df[columns].itertuples(index=False, name=None)]

        raw = self.engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.fast_executemany = True
            cur.executemany(sql, rows)
            raw.commit()
        finally:
            raw.close()
        return len(rows)

    def set_fk_enforcement(self, enabled):
        # SQL Server doesn't have a session-level toggle like PG.
        # Return True to indicate "no action needed".
        return True

    def add_fk_constraint(self, schema, table, fk_name, cols, ref_schema,
                          ref_table, ref_cols, on_delete, on_update):
        c = ', '.join(qident_mssql(x) for x in cols)
        rc = ', '.join(qident_mssql(x) for x in ref_cols)
        ddl = (f'ALTER TABLE {qident_mssql(schema)}.{qident_mssql(table)} '
               f'ADD CONSTRAINT {qident_mssql(fk_name)} '
               f'FOREIGN KEY ({c}) '
               f'REFERENCES {qident_mssql(ref_schema)}.{qident_mssql(ref_table)} ({rc})')
        # Note: skipping ON DELETE/UPDATE for simplicity — SQL Server syntax differs
        with self.engine.begin() as conn:
            conn.execute(text(ddl))


# ==================================================================
# MIGRATOR
# ==================================================================

class Migrator:
    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.stop_flag = threading.Event()

        mig = config['migration']
        self.normalize = make_normalizer(
            mig.get('convert_snake_case', False),
            mig.get('lowercase_names', True))
        self.schema_map = config['schema_map']
        self.source_schemas = list(self.schema_map.keys())
        self.primary_target = next(iter(self.schema_map.values()))

        # Resolve configs to 'schema.table' keys
        self._resolve_configs()

        # Build adapters
        direction = config['direction']
        if direction == 'mssql_to_pg':
            self.source = MSSQLSource(config['sqlserver'], self.normalize)
            self.target = PGTarget(config['postgres'], self.normalize)
            self.target_uses_copy = True
        elif direction == 'pg_to_mssql':
            self.source = PGSource(config['postgres'], self.normalize)
            self.target = MSSQLTarget(config['sqlserver'], self.normalize)
            self.target_uses_copy = False
        else:
            raise ValueError(f"Unknown direction: {direction}")

    # --------------------------------------------------------------
    def _resolve_table_key(self, name):
        if '.' in name:
            return name
        return f'{self.source_schemas[0]}.{name}'

    def _resolve_configs(self):
        cfg = self.config
        n = self.normalize

        def resolve_merges(d):
            out = {}
            for k, v in d.items():
                out[self._resolve_table_key(k)] = {
                    **v,
                    'merge_into': self._resolve_table_key(v['merge_into']),
                    'join_on': [list(p) for p in v['join_on']],
                }
            return out

        def resolve_redirects(d):
            out = {}
            for k, lst in d.items():
                out[self._resolve_table_key(k)] = [
                    {**r, 'via_table': self._resolve_table_key(r['via_table'])}
                    for r in lst
                ]
            return out

        def resolve_related(d):
            out = {}
            for k, lst in d.items():
                out[self._resolve_table_key(k)] = [
                    {**s, 'from_table': self._resolve_table_key(s['from_table'])}
                    for s in lst
                ]
            return out

        self.table_merges = resolve_merges(cfg.get('table_merges', {}))
        self.tree_inheritance = {
            self._resolve_table_key(k): v
            for k, v in cfg.get('tree_inheritance', {}).items()
        }
        self.fk_redirects = resolve_redirects(cfg.get('fk_redirects', {}))
        self.column_from_related = resolve_related(
            cfg.get('column_from_related', {}))
        self.column_renames = {
            self._resolve_table_key(k): {n(a): n(b) for a, b in v.items()}
            for k, v in cfg.get('column_renames', {}).items()
        }
        self.skip_columns = {
            self._resolve_table_key(k): {n(c) for c in v}
            for k, v in cfg.get('skip_columns', {}).items()
        }

    # --------------------------------------------------------------
    def target_schema_for(self, table_key):
        schema = table_key.split('.', 1)[0]
        return self.schema_map[schema]

    def target_table_for(self, table_key):
        _, t = table_key.split('.', 1)
        return self.normalize(t)

    def get_merge_sources(self, table_key):
        return [k for k, v in self.table_merges.items()
                if v['merge_into'] == table_key]

    # --------------------------------------------------------------
    def transform_dataframe(self, table_key, df):
        """Apply all column-level transformations."""
        # 1. FK redirects
        if table_key in self.fk_redirects:
            df = self._apply_fk_redirects(df, self.fk_redirects[table_key])

        # 2. Merges
        for src in self.get_merge_sources(table_key):
            cfg = self.table_merges[src]
            self.log(f"    + merging {src} into {table_key}")
            df_extra = self.source.read_table(src)
            df = self._apply_merge(df, df_extra, cfg, src)

        # 3. Tree inheritance
        if table_key in self.tree_inheritance:
            df = self._apply_tree_inheritance(
                df, self.tree_inheritance[table_key])

        # 4. Column from related
        if table_key in self.column_from_related:
            df = self._apply_column_from_related(
                df, self.column_from_related[table_key], table_key)

        # 5. Column renames
        if table_key in self.column_renames:
            df = df.rename(columns=self.column_renames[table_key])

        return df

    def _apply_fk_redirects(self, df, redirects):
        for r in redirects:
            from_col = self.normalize(r['from_column'])
            via_key = self.normalize(r['via_key'])
            value_col = self.normalize(r['value_column'])
            to_col = self.normalize(r['to_column'])
            if from_col not in df.columns:
                self.log(f"    ! redirect: '{from_col}' not in source")
                continue
            lookup = self.source.read_table(r['via_table'])
            if via_key not in lookup.columns or value_col not in lookup.columns:
                self.log(f"    ! redirect: {r['via_table']} missing columns")
                continue
            mapping = (lookup.drop_duplicates(subset=[via_key])
                             .set_index(via_key)[value_col])
            df[to_col] = df[from_col].map(mapping)
            df = df.drop(columns=[from_col])
            self.log(f"    + FK redirect: {from_col} -> {to_col}")
        return df

    def _apply_merge(self, df_target, df_extra, cfg, merge_src):
        pairs = cfg['join_on']
        child_keys = [self.normalize(c) for c, _ in pairs]
        parent_keys = [self.normalize(p) for _, p in pairs]

        for c in child_keys:
            if c not in df_extra.columns:
                raise RuntimeError(f"merge {merge_src}: missing '{c}'")
        for p in parent_keys:
            if p not in df_target.columns:
                raise RuntimeError(f"merge {merge_src}: missing '{p}'")

        if df_extra.duplicated(subset=child_keys).any():
            if cfg.get('on_multi') == 'error':
                raise RuntimeError(f"merge {merge_src}: multiple rows per key")
            elif cfg.get('on_multi') == 'take_first':
                df_extra = df_extra.drop_duplicates(subset=child_keys, keep='first')

        conflicts = (set(df_target.columns) & set(df_extra.columns)) - set(child_keys)
        if conflicts:
            self.log(f"    ! merge: duplicate columns "
                     f"{sorted(conflicts)} kept from target")
            df_extra = df_extra.drop(columns=list(conflicts))

        if set(child_keys) == set(parent_keys):
            merged = df_target.merge(df_extra, on=parent_keys, how='left')
        else:
            merged = df_target.merge(df_extra, left_on=parent_keys,
                                     right_on=child_keys, how='left')
            drop_after = [c for c in child_keys
                          if c in merged.columns and c not in df_target.columns]
            if drop_after:
                merged = merged.drop(columns=drop_after)
        return merged

    def _apply_tree_inheritance(self, df, cfg):
        id_col = self.normalize(cfg['id_col'])
        parent_col = self.normalize(cfg['parent_col'])
        cols = [self.normalize(c) for c in cfg['columns']]

        if id_col not in df.columns or parent_col not in df.columns:
            self.log(f"    ! tree inheritance: missing {id_col}/{parent_col}")
            return df

        id_to_parent = {}
        for _id, _pid in zip(df[id_col], df[parent_col]):
            if pd.notna(_id):
                id_to_parent[_id] = _pid if pd.notna(_pid) else None

        for col in cols:
            if col not in df.columns:
                continue
            col_values = dict(zip(df[id_col], df[col]))
            memo = {}

            def find_root(node, seen=None):
                if pd.isna(node):
                    return None
                if node in memo:
                    return memo[node]
                if seen is None:
                    seen = set()
                if node in seen:
                    memo[node] = None
                    return None
                seen.add(node)
                v = col_values.get(node)
                if pd.notna(v):
                    memo[node] = v
                    return v
                p = id_to_parent.get(node)
                r = find_root(p, seen) if p is not None else None
                memo[node] = r
                return r

            df[col] = df[id_col].map(find_root)
            nn = df[col].notna().sum()
            self.log(f"    + tree: '{col}' populated for {nn}/{len(df)} rows")
        return df

    def _apply_column_from_related(self, df, specs, table_key):
        for spec in specs:
            from_table = spec['from_table']
            from_schema = self.target_schema_for(from_table)
            from_name = self.target_table_for(from_table)
            from_col = self.normalize(spec['from_column'])
            via_key = self.normalize(spec['via_key'])
            via_fk = self.normalize(spec['via_fk'])
            to_col = self.normalize(spec['to_column'])

            if via_fk not in df.columns:
                self.log(f"    ! col-from-related: '{via_fk}' missing")
                continue

            try:
                self.target.cursor.execute(
                    f'SELECT {qident_pg(via_key)}, {qident_pg(from_col)} '
                    f'FROM {qident_pg(from_schema)}.{qident_pg(from_name)}')
                mapping = dict(self.target.cursor.fetchall())
            except Exception as e:
                self.log(f"    ! col-from-related: {e}")
                continue

            df[to_col] = df[via_fk].map(mapping)
            nn = df[to_col].notna().sum()
            self.log(f"    + col-from-related: '{to_col}' "
                     f"({nn}/{len(df)} rows)")
        return df

    def coerce_values(self, df, target_cols, table_key):
        """Coerce values for the target's column types."""
        for col in list(df.columns):
            if col not in target_cols:
                continue
            t = target_cols[col]
            # Normalize target type to a simple string
            if isinstance(t, dict):
                tt = t.get('type', '').lower()
                udt = t.get('udt', '').lower()
            else:
                tt = ''
                udt = ''

            s = df[col]
            # Ints
            if tt in ('smallint', 'integer', 'bigint') or udt in ('int2', 'int4', 'int8'):
                if pd.api.types.is_float_dtype(s):
                    nn = s.dropna()
                    if len(nn) == 0 or (nn % 1 == 0).all():
                        df[col] = s.astype('Int64')
            # Bools
            elif tt in ('boolean',) or udt in ('bool',):
                if not pd.api.types.is_bool_dtype(s):
                    df[col] = s.map(self._to_bool)
            # UUIDs
            elif tt == 'uuid':
                df[col] = s.map(lambda v: str(v).lower()
                                if v is not None and not (isinstance(v, float) and pd.isna(v))
                                else None)
        return df

    @staticmethod
    def _to_bool(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, bool):
            return v
        try:
            return int(v) != 0
        except (ValueError, TypeError):
            pass
        s = str(v).strip().lower()
        if s in ('true', 't', 'yes', 'y', 'on', '1'):
            return True
        if s in ('false', 'f', 'no', 'n', 'off', '0'):
            return False
        return None

    # --------------------------------------------------------------
    def build_create_table(self, table_key, col_defs, constraints):
        """Return the CREATE TABLE statement for the target."""
        target_schema = self.target_schema_for(table_key)
        target_name = self.target_table_for(table_key)

        is_pg = self.config['direction'] == 'mssql_to_pg'
        q = qident_pg if is_pg else qident_mssql

        parts = []
        existing_names = set()
        for cd in col_defs:
            parts.append(cd)
            # cd looks like '    "col" TYPE NOT NULL' or '    [col] TYPE NOT NULL'
            # We derive name from the first quoted/bracketed token
            m = re.match(r'\s*(?:"([^"]+)"|\[([^\]]+)\])', cd)
            if m:
                existing_names.add(m.group(1) or m.group(2))

        # PK
        pk = constraints['primary_key']
        if pk:
            cols = [self.normalize(c) for c in pk['columns']]
            if all(c in existing_names for c in cols):
                col_list = ', '.join(q(c) for c in cols)
                parts.append(f'    CONSTRAINT {q(self.normalize(pk["name"]))} '
                             f'PRIMARY KEY ({col_list})')

        # Uniques
        for u in constraints['uniques']:
            cols = [self.normalize(c) for c in u['columns']]
            if all(c in existing_names for c in cols):
                col_list = ', '.join(q(c) for c in cols)
                parts.append(f'    CONSTRAINT {q(self.normalize(u["name"]))} '
                             f'UNIQUE ({col_list})')

        if is_pg:
            ddl = (f'CREATE TABLE IF NOT EXISTS {q(target_schema)}.{q(target_name)} (\n'
                   + ',\n'.join(parts) + '\n);')
        else:
            # SQL Server: check existence first
            ddl = (f"IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                   f"WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{target_name}') "
                   f"BEGIN CREATE TABLE {q(target_schema)}.{q(target_name)} (\n"
                   + ',\n'.join(parts) + '\n) END;')
        return ddl

    def generate_column_definitions(self, table_key, source_cols, extra_cols):
        """Render the column list (name + type + nullability + default)."""
        is_pg = self.config['direction'] == 'mssql_to_pg'
        q = qident_pg if is_pg else qident_mssql

        all_cols = list(source_cols)
        seen = {self.normalize(c['column_name']) for c in all_cols}
        for c in extra_cols:
            n = self.normalize(c['column_name'])
            if n not in seen:
                all_cols.append(c)
                seen.add(n)

        definitions = []
        for c in all_cols:
            cname = self.normalize(c['column_name'])
            if c.get('is_computed'):
                self.log(f"    ! computed column '{cname}' skipped")
                continue
            try:
                type_str = self.target.render_type(c)
            except Exception as e:
                self.log(f"    ! type for '{cname}': {e}")
                continue

            parts = [q(cname), type_str]
            # Identity
            if c.get('is_identity'):
                if is_pg:
                    parts.append('GENERATED BY DEFAULT AS IDENTITY')
                else:
                    parts.append('IDENTITY(1,1)')

            if not c.get('is_nullable', True):
                parts.append('NOT NULL')

            if c.get('default_definition'):
                d = self.target.translate_default(
                    c['default_definition'], type_str)
                if d:
                    parts.append(f'DEFAULT {d}')

            definitions.append('    ' + ' '.join(parts))

        return definitions

    # --------------------------------------------------------------
    def collect_extra_columns(self, table_key):
        """Gather columns from redirects, merges, and column_from_related."""
        extras = []
        # From redirects
        for r in self.fk_redirects.get(table_key, []):
            info = self.find_source_column(r['via_table'], r['value_column'])
            if info:
                c = dict(info)
                c['column_name'] = r['to_column']
                c['is_nullable'] = True
                c['is_identity'] = False
                c['is_computed'] = False
                c['default_definition'] = None
                extras.append(c)
        # From merges
        for src in self.get_merge_sources(table_key):
            protected = {self.normalize(a) for a, _ in self.table_merges[src]['join_on']}
            for c in self.source.get_columns(src):
                n = self.normalize(c['column_name'])
                if n in protected:
                    continue
                c = dict(c)
                c['is_nullable'] = True
                c['is_identity'] = False
                c['is_computed'] = False
                extras.append(c)
        # From column_from_related
        for spec in self.column_from_related.get(table_key, []):
            info = self.find_source_column(spec['from_table'], spec['from_column'])
            if info:
                c = dict(info)
                c['column_name'] = spec['to_column']
                c['is_nullable'] = True
                c['is_identity'] = False
                c['is_computed'] = False
                c['default_definition'] = None
                extras.append(c)
        return extras

    def find_source_column(self, table_key, col_name):
        want = self.normalize(col_name)
        for c in self.source.get_columns(table_key):
            if self.normalize(c['column_name']) == want:
                return c
        for src in self.get_merge_sources(table_key):
            for c in self.source.get_columns(src):
                if self.normalize(c['column_name']) == want:
                    return c
        return None

    # --------------------------------------------------------------
    def ensure_schema_and_tables(self, all_tables):
        merged_away = set(self.table_merges.keys())
        seen_targets = {}

        # Create all target schemas
        for schema in set(self.schema_map.values()):
            self.target.ensure_schema(schema)
        self.log(f"Target schemas ensured: "
                 f"{sorted(set(self.schema_map.values()))}")

        all_constraints = {}

        for t in all_tables:
            if t in merged_away:
                self.log(f"  · {t}: merged into {self.table_merges[t]['merge_into']}")
                continue

            ts = self.target_schema_for(t)
            tn = self.target_table_for(t)
            key = (ts, tn)

            if key in seen_targets:
                self.log(f"  ✗ {t}: target name '{ts}.{tn}' already used by "
                         f"{seen_targets[key]}")
                continue
            seen_targets[key] = t

            constraints = self.source.get_constraints(t)
            all_constraints[t] = constraints

            if self.target.table_exists(ts, tn):
                self.log(f"  · {t}: {ts}.{tn} exists, skipping CREATE")
                continue

            source_cols = self.source.get_columns(t)
            extra_cols, drop_cols = self.collect_extra_columns(t)
            col_defs = self.generate_column_definitions(source_cols, extra_cols, drop_cols)
            ddl = self.build_create_table(t, col_defs, constraints)

            try:
                self.target.execute(ddl)
                self.log(f"  + {t}: created as {ts}.{tn}")
            except Exception as e:
                self.log(f"  ✗ {t}: CREATE failed: {str(e).splitlines()[0]}")

        # Add FKs
        self.log("Adding foreign keys...")
        for t, constraints in all_constraints.items():
            ts = self.target_schema_for(t)
            tn = self.target_table_for(t)
            for fk in constraints['foreign_keys']:
                ref = fk['ref_key']
                if ref in merged_away or ref not in all_constraints:
                    continue
                ref_schema = ref.split('.', 1)[0]
                if ref_schema not in self.schema_map:
                    continue
                ref_ts = self.target_schema_for(ref)
                ref_tn = self.target_table_for(ref)
                fk_name = self.normalize(fk['name'])
                cols = [self.normalize(c) for c in fk['columns']]
                ref_cols = [self.normalize(c) for c in fk['ref_columns']]
                try:
                    self.target.add_fk_constraint(
                        ts, tn, fk_name, cols, ref_ts, ref_tn, ref_cols,
                        fk.get('on_delete'), fk.get('on_update'))
                    self.log(f"  + {t}.{fk_name} → {ref}")
                except Exception as e:
                    self.log(f"  ✗ {t}.{fk_name}: {str(e).splitlines()[0]}")

        # Post-create DDL
        for i, stmt in enumerate(self.config.get('post_create_ddl', []), 1):
            sql = stmt.replace('{schema}', qident_pg(self.primary_target))
            try:
                self.target.execute(sql)
                self.log(f"  + [{i}] post-create OK")
            except Exception as e:
                self.log(f"  ✗ [{i}] post-create: {str(e).splitlines()[0]}")

    def auto_load_order(self, source_tables):
        """Topological sort using the target's FK constraints."""
        name_map = {}
        for t in source_tables:
            name_map[(self.target_schema_for(t),
                      self.target_table_for(t))] = t
        target_keys = set(name_map.keys())

        # Query FK dependencies from the target
        if self.config['direction'] == 'mssql_to_pg':
            fk_rows = []
            for schema in set(self.schema_map.values()):
                self.target.cursor.execute("""
                    SELECT DISTINCT cn.nspname, child.relname,
                           pn.nspname, parent.relname
                    FROM pg_constraint c
                    JOIN pg_class child  ON child.oid  = c.conrelid
                    JOIN pg_namespace cn ON cn.oid = child.relnamespace
                    JOIN pg_class parent ON parent.oid = c.confrelid
                    JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                    WHERE c.contype = 'f' AND cn.nspname = %s
                """, (schema,))
                fk_rows.extend(self.target.cursor.fetchall())
        else:
            # MSSQL target — query INFORMATION_SCHEMA
            fk_rows = []
            with self.target.engine.connect() as c:
                for schema in set(self.schema_map.values()):
                    rows = c.execute(text("""
                        SELECT DISTINCT
                            cs.TABLE_SCHEMA, cs.TABLE_NAME,
                            ps.TABLE_SCHEMA, ps.TABLE_NAME
                        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cs
                          ON cs.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ps
                          ON ps.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                        WHERE cs.TABLE_SCHEMA = :s
                    """), {"s": schema}).fetchall()
                    fk_rows.extend(rows)

        parents_of = defaultdict(set)
        for cs, ct, ps, pt in fk_rows:
            child = (cs.lower(), ct.lower())
            parent = (ps.lower(), pt.lower())
            if child == parent:
                continue
            if child in target_keys and parent in target_keys:
                parents_of[child].add(parent)

        in_degree = {t: len(parents_of.get(t, ())) for t in target_keys}
        children_of = defaultdict(set)
        for child, parents in parents_of.items():
            for p in parents:
                children_of[p].add(child)

        ready = deque(sorted(t for t in target_keys if in_degree[t] == 0))
        ordered = []
        while ready:
            t = ready.popleft()
            ordered.append(t)
            for c in sorted(children_of[t]):
                in_degree[c] -= 1
                if in_degree[c] == 0:
                    ready.append(c)

        remaining = sorted(target_keys - set(ordered))
        if remaining:
            self.log(f"    ! circular dependency: {remaining}")
            ordered.extend(remaining)

        return [name_map[t] for t in ordered]

    # --------------------------------------------------------------
    def run(self):
        cfg = self.config
        mig = cfg['migration']
        out_dir = mig.get('output_dir', 'csv_export')
        os.makedirs(out_dir, exist_ok=True)

        self.log("Testing connections...")
        self.source.test()
        self.target.test()
        self.log("  ✓ both connections OK\n")

        all_tables = self.source.list_tables(self.source_schemas)
        def table_part(k): return k.split('.', 1)[1]
        skip = set(mig.get('skip_tables', []))
        all_tables = [t for t in all_tables if table_part(t) not in skip]

        merged_away = set(self.table_merges.keys())
        tables = [t for t in all_tables if t not in merged_away]

        if mig.get('create_missing_tables', True):
            self.log("Ensuring target schema and tables...")
            self.ensure_schema_and_tables(
                [t for t in all_tables if table_part(t) not in skip])
            self.log("")

        self.log("Auto-detecting load order...")
        tables = self.auto_load_order(tables)
        self.log(f"\nLoad order:")
        for t in tables:
            self.log(f"  {t} → {self.target_schema_for(t)}.{self.target_table_for(t)}")
        self.log("")

        # Disable FKs
        if mig.get('disable_fk_during_load', True):
            ok = self.target.set_fk_enforcement(False)
            if ok:
                self.log("FK enforcement disabled.\n")

        succeeded, failed = [], []
        for t in tables:
            if self.stop_flag.is_set():
                self.log("! Stop requested — aborting.")
                break
            self.log(f"--- {t} ---")
            try:
                df = self.source.read_table(t)
                df = self.transform_dataframe(t, df)

                ts = self.target_schema_for(t)
                tn = self.target_table_for(t)
                target_cols = self.target.get_columns(ts, tn)
                if not target_cols:
                    raise RuntimeError(f"target {ts}.{tn} not found")

                # Filter to target columns
                skips = self.skip_columns.get(t, set())
                keep, dropped = [], []
                for c in df.columns:
                    if c in skips or c not in target_cols:
                        dropped.append(c)
                    else:
                        keep.append(c)
                if dropped:
                    self.log(f"    ! dropping columns: {dropped}")
                df = df[keep]

                if not list(df.columns):
                    raise RuntimeError("no matching columns")

                df = self.coerce_values(df, target_cols, t)

                # Save CSV for inspection
                csv_name = t.replace('.', '__') + '.csv'
                csv_path = os.path.join(out_dir, csv_name)
                df.to_csv(csv_path, index=False, encoding='utf-8',
                          na_rep='\\N')

                n = self.target.load_dataframe(ts, tn, list(df.columns), df)
                self.log(f"  ✓ loaded {n} rows\n")
                succeeded.append((t, n))
            except Exception as e:
                msg = str(e).strip().replace('\n', ' | ')
                self.log(f"  ✗ FAILED: {msg}\n")
                failed.append((t, msg))

        # Post-load DDL
        for i, stmt in enumerate(cfg.get('post_load_ddl', []), 1):
            if self.stop_flag.is_set():
                break
            sql = stmt.replace('{schema}', qident_pg(self.primary_target))
            try:
                self.target.execute(sql)
                self.log(f"  + [{i}] post-load OK")
            except Exception as e:
                self.log(f"  ✗ [{i}] post-load: {str(e).splitlines()[0]}")

        if mig.get('disable_fk_during_load', True):
            self.target.set_fk_enforcement(True)
            self.log("\nFK enforcement restored.")

        self.log("=" * 60)
        self.log(f"Done. {len(succeeded)} succeeded, {len(failed)} failed.")
        for t, n in succeeded:
            self.log(f"  ✓ {t} ({n} rows)")
        for t, err in failed:
            self.log(f"  ✗ {t}: {err[:200]}")


# ==================================================================
# UI
# ==================================================================

class ETLApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL Studio — SQL Server ↔ PostgreSQL")
        self.root.geometry("1000x750")

        self.config = json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy
        self.log_queue = queue.Queue()
        self.worker = None
        self.migrator = None

        self._build_menu()
        self._build_ui()
        self._poll_log()

    # --------------------------------------------------------------
    def _build_menu(self):
        menubar = tk.Menu(self.root)

        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Load Config...", command=self.load_config)
        file_menu.add_command(label="Save Config...", command=self.save_config)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        menubar.add_cascade(label="Help", menu=help_menu)

        self.root.config(menu=menubar)

    def _build_ui(self):
        nb = ttk.Notebook(self.root)
        nb.pack(fill='both', expand=True, padx=8, pady=8)

        self._build_connection_tab(nb)
        self._build_migration_tab(nb)
        self._build_advanced_tab(nb)
        self._build_log_tab(nb)

        bottom = ttk.Frame(self.root)
        bottom.pack(fill='x', padx=8, pady=(0, 8))

        self.btn_test = ttk.Button(bottom, text="Test Connections",
                                   command=self.test_connections)
        self.btn_test.pack(side='left', padx=2)

        self.btn_run = ttk.Button(bottom, text="Run Migration",
                                  command=self.run_migration)
        self.btn_run.pack(side='left', padx=2)

        self.btn_stop = ttk.Button(bottom, text="Stop",
                                   command=self.stop_migration,
                                   state='disabled')
        self.btn_stop.pack(side='left', padx=2)

        self.status = tk.StringVar(value="Idle")
        ttk.Label(bottom, textvariable=self.status).pack(side='right')

    # --------------------------------------------------------------
    def _build_connection_tab(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Connections")

        # Direction
        dir_frame = ttk.LabelFrame(frame, text="Direction")
        dir_frame.pack(fill='x', padx=8, pady=8)
        self.dir_var = tk.StringVar(value=self.config['direction'])
        ttk.Radiobutton(dir_frame, text="SQL Server → PostgreSQL",
                        variable=self.dir_var, value='mssql_to_pg',
                        command=self._on_direction_change).pack(anchor='w', padx=8, pady=2)
        ttk.Radiobutton(dir_frame, text="PostgreSQL → SQL Server",
                        variable=self.dir_var, value='pg_to_mssql',
                        command=self._on_direction_change).pack(anchor='w', padx=8, pady=2)

        # SQL Server
        ss = ttk.LabelFrame(frame, text="SQL Server")
        ss.pack(fill='x', padx=8, pady=8)
        self.ss_vars = {}
        for i, (k, label, default) in enumerate([
            ('host', 'Host', 'localhost'),
            ('port', 'Port', '1433'),
            ('user', 'User', 'sa'),
            ('password', 'Password', ''),
            ('database', 'Database', 'MyDB'),
        ]):
            ttk.Label(ss, text=label).grid(row=i, column=0, sticky='e',
                                           padx=5, pady=2)
            v = tk.StringVar(value=str(self.config['sqlserver'].get(k, default)))
            ttk.Entry(ss, textvariable=v, width=40,
                      show='*' if k == 'password' else '').grid(
                row=i, column=1, sticky='w', padx=5, pady=2)
            self.ss_vars[k] = v
        self.ss_trust = tk.BooleanVar(value=self.config['sqlserver']['trust_cert'])
        ttk.Checkbutton(ss, text="Trust server certificate",
                        variable=self.ss_trust).grid(row=5, column=1,
                                                     sticky='w', padx=5, pady=2)

        # PostgreSQL
        pg = ttk.LabelFrame(frame, text="PostgreSQL")
        pg.pack(fill='x', padx=8, pady=8)
        self.pg_vars = {}
        for i, (k, label, default) in enumerate([
            ('host', 'Host', 'localhost'),
            ('port', 'Port', '5432'),
            ('user', 'User', 'postgres'),
            ('password', 'Password', ''),
            ('database', 'Database', 'MyPgDB'),
        ]):
            ttk.Label(pg, text=label).grid(row=i, column=0, sticky='e',
                                           padx=5, pady=2)
            v = tk.StringVar(value=str(self.config['postgres'].get(k, default)))
            ttk.Entry(pg, textvariable=v, width=40,
                      show='*' if k == 'password' else '').grid(
                row=i, column=1, sticky='w', padx=5, pady=2)
            self.pg_vars[k] = v

        # Schema map
        sm = ttk.LabelFrame(frame, text="Schema Map (source → target, one per line)")
        sm.pack(fill='both', expand=True, padx=8, pady=8)
        self.schema_text = tk.Text(sm, height=6)
        self.schema_text.pack(fill='both', expand=True, padx=5, pady=5)
        self._load_schema_map()

    def _load_schema_map(self):
        self.schema_text.delete('1.0', 'end')
        for src, tgt in self.config['schema_map'].items():
            self.schema_text.insert('end', f'{src} = {tgt}\n')

    def _save_schema_map(self):
        txt = self.schema_text.get('1.0', 'end').strip()
        mapping = {}
        for line in txt.splitlines():
            line = line.strip()
            if not line or '=' not in line:
                continue
            k, v = line.split('=', 1)
            mapping[k.strip()] = v.strip()
        self.config['schema_map'] = mapping

    def _on_direction_change(self):
        self.config['direction'] = self.dir_var.get()

    # --------------------------------------------------------------
    def _build_migration_tab(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Migration")

        opts = ttk.LabelFrame(frame, text="Options")
        opts.pack(fill='x', padx=8, pady=8)

        self.snake_var = tk.BooleanVar(
            value=self.config['migration']['convert_snake_case'])
        ttk.Checkbutton(opts, text="Convert names to snake_case",
                        variable=self.snake_var).pack(anchor='w', padx=8, pady=2)

        self.lower_var = tk.BooleanVar(
            value=self.config['migration']['lowercase_names'])
        ttk.Checkbutton(opts, text="Lowercase names (if snake_case off)",
                        variable=self.lower_var).pack(anchor='w', padx=8, pady=2)

        self.create_var = tk.BooleanVar(
            value=self.config['migration']['create_missing_tables'])
        ttk.Checkbutton(opts, text="Create missing tables in target",
                        variable=self.create_var).pack(anchor='w', padx=8, pady=2)

        self.fkoff_var = tk.BooleanVar(
            value=self.config['migration']['disable_fk_during_load'])
        ttk.Checkbutton(opts, text="Disable FK enforcement during load",
                        variable=self.fkoff_var).pack(anchor='w', padx=8, pady=2)

        # Output dir
        od = ttk.LabelFrame(frame, text="CSV Output Directory")
        od.pack(fill='x', padx=8, pady=8)
        self.outdir_var = tk.StringVar(
            value=self.config['migration']['output_dir'])
        row = ttk.Frame(od)
        row.pack(fill='x', padx=5, pady=5)
        ttk.Entry(row, textvariable=self.outdir_var, width=60).pack(
            side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(row, text="Browse...", command=self._pick_outdir).pack(side='left')

        # Skip tables
        st = ttk.LabelFrame(frame, text="Skip Tables (one per line, bare name)")
        st.pack(fill='both', expand=True, padx=8, pady=8)
        self.skip_text = tk.Text(st, height=6)
        self.skip_text.pack(fill='both', expand=True, padx=5, pady=5)
        for t in self.config['migration'].get('skip_tables', []):
            self.skip_text.insert('end', f'{t}\n')

    def _pick_outdir(self):
        d = filedialog.askdirectory()
        if d:
            self.outdir_var.set(d)

    # --------------------------------------------------------------
    def _build_advanced_tab(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Advanced")

        canvas = tk.Canvas(frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=canvas.yview)
        inner = ttk.Frame(canvas)

        canvas.create_window((0, 0), window=inner, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        def on_configure(event):
            canvas.configure(scrollregion=canvas.bbox('all'))
        inner.bind('<Configure>', on_configure)

        sections = [
            ('table_merges', 'TABLE_MERGES (JSON)',
             'Map child tables to merge into parents'),
            ('tree_inheritance', 'TREE_INHERITANCE (JSON)',
             'Propagate tree-root values to descendants'),
            ('fk_redirects', 'FK_REDIRECTS (JSON)',
             'Replace a column value via lookup in another table'),
            ('column_from_related', 'COLUMN_FROM_RELATED (JSON)',
             'Populate a column from an already-loaded target table'),
            ('column_renames', 'COLUMN_RENAMES (JSON)',
             'Rename columns during migration'),
            ('skip_columns', 'SKIP_COLUMNS (JSON)',
             'Drop columns that do not exist in target'),
            ('post_create_ddl', 'POST_CREATE_DDL (list of SQL)',
             'Run after tables are created, before data load'),
            ('post_load_ddl', 'POST_LOAD_DDL (list of SQL)',
             'Run after data load, before FK enforcement restored'),
        ]

        self.adv_widgets = {}
        for key, title, tooltip in sections:
            lf = ttk.LabelFrame(inner, text=title)
            lf.pack(fill='x', padx=8, pady=6)
            ttk.Label(lf, text=tooltip, foreground='gray').pack(
                anchor='w', padx=5)
            txt = tk.Text(lf, height=6, wrap='none')
            txt.pack(fill='x', padx=5, pady=5)
            txt.insert('1.0', json.dumps(self.config.get(key, {}), indent=2))
            self.adv_widgets[key] = txt

    # --------------------------------------------------------------
    def _build_log_tab(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Log")

        self.log_text = scrolledtext.ScrolledText(
            frame, wrap='word', font=('TkFixedFont', 9))
        self.log_text.pack(fill='both', expand=True, padx=5, pady=5)

    # --------------------------------------------------------------
    def _poll_log(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.insert('end', msg + '\n')
                self.log_text.see('end')
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log)

    def log(self, msg):
        self.log_queue.put(msg)

    def set_status(self, msg):
        self.status.set(msg)

    # --------------------------------------------------------------
    def _collect_config(self):
        """Read UI into config."""
        c = self.config

        c['direction'] = self.dir_var.get()

        for k, v in self.ss_vars.items():
            val = v.get()
            if k == 'port':
                val = int(val) if val else 1433
            c['sqlserver'][k] = val
        c['sqlserver']['trust_cert'] = self.ss_trust.get()

        for k, v in self.pg_vars.items():
            val = v.get()
            if k == 'port':
                val = int(val) if val else 5432
            c['postgres'][k] = val

        c['migration']['convert_snake_case'] = self.snake_var.get()
        c['migration']['lowercase_names'] = self.lower_var.get()
        c['migration']['create_missing_tables'] = self.create_var.get()
        c['migration']['disable_fk_during_load'] = self.fkoff_var.get()
        c['migration']['output_dir'] = self.outdir_var.get()

        skip_txt = self.skip_text.get('1.0', 'end').strip()
        c['migration']['skip_tables'] = [
            s.strip() for s in skip_txt.splitlines() if s.strip()
        ]

        self._save_schema_map()

        for key, widget in self.adv_widgets.items():
            raw = widget.get('1.0', 'end').strip()
            if not raw:
                c[key] = {}
                continue
            try:
                c[key] = json.loads(raw)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in '{key}': {e}")

        return c

    # --------------------------------------------------------------
    def test_connections(self):
        try:
            cfg = self._collect_config()
        except ValueError as e:
            messagebox.showerror("Config error", str(e))
            return

        def work():
            try:
                self.log("Testing connections...")
                if cfg['direction'] == 'mssql_to_pg':
                    MSSQLSource(cfg['sqlserver'],
                                make_normalizer(False, True)).test()
                    PGTarget(cfg['postgres'],
                             make_normalizer(False, True)).test()
                else:
                    PGSource(cfg['postgres'],
                             make_normalizer(False, True)).test()
                    MSSQLTarget(cfg['sqlserver'],
                                make_normalizer(False, True)).test()
                self.log("  ✓ both OK")
                self.log_queue.put("__STATUS__:Connections OK")
            except Exception as e:
                self.log(f"  ✗ {e}")
                self.log_queue.put(f"__STATUS__:Connection failed")

        threading.Thread(target=work, daemon=True).start()

    # --------------------------------------------------------------
    def run_migration(self):
        try:
            cfg = self._collect_config()
        except ValueError as e:
            messagebox.showerror("Config error", str(e))
            return

        self.btn_run.config(state='disabled')
        self.btn_test.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.set_status("Running...")
        self.log_text.delete('1.0', 'end')

        def work():
            try:
                self.migrator = Migrator(cfg, self.log)
                self.migrator.run()
            except Exception:
                tb = traceback.format_exc()
                self.log(tb)
            finally:
                self.log_queue.put("__STATUS__:Idle")

        self.worker = threading.Thread(target=work, daemon=True)
        self.worker.start()

        self._watch_worker()

    def _watch_worker(self):
        if self.worker and self.worker.is_alive():
            self.root.after(300, self._watch_worker)
            return
        # Finished
        self.btn_run.config(state='normal')
        self.btn_test.config(state='normal')
        self.btn_stop.config(state='disabled')
        # Drain any __STATUS__ messages
        try:
            while True:
                msg = self.log_queue.get_nowait()
                if isinstance(msg, str) and msg.startswith('__STATUS__:'):
                    self.set_status(msg.split(':', 1)[1])
                else:
                    self.log_text.insert('end', msg + '\n')
        except queue.Empty:
            pass
        self.set_status("Idle")

    def stop_migration(self):
        if self.migrator:
            self.migrator.stop_flag.set()
            self.log("! Stop requested...")

    # --------------------------------------------------------------
    def load_config(self):
        path = filedialog.askopenfilename(
            filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if not path:
            return
        try:
            with open(path) as f:
                cfg = json.load(f)
            # Merge onto defaults so missing keys are filled
            merged = json.loads(json.dumps(DEFAULT_CONFIG))
            for k, v in cfg.items():
                if isinstance(v, dict) and isinstance(merged.get(k), dict):
                    merged[k].update(v)
                else:
                    merged[k] = v
            self.config = merged
            self._reload_ui_from_config()
            self.log(f"Loaded config: {path}")
        except Exception as e:
            messagebox.showerror("Load failed", str(e))

    def save_config(self):
        path = filedialog.asksaveasfilename(
            defaultextension='.json',
            filetypes=[("JSON", "*.json")])
        if not path:
            return
        try:
            cfg = self._collect_config()
            with open(path, 'w') as f:
                json.dump(cfg, f, indent=2)
            self.log(f"Saved config: {path}")
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

    def _reload_ui_from_config(self):
        c = self.config
        self.dir_var.set(c['direction'])
        for k, v in self.ss_vars.items():
            v.set(str(c['sqlserver'].get(k, '')))
        self.ss_trust.set(c['sqlserver'].get('trust_cert', True))
        for k, v in self.pg_vars.items():
            v.set(str(c['postgres'].get(k, '')))
        self.snake_var.set(c['migration']['convert_snake_case'])
        self.lower_var.set(c['migration']['lowercase_names'])
        self.create_var.set(c['migration']['create_missing_tables'])
        self.fkoff_var.set(c['migration']['disable_fk_during_load'])
        self.outdir_var.set(c['migration']['output_dir'])
        self.skip_text.delete('1.0', 'end')
        for t in c['migration'].get('skip_tables', []):
            self.skip_text.insert('end', f'{t}\n')
        self._load_schema_map()
        for key, widget in self.adv_widgets.items():
            widget.delete('1.0', 'end')
            widget.insert('1.0', json.dumps(c.get(key, {}), indent=2))

    def show_about(self):
        messagebox.showinfo(
            "About ETL Studio",
            "ETL Studio\n\nBidirectional SQL Server ↔ PostgreSQL migration "
            "with column-level transforms.\n\n"
            "Configure, test, and run migrations from a native UI.")


# ==================================================================
# ENTRY
# ==================================================================

def main():
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()