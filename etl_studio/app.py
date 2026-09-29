#!/usr/bin/env python3
"""
ETL Studio — bidirectional SQL Server <-> PostgreSQL migration with a
modern cross-platform PySide6 UI.

Run:  python -m etl_studio
"""

import os
import re
import sys
import json
import platform
import threading
import traceback
from collections import defaultdict, deque
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import psycopg2

from PySide6.QtCore import (
    Qt, QObject, QThread, Signal, Slot, QTimer, QSettings
)
from PySide6.QtGui import (
    QAction, QActionGroup, QFont, QFontDatabase, QTextCursor
)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QFormLayout, QGridLayout, QLabel, QLineEdit, QCheckBox,
    QPushButton, QTabWidget, QPlainTextEdit, QFileDialog,
    QMessageBox, QGroupBox, QScrollArea, QFrame, QStatusBar,
    QProgressBar, QSpinBox, QRadioButton, QButtonGroup
)

# ==================================================================
# DEFAULTS
# ==================================================================

DEFAULT_CONFIG = {
    "direction": "mssql_to_pg",
    "sqlserver": {
        "host": "localhost", "port": 1433, "user": "sa",
        "password": "", "database": "MyDB", "trust_cert": True,
    },
    "postgres": {
        "host": "localhost", "port": 5432, "user": "postgres",
        "password": "", "database": "MyPgDB",
    },
    "migration": {
        "convert_snake_case": True,
        "lowercase_names": True,
        "create_missing_tables": True,
        "disable_fk_during_load": True,
        "schema_only": False,
        "skip_tables": [],
        "output_dir": "csv_export",
        "grant_privileges": {
            "enabled": False,
            "users": [],
            "privileges": "ALL",
        },
    },
    "schema_map": {"MD": "md", "GNR": "gnr"},
    "table_merges": {},
    "tree_inheritance": {},
    "fk_redirects": {},
    "column_from_related": {},
    "column_renames": {},
    "skip_columns": {},
    "post_create_ddl": [],
    "post_load_ddl": [],
}

# ==================================================================
# TYPE MAPS
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
    'text': 'nvarchar(max)', 'bytea': 'varbinary(max)',
    'uuid': 'uniqueidentifier',
    'date': 'date', 'time': 'time',
    'time without time zone': 'time', 'time with time zone': 'time',
    'timestamp': 'datetime2', 'timestamp without time zone': 'datetime2',
    'timestamp with time zone': 'datetimeoffset', 'timestamptz': 'datetimeoffset',
    'xml': 'xml', 'json': 'nvarchar(max)', 'jsonb': 'nvarchar(max)',
    'inet': 'nvarchar(64)', 'cidr': 'nvarchar(64)', 'macaddr': 'nvarchar(32)',
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


def make_normalizer(snake, lowercase):
    def n(name):
        if snake:     return to_snake_case(name)
        if lowercase: return name.lower()
        return name

    return n


def qident_pg(name):
    return '"' + name.replace('"', '""') + '"'


def qident_mssql(name):
    return '[' + name.replace(']', ']]') + ']'


def _build_mssql_url(cfg):
    """Build a mssql+pyodbc URL, URL-encoding credentials so special
    characters (e.g. '@' in passwords) don't break URL parsing."""
    user = quote_plus(str(cfg.get('user', '')))
    pwd = quote_plus(str(cfg.get('password', '')))
    db = quote_plus(str(cfg.get('database', '')))
    host = cfg['host']
    port = cfg.get('port', 1433)
    trust = 'yes' if cfg.get('trust_cert') else 'no'
    return (
        f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&TrustServerCertificate={trust}"
    )


# ==================================================================
# SOURCE ADAPTERS
# ==================================================================

class MSSQLSource:
    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        self.engine = create_engine(_build_mssql_url(cfg))

    def test(self):
        with self.engine.connect() as c:
            c.execute(text("SELECT 1"))

    def list_tables(self, schemas):
        q = text("""
                 SELECT TABLE_SCHEMA, TABLE_NAME
                 FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_TYPE = 'BASE TABLE'
                   AND TABLE_SCHEMA IN :schemas
                 ORDER BY TABLE_SCHEMA, TABLE_NAME
                 """).bindparams(bindparam("schemas", expanding=True))
        with self.engine.connect() as c:
            rows = c.execute(q, {"schemas": list(schemas)}).fetchall()
        return [f'{r[0]}.{r[1]}' for r in rows]

    def get_columns(self, table_key):
        schema, table = table_key.split('.', 1)
        q = text("""
                 SELECT c.name       AS column_name,
                        t.name       AS data_type,
                        c.max_length AS max_length,
                        c.precision AS precision,
                   c.scale AS scale, c.is_nullable AS is_nullable,
                   c.is_identity AS is_identity, c.is_computed AS is_computed,
                   dc.definition AS default_definition
                 FROM sys.columns c
                     JOIN sys.types t
                 ON c.user_type_id = t.user_type_id
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
                    SELECT i.name           AS constraint_name,
                           i.is_primary_key AS is_primary,
                           c.name           AS column_name,
                           ic.key_ordinal   AS ordinal
                    FROM sys.indexes i
                             JOIN sys.index_columns ic
                                  ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                             JOIN sys.columns c
                                  ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE i.object_id = OBJECT_ID(:qualified)
                      AND (i.is_primary_key = 1 OR i.is_unique_constraint = 1)
                      AND ic.is_included_column = 0
                    ORDER BY i.name, ic.key_ordinal
                    """)
        fk_q = text("""
                    SELECT fk.name                                     AS constraint_name,
                           OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
                           OBJECT_NAME(fk.referenced_object_id)        AS ref_table,
                           cp.name                                     AS parent_column,
                           cr.name                                     AS referenced_column,
                           fkc.constraint_column_id                    AS ordinal,
                           fk.delete_referential_action_desc           AS on_delete,
                           fk.update_referential_action_desc           AS on_update
                    FROM sys.foreign_keys fk
                             JOIN sys.foreign_key_columns fkc
                                  ON fk.object_id = fkc.constraint_object_id
                             JOIN sys.columns cp
                                  ON fkc.parent_object_id = cp.object_id
                                      AND fkc.parent_column_id = cp.column_id
                             JOIN sys.columns cr
                                  ON fkc.referenced_object_id = cr.object_id
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
        df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", self.engine)
        df.columns = [self.normalize(c) for c in df.columns]
        return df


class PGSource:
    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        self.conn = psycopg2.connect(
            dbname=cfg['database'], user=cfg['user'], password=cfg['password'],
            host=cfg['host'], port=cfg.get('port', 5432))
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def test(self):
        self.cursor.execute("SELECT 1")

    def list_tables(self, schemas):
        self.cursor.execute("""
                            SELECT table_schema, table_name
                            FROM information_schema.tables
                            WHERE table_type = 'BASE TABLE'
                              AND table_schema = ANY (%s)
                            ORDER BY table_schema, table_name
                            """, (list(schemas),))
        return [f'{r[0]}.{r[1]}' for r in self.cursor.fetchall()]

    def get_columns(self, table_key):
        schema, table = table_key.split('.', 1)
        self.cursor.execute("""
                            SELECT c.column_name,
                                   c.data_type,
                                   c.udt_name,
                                   c.character_maximum_length,
                                   c.numeric_precision,
                                   c.numeric_scale,
                                   c.is_nullable,
                                   c.column_default,
                                   c.is_identity
                            FROM information_schema.columns c
                            WHERE c.table_schema = %s
                              AND c.table_name = %s
                            ORDER BY c.ordinal_position
                            """, (schema, table))
        cols = []
        for r in self.cursor.fetchall():
            cols.append({
                'column_name': r[0], 'data_type': r[1].lower(),
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
        self.cursor.execute("""
                            SELECT tc.constraint_name,
                                   tc.constraint_type,
                                   kcu.column_name,
                                   kcu.ordinal_position
                            FROM information_schema.table_constraints tc
                                     JOIN information_schema.key_column_usage kcu
                                          ON tc.constraint_name = kcu.constraint_name
                                              AND tc.table_schema = kcu.table_schema
                            WHERE tc.table_schema = %s
                              AND tc.table_name = %s
                              AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                            ORDER BY tc.constraint_name, kcu.ordinal_position
                            """, (schema, table))
        prim, uniq = None, defaultdict(list)
        for cname, ctype, col, ord_ in self.cursor.fetchall():
            if ctype == 'PRIMARY KEY':
                prim = prim or {'name': cname, 'columns': []}
                prim['columns'].append((ord_, col))
            else:
                uniq[cname].append((ord_, col))
        if prim:
            prim['columns'] = [c for _, c in sorted(prim['columns'])]
        uniq_list = [{'name': n, 'columns': [c for _, c in sorted(cols)]}
                     for n, cols in uniq.items()]

        self.cursor.execute("""
                            SELECT tc.constraint_name,
                                   ccu.table_schema,
                                   ccu.table_name,
                                   kcu.column_name,
                                   ccu.column_name,
                                   kcu.ordinal_position,
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
                            WHERE tc.table_schema = %s
                              AND tc.table_name = %s
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
            dbname=cfg['database'], user=cfg['user'], password=cfg['password'],
            host=cfg['host'], port=cfg.get('port', 5432))
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def test(self):
        self.cursor.execute("SELECT 1")

    def ensure_schema(self, schema):
        self.cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {qident_pg(schema)}')

    def table_exists(self, schema, table):
        self.cursor.execute("""
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = %s
                              AND table_name = %s
                            """, (schema, table))
        return self.cursor.fetchone() is not None

    def get_columns(self, schema, table):
        self.cursor.execute("""
                            SELECT column_name, data_type, udt_name, is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = %s
                              AND table_name = %s
                            ORDER BY ordinal_position
                            """, (schema, table))
        return {r[0]: {'type': r[1].lower(), 'udt': r[2].lower(),
                       'nullable': r[3] == 'YES'}
                for r in self.cursor.fetchall()}

    def execute(self, sql):
        self.cursor.execute(sql)

    def render_type(self, col):
        sql_type = col['data_type'].lower()
        if sql_type not in MSSQL_TO_PG_TYPES:
            raise ValueError(f"Unmapped source type: {sql_type}")
        base = MSSQL_TO_PG_TYPES[sql_type]
        if (base in PG_TYPES_NO_PARAMS or base.startswith('numeric(')
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
        import io
        qualified = f'{qident_pg(schema)}.{qident_pg(table)}'
        col_list = ", ".join(qident_pg(c) for c in columns)
        buf = df[columns].to_csv(index=False, header=False, na_rep='\\N')
        self.cursor.copy_expert(
            f"COPY {qualified} ({col_list}) FROM STDIN WITH CSV NULL '\\N'",
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
               f'ADD CONSTRAINT {qident_pg(fk_name)} FOREIGN KEY ({c}) '
               f'REFERENCES {qident_pg(ref_schema)}.{qident_pg(ref_table)} ({rc})')
        if on_delete and on_delete.upper() != 'NO ACTION':
            ddl += f' ON DELETE {on_delete.upper().replace("_", " ")}'
        if on_update and on_update.upper() != 'NO ACTION':
            ddl += f' ON UPDATE {on_update.upper().replace("_", " ")}'
        self.cursor.execute(ddl)

    def grant_table_privileges(self, schema, table, users, privileges):
        if not users:
            return
        priv = (privileges or "ALL").strip()
        if priv.upper() == "ALL":
            priv = "ALL PRIVILEGES"

        for user in users:
            try:
                self.cursor.execute(
                    f'GRANT USAGE ON SCHEMA {qident_pg(schema)} '
                    f'TO {qident_pg(user)}')
            except Exception as e:
                raise RuntimeError(f"grant USAGE on {schema} to {user}: "
                                   f"{str(e).splitlines()[0]}")

        users_q = ", ".join(qident_pg(u) for u in users)
        self.cursor.execute(
            f'GRANT {priv} ON TABLE {qident_pg(schema)}.{qident_pg(table)} '
            f'TO {users_q}')

    def scan_identity_sequences(self, schema, table):
        """Read-only scan. Returns list of (col, seq, current_next, target_next)."""
        self.cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND (is_identity = 'YES'
                   OR column_default LIKE 'nextval(%%')
        """, (schema, table))
        cols = [r[0] for r in self.cursor.fetchall()]

        results = []
        for col in cols:
            self.cursor.execute(
                "SELECT pg_get_serial_sequence(%s, %s)",
                (f'"{schema}"."{table}"', col))
            row = self.cursor.fetchone()
            if not row or not row[0]:
                continue
            seq = row[0]

            self.cursor.execute(f"SELECT last_value, is_called FROM {seq}")
            last_val, is_called = self.cursor.fetchone()
            current_next = last_val + 1 if is_called else last_val

            self.cursor.execute(f"""
                SELECT COALESCE(MAX({qident_pg(col)}), 0) + 1
                FROM {qident_pg(schema)}.{qident_pg(table)}
            """)
            target_next = self.cursor.fetchone()[0]

            results.append((col, seq, current_next, target_next))
        return results

    def reset_identity_sequences(self, schema, table):
        """Reset identity/serial sequences to MAX(col)+1.
        Returns list of (column, new_value) tuples for reporting."""
        self.cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND (is_identity = 'YES'
                   OR column_default LIKE 'nextval(%%')
        """, (schema, table))
        cols = [r[0] for r in self.cursor.fetchall()]

        results = []
        for col in cols:
            self.cursor.execute(
                "SELECT pg_get_serial_sequence(%s, %s)",
                (f'"{schema}"."{table}"', col))
            row = self.cursor.fetchone()
            if not row or not row[0]:
                continue
            seq = row[0]

            self.cursor.execute(f"""
                SELECT setval(
                    %s,
                    COALESCE((
                        SELECT MAX({qident_pg(col)})
                        FROM {qident_pg(schema)}.{qident_pg(table)}
                    ), 0) + 1,
                    false
                )
            """, (seq,))
            new_val = self.cursor.fetchone()[0]
            results.append((col, new_val))
        return results


class MSSQLTarget:
    def __init__(self, cfg, normalizer):
        self.cfg = cfg
        self.normalize = normalizer
        self.engine = create_engine(_build_mssql_url(cfg))

    def test(self):
        with self.engine.connect() as c:
            c.execute(text("SELECT 1"))

    def ensure_schema(self, schema):
        with self.engine.begin() as c:
            c.execute(text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :n) "
                f"EXEC('CREATE SCHEMA {qident_mssql(schema)}')"), {"n": schema})

    def table_exists(self, schema, table):
        with self.engine.connect() as c:
            r = c.execute(text("""
                               SELECT 1
                               FROM INFORMATION_SCHEMA.TABLES
                               WHERE TABLE_SCHEMA = :s
                                 AND TABLE_NAME = :t
                               """), {"s": schema, "t": table}).fetchone()
        return r is not None

    def get_columns(self, schema, table):
        with self.engine.connect() as c:
            rows = c.execute(text("""
                                  SELECT c.name AS column_name,
                                         t.name AS data_type,
                                         c.max_length,
                                         c.precision,
                                         c.scale,
                                         c.is_nullable
                                  FROM sys.columns c
                                           JOIN sys.types t ON c.user_type_id = t.user_type_id
                                  WHERE c.object_id = OBJECT_ID(:q)
                                  ORDER BY c.column_id
                                  """), {"q": f'[{schema}].[{table}]'}).fetchall()
        return {r[0]: {'type': r[1].lower(), 'max_length': r[2],
                       'precision': r[3], 'scale': r[4],
                       'nullable': bool(r[5])}
                for r in rows}

    def execute(self, sql):
        with self.engine.begin() as c:
            c.execute(text(sql))

    def render_type(self, col):
        dt = col['data_type'].lower()
        udt = col.get('udt_name', '').lower()
        key = udt if udt in PG_TO_MSSQL_TYPES else dt
        if key not in PG_TO_MSSQL_TYPES:
            return 'nvarchar(max)'
        base = PG_TO_MSSQL_TYPES[key]
        if base in MSSQL_TYPES_NO_PARAMS or base.endswith('(max)'):
            return base
        if base in ('nvarchar', 'nchar'):
            ml = col.get('max_length', -1)
            return 'nvarchar(max)' if ml is None or ml == -1 else f'{base}({ml})'
        if base == 'varbinary':
            ml = col.get('max_length', -1)
            return 'varbinary(max)' if ml is None or ml == -1 else f'varbinary({ml})'
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
        col_names = ', '.join(qident_mssql(c) for c in columns)
        placeholders = ', '.join(['?'] * len(columns))
        sql = (f'INSERT INTO {qident_mssql(schema)}.{qident_mssql(table)} '
               f'({col_names}) VALUES ({placeholders})')

        def clean(v):
            if v is None: return None
            try:
                if pd.isna(v): return None
            except (TypeError, ValueError):
                pass
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
        return True

    def add_fk_constraint(self, schema, table, fk_name, cols, ref_schema,
                          ref_table, ref_cols, on_delete, on_update):
        c = ', '.join(qident_mssql(x) for x in cols)
        rc = ', '.join(qident_mssql(x) for x in ref_cols)
        ddl = (f'ALTER TABLE {qident_mssql(schema)}.{qident_mssql(table)} '
               f'ADD CONSTRAINT {qident_mssql(fk_name)} FOREIGN KEY ({c}) '
               f'REFERENCES {qident_mssql(ref_schema)}.{qident_mssql(ref_table)} ({rc})')
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    def grant_table_privileges(self, schema, table, users, privileges):
        if not users:
            return
        priv = (privileges or "ALL").strip()
        if priv.upper() == "ALL":
            priv = "SELECT, INSERT, UPDATE, DELETE, REFERENCES"
        users_q = ", ".join(qident_mssql(u) for u in users)
        ddl = (f'GRANT {priv} ON {qident_mssql(schema)}.{qident_mssql(table)} '
               f'TO {users_q}')
        with self.engine.begin() as c:
            c.execute(text(ddl))

    def scan_identity_sequences(self, schema, table):
        return []   # SQL Server IDENTITY tracks its own max

    def reset_identity_sequences(self, schema, table):
        return []   # SQL Server IDENTITY tracks its own max


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

        self._resolve_configs()

        if config['direction'] == 'mssql_to_pg':
            self.source = MSSQLSource(config['sqlserver'], self.normalize)
            self.target = PGTarget(config['postgres'], self.normalize)
        else:
            self.source = PGSource(config['postgres'], self.normalize)
            self.target = MSSQLTarget(config['sqlserver'], self.normalize)

    def _resolve_table_key(self, name):
        return name if '.' in name else f'{self.source_schemas[0]}.{name}'

    def _resolve_configs(self):
        cfg = self.config
        n = self.normalize
        rk = self._resolve_table_key

        self.table_merges = {
            rk(k): {**v, 'merge_into': rk(v['merge_into']),
                    'join_on': [list(p) for p in v['join_on']]}
            for k, v in cfg.get('table_merges', {}).items()
        }
        self.tree_inheritance = {rk(k): v
                                 for k, v in cfg.get('tree_inheritance', {}).items()}
        self.fk_redirects = {
            rk(k): [{**r, 'via_table': rk(r['via_table'])} for r in lst]
            for k, lst in cfg.get('fk_redirects', {}).items()
        }
        self.column_from_related = {
            rk(k): [{**s, 'from_table': rk(s['from_table'])} for s in lst]
            for k, lst in cfg.get('column_from_related', {}).items()
        }
        self.column_renames = {
            rk(k): {n(a): n(b) for a, b in v.items()}
            for k, v in cfg.get('column_renames', {}).items()
        }
        self.skip_columns = {
            rk(k): {n(c) for c in v}
            for k, v in cfg.get('skip_columns', {}).items()
        }

    def target_schema_for(self, table_key):
        return self.schema_map[table_key.split('.', 1)[0]]

    def target_table_for(self, table_key):
        return self.normalize(table_key.split('.', 1)[1])

    def get_merge_sources(self, table_key):
        return [k for k, v in self.table_merges.items()
                if v['merge_into'] == table_key]

    # ----------------------------------------------------------
    def transform_dataframe(self, table_key, df):
        if table_key in self.fk_redirects:
            df = self._apply_fk_redirects(df, self.fk_redirects[table_key])
        for src in self.get_merge_sources(table_key):
            self.log(f"    + merging {src} into {table_key}")
            df_extra = self.source.read_table(src)
            df = self._apply_merge(df, df_extra, self.table_merges[src], src)
        if table_key in self.tree_inheritance:
            df = self._apply_tree_inheritance(df, self.tree_inheritance[table_key])
        if table_key in self.column_from_related:
            df = self._apply_column_from_related(
                df, self.column_from_related[table_key], table_key)
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
            return df_target.merge(df_extra, on=parent_keys, how='left')
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
                if pd.isna(node): return None
                if node in memo:  return memo[node]
                if seen is None:  seen = set()
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
            self.log(f"    + col-from-related: '{to_col}' ({nn}/{len(df)} rows)")
        return df

    def coerce_values(self, df, target_cols):
        for col in list(df.columns):
            if col not in target_cols:
                continue
            t = target_cols[col]
            tt = t.get('type', '').lower() if isinstance(t, dict) else ''
            udt = t.get('udt', '').lower() if isinstance(t, dict) else ''
            s = df[col]
            if tt in ('smallint', 'integer', 'bigint') or udt in ('int2', 'int4', 'int8'):
                if pd.api.types.is_float_dtype(s):
                    nn = s.dropna()
                    if len(nn) == 0 or (nn % 1 == 0).all():
                        df[col] = s.astype('Int64')
            elif tt in ('boolean',) or udt in ('bool',):
                if not pd.api.types.is_bool_dtype(s):
                    df[col] = s.map(self._to_bool)
            elif tt == 'uuid':
                df[col] = s.map(lambda v: str(v).lower()
                if v is not None and not (isinstance(v, float) and pd.isna(v))
                else None)
        return df

    @staticmethod
    def _to_bool(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return None
        if isinstance(v, bool): return v
        try:
            return int(v) != 0
        except (ValueError, TypeError):
            pass
        s = str(v).strip().lower()
        if s in ('true', 't', 'yes', 'y', 'on', '1'):  return True
        if s in ('false', 'f', 'no', 'n', 'off', '0'): return False
        return None

    # ----------------------------------------------------------
    def collect_extra_columns(self, table_key):
        """Return (extra_columns, drop_columns).
        drop_columns = source columns removed by FK_REDIRECTS."""
        extras = []
        drops = set()

        for r in self.fk_redirects.get(table_key, []):
            from_col = self.normalize(r['from_column'])
            drops.add(from_col)
            info = self.find_source_column(r['via_table'], r['value_column'])
            if info:
                c = dict(info)
                c.update(column_name=r['to_column'], is_nullable=True,
                         is_identity=False, is_computed=False,
                         default_definition=None)
                extras.append(c)

        for src in self.get_merge_sources(table_key):
            protected = {self.normalize(a)
                         for a, _ in self.table_merges[src]['join_on']}
            for c in self.source.get_columns(src):
                if self.normalize(c['column_name']) in protected:
                    continue
                c = dict(c)
                c.update(is_nullable=True, is_identity=False, is_computed=False)
                extras.append(c)

        for spec in self.column_from_related.get(table_key, []):
            info = self.find_source_column(spec['from_table'], spec['from_column'])
            if info:
                c = dict(info)
                c.update(column_name=spec['to_column'], is_nullable=True,
                         is_identity=False, is_computed=False,
                         default_definition=None)
                extras.append(c)

        return extras, drops

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

    def generate_column_definitions(self, source_cols, extra_cols, drop_cols=None):
        drop_cols = drop_cols or set()
        is_pg = self.config['direction'] == 'mssql_to_pg'
        q = qident_pg if is_pg else qident_mssql

        all_cols = []
        seen = set()
        for c in source_cols:
            n = self.normalize(c['column_name'])
            if n in drop_cols:
                continue
            all_cols.append(c)
            seen.add(n)
        for c in extra_cols:
            n = self.normalize(c['column_name'])
            if n not in seen:
                all_cols.append(c)
                seen.add(n)

        defs = []
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
            if c.get('is_identity'):
                parts.append('GENERATED BY DEFAULT AS IDENTITY' if is_pg
                             else 'IDENTITY(1,1)')
            if not c.get('is_nullable', True):
                parts.append('NOT NULL')
            if c.get('default_definition'):
                d = self.target.translate_default(c['default_definition'], type_str)
                if d:
                    parts.append(f'DEFAULT {d}')
            defs.append('    ' + ' '.join(parts))
        return defs

    def build_create_table(self, table_key, col_defs, constraints):
        ts = self.target_schema_for(table_key)
        tn = self.target_table_for(table_key)
        is_pg = self.config['direction'] == 'mssql_to_pg'
        q = qident_pg if is_pg else qident_mssql
        parts = list(col_defs)
        names = set()
        for cd in parts:
            m = re.match(r'\s*(?:"([^"]+)"|\[([^\]]+)\])', cd)
            if m:
                names.add(m.group(1) or m.group(2))
        pk = constraints['primary_key']
        if pk:
            cols = [self.normalize(c) for c in pk['columns']]
            if all(c in names for c in cols):
                parts.append(f'    CONSTRAINT {q(self.normalize(pk["name"]))} '
                             f'PRIMARY KEY ({", ".join(q(c) for c in cols)})')
        for u in constraints['uniques']:
            cols = [self.normalize(c) for c in u['columns']]
            if all(c in names for c in cols):
                parts.append(f'    CONSTRAINT {q(self.normalize(u["name"]))} '
                             f'UNIQUE ({", ".join(q(c) for c in cols)})')
        if is_pg:
            return (f'CREATE TABLE IF NOT EXISTS {q(ts)}.{q(tn)} (\n'
                    + ',\n'.join(parts) + '\n);')
        return (f"IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{ts}' AND TABLE_NAME = '{tn}') "
                f"BEGIN CREATE TABLE {q(ts)}.{q(tn)} (\n"
                + ',\n'.join(parts) + '\n) END;')

    # ----------------------------------------------------------
    def ensure_schema_and_tables(self, all_tables):
        merged_away = set(self.table_merges.keys())
        seen_targets = {}
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
        self.log("Adding foreign keys...")
        for t, constraints in all_constraints.items():
            ts = self.target_schema_for(t)
            tn = self.target_table_for(t)
            for fk in constraints['foreign_keys']:
                ref = fk['ref_key']
                if ref in merged_away or ref not in all_constraints:
                    continue
                if ref.split('.', 1)[0] not in self.schema_map:
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
        for i, stmt in enumerate(self.config.get('post_create_ddl', []), 1):
            sql = stmt.replace('{schema}', qident_pg(self.primary_target))
            try:
                self.target.execute(sql)
                self.log(f"  + [{i}] post-create OK")
            except Exception as e:
                self.log(f"  ✗ [{i}] post-create: {str(e).splitlines()[0]}")

        grant_cfg = self.config['migration'].get('grant_privileges', {})
        if grant_cfg.get('enabled') and grant_cfg.get('users'):
            users = grant_cfg['users']
            priv = grant_cfg.get('privileges', 'ALL')
            self.log(f"\nGranting '{priv}' on all tables to: {users}")
            granted, failed_grants = 0, 0
            for t in all_tables:
                if t in merged_away:
                    continue
                ts = self.target_schema_for(t)
                tn = self.target_table_for(t)
                try:
                    self.target.grant_table_privileges(ts, tn, users, priv)
                    granted += 1
                except Exception as e:
                    failed_grants += 1
                    self.log(f"  ✗ {ts}.{tn}: {str(e).splitlines()[0]}")
            if failed_grants == 0:
                self.log(f"  ✓ granted on {granted} tables")
            else:
                self.log(f"  ! granted on {granted} tables, "
                         f"{failed_grants} failed")

    def auto_load_order(self, source_tables):
        name_map = {(self.target_schema_for(t), self.target_table_for(t)): t
                    for t in source_tables}
        target_keys = set(name_map.keys())
        fk_rows = []
        if self.config['direction'] == 'mssql_to_pg':
            for schema in set(self.schema_map.values()):
                self.target.cursor.execute("""
                                           SELECT DISTINCT cn.nspname,
                                                           child.relname,
                                                           pn.nspname,
                                                           parent.relname
                                           FROM pg_constraint c
                                                    JOIN pg_class child ON child.oid = c.conrelid
                                                    JOIN pg_namespace cn ON cn.oid = child.relnamespace
                                                    JOIN pg_class parent ON parent.oid = c.confrelid
                                                    JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                                           WHERE c.contype = 'f'
                                             AND cn.nspname = %s
                                           """, (schema,))
                fk_rows.extend(self.target.cursor.fetchall())
        else:
            with self.target.engine.connect() as c:
                for schema in set(self.schema_map.values()):
                    rows = c.execute(text("""
                                          SELECT DISTINCT cs.TABLE_SCHEMA,
                                                          cs.TABLE_NAME,
                                                          ps.TABLE_SCHEMA,
                                                          ps.TABLE_NAME
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
            if child != parent and child in target_keys and parent in target_keys:
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

    # ----------------------------------------------------------
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

        def table_part(k):
            return k.split('.', 1)[1]

        skip = set(mig.get('skip_tables', []))
        all_tables = [t for t in all_tables if table_part(t) not in skip]
        merged_away = set(self.table_merges.keys())
        tables = [t for t in all_tables if t not in merged_away]

        if mig.get('create_missing_tables', True):
            self.log("Ensuring target schema and tables...")
            self.ensure_schema_and_tables(all_tables)
            self.log("")

        if mig.get('schema_only', False):
            self.log("Schema-only mode: skipping data load.")
            self.log("=" * 60)
            self.log("Done. Schema created.")
            return

        self.log("Auto-detecting load order...")
        tables = self.auto_load_order(tables)
        self.log("\nLoad order:")
        for t in tables:
            self.log(f"  {t} → {self.target_schema_for(t)}.{self.target_table_for(t)}")
        self.log("")

        if mig.get('disable_fk_during_load', True):
            if self.target.set_fk_enforcement(False):
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
                df = self.coerce_values(df, target_cols)
                csv_name = t.replace('.', '__') + '.csv'
                df.to_csv(os.path.join(out_dir, csv_name), index=False,
                          encoding='utf-8', na_rep='\\N')
                n = self.target.load_dataframe(ts, tn, list(df.columns), df)
                self.log(f"  ✓ loaded {n} rows\n")
                succeeded.append((t, n))
            except Exception as e:
                msg = str(e).strip().replace('\n', ' | ')
                self.log(f"  ✗ FAILED: {msg}\n")
                failed.append((t, msg))

        # ---- Reset identity/serial sequences to MAX+1 ----
        if self.config['direction'] == 'mssql_to_pg':
            self.log("\nResetting identity sequences...")
            fixed = 0
            for t, _ in succeeded:
                ts = self.target_schema_for(t)
                tn = self.target_table_for(t)
                try:
                    results = self.target.reset_identity_sequences(ts, tn)
                    for col, new_val in results:
                        self.log(f"  + {ts}.{tn}.{col} → next id = {new_val}")
                        fixed += 1
                except Exception as e:
                    self.log(f"  ✗ {ts}.{tn}: {str(e).splitlines()[0]}")
            if fixed:
                self.log(f"  ✓ reset {fixed} sequence(s)")
            else:
                self.log("  · no identity sequences to reset")

        for i, stmt in enumerate(cfg.get('post_load_ddl', []), 1):
            if self.stop_flag.is_set(): break
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
# WORKERS (background threads)
# ==================================================================

class MigrationWorker(QObject):
    log_msg = Signal(str)
    finished = Signal(bool)

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.stop_flag = threading.Event()

    @Slot()
    def run(self):
        ok = True
        try:
            mig = Migrator(self.config, self.log_msg.emit)
            mig.stop_flag = self.stop_flag
            mig.run()
        except Exception:
            self.log_msg.emit(traceback.format_exc())
            ok = False
        finally:
            self.finished.emit(ok)


class ConnectionTestWorker(QObject):
    """Runs a connection test on a QThread. All UI updates are emitted
    through signals so they are marshalled to the main thread — touching
    widgets from a worker thread is the classic cause of SIGSEGV."""

    log_msg  = Signal(str)
    status   = Signal(str, str)   # text, kind ("success"|"danger"|"muted")
    done     = Signal(bool, str)  # ok, error message
    finished = Signal()

    def __init__(self, cfg):
        super().__init__()
        self.cfg = cfg

    @Slot()
    def run(self):
        ok, err = True, ""
        try:
            cfg = self.cfg
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
            self.log_msg.emit("✓ Connections OK")
            self.status.emit("Connections OK", "success")
        except Exception as e:
            ok = False
            err = str(e).splitlines()[0] if str(e) else repr(e)
            self.log_msg.emit(f"✗ Connection failed: {err}")
            self.status.emit("Connection failed", "danger")
        finally:
            self.done.emit(ok, err)
            self.finished.emit()


class SequenceWorker(QObject):
    """Fixes identity sequences on an existing Postgres database.

    Runs on a QThread; all UI updates are marshalled via signals so nothing
    touches a widget from a background thread (which was causing SIGSEGV)."""

    log_msg = Signal(str)
    finished = Signal()

    def __init__(self, cfg, schemas, dry_run):
        super().__init__()
        self.cfg = cfg
        self.schemas = schemas
        self.dry_run = dry_run

    @Slot()
    def run(self):
        try:
            target = PGTarget(self.cfg['postgres'],
                              make_normalizer(False, False))

            self.log_msg.emit(f"Scanning schemas: {self.schemas}\n")
            total = 0

            for schema in self.schemas:
                target.cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (schema,))
                tables = [r[0] for r in target.cursor.fetchall()]
                self.log_msg.emit(f"--- {schema} ({len(tables)} tables) ---")

                for table in tables:
                    try:
                        if self.dry_run:
                            seqs = target.scan_identity_sequences(schema, table)
                            for col, seq, curr, tgt in seqs:
                                if curr != tgt:
                                    self.log_msg.emit(
                                        f"  ~ {schema}.{table}.{col}: "
                                        f"{curr} → {tgt}")
                                    total += 1
                                else:
                                    self.log_msg.emit(
                                        f"  · {schema}.{table}.{col}: "
                                        f"already correct ({curr})")
                        else:
                            results = target.reset_identity_sequences(
                                schema, table)
                            for col, new_val in results:
                                self.log_msg.emit(
                                    f"  + {schema}.{table}.{col} → "
                                    f"next id = {new_val}")
                                total += 1
                    except Exception as e:
                        self.log_msg.emit(
                            f"  ✗ {schema}.{table}: {str(e).splitlines()[0]}")

                self.log_msg.emit("")

            if total == 0:
                self.log_msg.emit("Nothing to do.")
            else:
                verb = "would fix" if self.dry_run else "fixed"
                self.log_msg.emit(f"Done. {verb} {total} sequence(s).")

        except Exception as e:
            self.log_msg.emit(f"FAILED: {e}")
        finally:
            self.finished.emit()


# ==================================================================
# THEME
# ==================================================================

LIGHT = {
    'bg': '#f5f6f8',
    'surface': '#ffffff',
    'surface_alt': '#fafbfc',
    'border': '#e2e4e8',
    'border_focus': '#3b82f6',
    'text': '#1a1d21',
    'text_muted': '#6b7280',
    'accent': '#3b82f6',
    'accent_hover': '#2563eb',
    'accent_text': '#ffffff',
    'hover': '#f0f2f5',
    'pressed': '#e4e7ec',
    'success': '#10b981',
    'danger': '#ef4444',
    'warning': '#f59e0b',
    'log_bg': '#fbfbfd',
    'input_bg': '#ffffff',
}

DARK = {
    'bg': '#181a1f',
    'surface': '#1f2229',
    'surface_alt': '#252932',
    'border': '#2d313b',
    'border_focus': '#60a5fa',
    'text': '#e8eaed',
    'text_muted': '#9ca3af',
    'accent': '#3b82f6',
    'accent_hover': '#60a5fa',
    'accent_text': '#ffffff',
    'hover': '#2a2e37',
    'pressed': '#333844',
    'success': '#34d399',
    'danger': '#f87171',
    'warning': '#fbbf24',
    'log_bg': '#1a1d22',
    'input_bg': '#22262e',
}


def font_family():
    system = platform.system()
    if system == 'Windows':
        return 'Segoe UI'
    if system == 'Darwin':
        return 'SF Pro Text'
    available = set(QFontDatabase.families())
    for name in ('Inter', 'Noto Sans', 'Cantarell', 'Ubuntu', 'DejaVu Sans'):
        if name in available:
            return name
    return 'Sans'


def build_stylesheet(c):
    ff = font_family()
    return f"""
    * {{
        font-family: "{ff}", system-ui, sans-serif;
        font-size: 10pt;
        outline: none;
    }}

    QMainWindow, QWidget {{
        background-color: {c['bg']};
        color: {c['text']};
    }}

    QMenuBar {{
        background-color: {c['surface']};
        color: {c['text']};
        border-bottom: 1px solid {c['border']};
        padding: 4px 6px;
    }}
    QMenuBar::item {{
        padding: 6px 12px;
        border-radius: 6px;
        background: transparent;
    }}
    QMenuBar::item:selected {{
        background-color: {c['hover']};
    }}
    QMenu {{
        background-color: {c['surface']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 6px;
    }}
    QMenu::item {{
        padding: 6px 24px 6px 16px;
        border-radius: 6px;
    }}
    QMenu::item:selected {{
        background-color: {c['accent']};
        color: {c['accent_text']};
    }}
    QMenu::separator {{
        height: 1px;
        background: {c['border']};
        margin: 6px 8px;
    }}

    QTabWidget::pane {{
        background-color: {c['surface']};
        border: 1px solid {c['border']};
        border-radius: 10px;
        top: -1px;
    }}
    QTabBar {{ background: transparent; }}
    QTabBar::tab {{
        background: transparent;
        color: {c['text_muted']};
        padding: 9px 18px;
        margin-right: 2px;
        border-top-left-radius: 8px;
        border-top-right-radius: 8px;
        border: none;
    }}
    QTabBar::tab:hover {{
        background-color: {c['hover']};
        color: {c['text']};
    }}
    QTabBar::tab:selected {{
        background-color: {c['surface']};
        color: {c['text']};
        border: 1px solid {c['border']};
        border-bottom: 1px solid {c['surface']};
        font-weight: 600;
    }}

    QGroupBox {{
        background-color: {c['surface']};
        border: 1px solid {c['border']};
        border-radius: 10px;
        margin-top: 14px;
        padding: 12px 14px 10px 14px;
        font-weight: 600;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 12px;
        top: 0px;
        padding: 0 6px;
        background-color: {c['bg']};
        color: {c['text_muted']};
        font-size: 9pt;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}

    QLineEdit, QSpinBox, QComboBox {{
        background-color: {c['input_bg']};
        color: {c['text']};
        border: 1px solid {c['border']};
        border-radius: 7px;
        padding: 7px 10px;
        selection-background-color: {c['accent']};
        selection-color: {c['accent_text']};
    }}
    QLineEdit:hover, QSpinBox:hover, QComboBox:hover {{
        border-color: {c['text_muted']};
    }}
    QLineEdit:focus, QSpinBox:focus, QComboBox:focus {{
        border: 1px solid {c['border_focus']};
    }}
    QLineEdit:disabled, QComboBox:disabled {{
        background-color: {c['surface_alt']};
        color: {c['text_muted']};
    }}

    QPushButton {{
        background-color: {c['surface']};
        color: {c['text']};
        border: 1px solid {c['border']};
        border-radius: 7px;
        padding: 7px 16px;
        font-weight: 500;
    }}
    QPushButton:hover {{
        background-color: {c['hover']};
        border-color: {c['text_muted']};
    }}
    QPushButton:pressed {{ background-color: {c['pressed']}; }}
    QPushButton:disabled {{
        color: {c['text_muted']};
        background-color: {c['surface_alt']};
        border-color: {c['border']};
    }}
    QPushButton[accent="true"] {{
        background-color: {c['accent']};
        color: {c['accent_text']};
        border: 1px solid {c['accent']};
        font-weight: 600;
    }}
    QPushButton[accent="true"]:hover {{
        background-color: {c['accent_hover']};
        border-color: {c['accent_hover']};
    }}
    QPushButton[accent="true"]:disabled {{
        background-color: {c['surface_alt']};
        color: {c['text_muted']};
        border-color: {c['border']};
    }}
    QPushButton[danger="true"] {{ color: {c['danger']}; }}
    QPushButton[danger="true"]:hover {{
        background-color: {c['danger']};
        color: white;
        border-color: {c['danger']};
    }}

    QCheckBox {{
        color: {c['text']};
        spacing: 8px;
        padding: 3px 0;
    }}
    QCheckBox::indicator {{
        width: 16px;
        height: 16px;
        border: 1.5px solid {c['border']};
        border-radius: 4px;
        background-color: {c['input_bg']};
    }}
    QCheckBox::indicator:hover {{ border-color: {c['accent']}; }}
    QCheckBox::indicator:checked {{
        background-color: {c['accent']};
        border-color: {c['accent']};
    }}

    QRadioButton {{
        color: {c['text']};
        spacing: 8px;
        padding: 3px 0;
    }}
    QRadioButton::indicator {{
        width: 15px;
        height: 15px;
        border: 1.5px solid {c['border']};
        border-radius: 8px;
        background-color: {c['input_bg']};
    }}
    QRadioButton::indicator:hover {{ border-color: {c['accent']}; }}
    QRadioButton::indicator:checked {{
        background-color: {c['accent']};
        border-color: {c['accent']};
    }}

    QPlainTextEdit {{
        background-color: {c['log_bg']};
        color: {c['text']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 8px;
        selection-background-color: {c['accent']};
        selection-color: {c['accent_text']};
    }}
    QPlainTextEdit:focus {{ border: 1px solid {c['border_focus']}; }}

    QScrollBar:vertical {{
        background: transparent;
        width: 10px;
        margin: 2px;
    }}
    QScrollBar::handle:vertical {{
        background: {c['border']};
        border-radius: 5px;
        min-height: 30px;
    }}
    QScrollBar::handle:vertical:hover {{ background: {c['text_muted']}; }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0px; }}
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{ background: transparent; }}

    QScrollBar:horizontal {{
        background: transparent;
        height: 10px;
        margin: 2px;
    }}
    QScrollBar::handle:horizontal {{
        background: {c['border']};
        border-radius: 5px;
        min-width: 30px;
    }}
    QScrollBar::handle:horizontal:hover {{ background: {c['text_muted']}; }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0px; }}

    QStatusBar {{
        background-color: {c['surface']};
        color: {c['text_muted']};
        border-top: 1px solid {c['border']};
    }}

    QProgressBar {{
        background-color: {c['surface_alt']};
        border: 1px solid {c['border']};
        border-radius: 6px;
        text-align: center;
        height: 8px;
        color: transparent;
    }}
    QProgressBar::chunk {{
        background-color: {c['accent']};
        border-radius: 5px;
    }}

    QFrame[role="card"] {{
        background-color: {c['surface']};
        border: 1px solid {c['border']};
        border-radius: 10px;
    }}

    QLabel[role="heading"] {{
        font-size: 15pt;
        font-weight: 700;
        color: {c['text']};
    }}
    QLabel[role="subheading"] {{
        font-size: 10pt;
        color: {c['text_muted']};
    }}
    QLabel[role="muted"] {{
        color: {c['text_muted']};
        font-size: 9pt;
    }}
    QLabel[role="success"] {{ color: {c['success']}; font-weight: 600; }}
    QLabel[role="danger"]  {{ color: {c['danger']};  font-weight: 600; }}
    """


# ==================================================================
# MAIN WINDOW
# ==================================================================

class ETLWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Studio — SQL Server ↔ PostgreSQL")
        self.resize(1180, 800)
        self.setMinimumSize(900, 600)

        self.settings = QSettings("ETLStudio", "ETLStudio")
        self.theme_name = self.settings.value("theme", "light")
        self.config = json.loads(json.dumps(DEFAULT_CONFIG))

        self.worker = None
        self.worker_thread = None
        self._seq_worker = None
        self._seq_thread = None
        self._test_worker = None
        self._test_thread = None

        self._build_menu()
        self._build_ui()
        self._apply_theme(self.theme_name)

    def _build_menu(self):
        mb = self.menuBar()
        fm = mb.addMenu("File")
        fm.addAction("Open Config…", self.on_open_config)
        fm.addAction("Save Config…", self.on_save_config)
        fm.addSeparator()
        fm.addAction("Quit", self.close)

        vm = mb.addMenu("View")
        tg = QActionGroup(self)
        tg.setExclusive(True)
        self.a_light = QAction("Light theme", self, checkable=True)
        self.a_dark = QAction("Dark theme", self, checkable=True)
        tg.addAction(self.a_light)
        tg.addAction(self.a_dark)
        vm.addAction(self.a_light)
        vm.addAction(self.a_dark)
        (self.a_light if self.theme_name == 'light' else self.a_dark).setChecked(True)
        self.a_light.triggered.connect(lambda: self._apply_theme('light'))
        self.a_dark.triggered.connect(lambda: self._apply_theme('dark'))

        hm = mb.addMenu("Help")
        hm.addAction("About", self.on_about)

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(14, 10, 14, 10)
        root.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("ETL Studio")
        title.setProperty("role", "heading")
        subtitle = QLabel("Bidirectional schema & data migration")
        subtitle.setProperty("role", "subheading")
        col = QVBoxLayout()
        col.setSpacing(0)
        col.addWidget(title)
        col.addWidget(subtitle)
        header.addLayout(col)
        header.addStretch()
        root.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        root.addWidget(self.tabs, 1)

        self._build_tab_connections()
        self._build_tab_migration()
        self._build_tab_advanced()
        self._build_tab_utilities()
        self._build_tab_log()

        bar = QFrame()
        bar.setProperty("role", "card")
        bar_layout = QHBoxLayout(bar)
        bar_layout.setContentsMargins(12, 8, 12, 8)
        bar_layout.setSpacing(8)

        self.btn_test = QPushButton("Test Connections")
        self.btn_test.clicked.connect(self.on_test)

        self.btn_run = QPushButton("Run Migration")
        self.btn_run.setProperty("accent", True)
        self.btn_run.clicked.connect(self.on_run)

        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setProperty("danger", True)
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.on_stop)

        bar_layout.addWidget(self.btn_test)
        bar_layout.addWidget(self.btn_run)
        bar_layout.addWidget(self.btn_stop)
        bar_layout.addSpacing(20)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.setVisible(False)
        self.progress.setFixedWidth(180)
        bar_layout.addWidget(self.progress)

        self.status_label = QLabel("Ready")
        self.status_label.setProperty("role", "muted")
        bar_layout.addWidget(self.status_label)
        bar_layout.addStretch()

        root.addWidget(bar)

    def _build_tab_connections(self):
        tab = QWidget()
        self.tabs.addTab(tab, "Connections")

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        outer = QVBoxLayout(tab)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        body = QWidget()
        scroll.setWidget(body)
        v = QVBoxLayout(body)
        v.setContentsMargins(14, 14, 14, 14)
        v.setSpacing(14)

        dir_box = QGroupBox("Direction")
        dl = QHBoxLayout(dir_box)
        self.dir_group = QButtonGroup(self)
        self.radio_m2p = QRadioButton("SQL Server → PostgreSQL")
        self.radio_p2m = QRadioButton("PostgreSQL → SQL Server")
        self.dir_group.addButton(self.radio_m2p, 0)
        self.dir_group.addButton(self.radio_p2m, 1)
        self.radio_m2p.setChecked(True)
        dl.addWidget(self.radio_m2p)
        dl.addWidget(self.radio_p2m)
        dl.addStretch()
        v.addWidget(dir_box)

        cols = QHBoxLayout()
        cols.setSpacing(14)

        self.ss_inputs = {}
        ss_box = QGroupBox("SQL Server")
        ss_form = QFormLayout(ss_box)
        ss_form.setLabelAlignment(Qt.AlignRight)
        ss_form.setSpacing(8)
        for key, label, default, pw in [
            ('host', 'Host', 'localhost', False),
            ('port', 'Port', 1433, False),
            ('user', 'User', 'sa', False),
            ('password', 'Password', '', True),
            ('database', 'Database', 'MyDB', False),
        ]:
            if key == 'port':
                w = QSpinBox()
                w.setRange(1, 65535)
                w.setValue(int(default))
            else:
                w = QLineEdit(str(default))
                if pw: w.setEchoMode(QLineEdit.Password)
            ss_form.addRow(label, w)
            self.ss_inputs[key] = w
        self.ss_trust = QCheckBox("Trust server certificate")
        self.ss_trust.setChecked(True)
        ss_form.addRow("", self.ss_trust)
        cols.addWidget(ss_box, 1)

        self.pg_inputs = {}
        pg_box = QGroupBox("PostgreSQL")
        pg_form = QFormLayout(pg_box)
        pg_form.setLabelAlignment(Qt.AlignRight)
        pg_form.setSpacing(8)
        for key, label, default, pw in [
            ('host', 'Host', 'localhost', False),
            ('port', 'Port', 5432, False),
            ('user', 'User', 'postgres', False),
            ('password', 'Password', '', True),
            ('database', 'Database', 'MyPgDB', False),
        ]:
            if key == 'port':
                w = QSpinBox()
                w.setRange(1, 65535)
                w.setValue(int(default))
            else:
                w = QLineEdit(str(default))
                if pw: w.setEchoMode(QLineEdit.Password)
            pg_form.addRow(label, w)
            self.pg_inputs[key] = w
        cols.addWidget(pg_box, 1)

        v.addLayout(cols)

        sm_box = QGroupBox("Schema Map")
        sm_layout = QVBoxLayout(sm_box)
        sm_layout.setSpacing(6)
        hint = QLabel("Source schema = target schema, one per line. "
                      "Example: MD = md")
        hint.setProperty("role", "muted")
        sm_layout.addWidget(hint)
        self.schema_text = QPlainTextEdit()
        self.schema_text.setFixedHeight(100)
        self.schema_text.setPlaceholderText("MD = md\nGNR = gnr")
        sm_layout.addWidget(self.schema_text)
        self._load_schema_map()
        v.addWidget(sm_box)

        v.addStretch()

    def _load_schema_map(self):
        self.schema_text.setPlainText(
            "\n".join(f"{k} = {v}" for k, v in self.config['schema_map'].items()))

    def _build_tab_migration(self):
        tab = QWidget()
        self.tabs.addTab(tab, "Migration")

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        outer = QVBoxLayout(tab)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        body = QWidget()
        scroll.setWidget(body)
        v = QVBoxLayout(body)
        v.setContentsMargins(14, 14, 14, 14)
        v.setSpacing(14)

        opt_box = QGroupBox("Options")
        ov = QVBoxLayout(opt_box)
        ov.setSpacing(6)
        self.chk_snake = QCheckBox("Convert names to snake_case")
        self.chk_lower = QCheckBox("Lowercase names (only if snake_case is off)")
        self.chk_create = QCheckBox("Create missing target tables")
        self.chk_fkoff = QCheckBox("Disable FK enforcement during load")
        self.chk_schema_only = QCheckBox("Schema only (create tables, no data)")
        for w in (self.chk_snake, self.chk_lower, self.chk_create,
                  self.chk_fkoff, self.chk_schema_only):
            ov.addWidget(w)
        self.chk_snake.setChecked(True)
        self.chk_lower.setChecked(True)
        self.chk_create.setChecked(True)
        self.chk_fkoff.setChecked(True)
        self.chk_schema_only.setChecked(False)
        v.addWidget(opt_box)

        out_box = QGroupBox("CSV Output Directory")
        ol = QHBoxLayout(out_box)
        self.out_edit = QLineEdit(self.config['migration']['output_dir'])
        self.out_edit.setPlaceholderText("csv_export")
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._pick_outdir)
        ol.addWidget(self.out_edit, 1)
        ol.addWidget(browse)
        v.addWidget(out_box)

        skip_box = QGroupBox("Skip Tables")
        sl = QVBoxLayout(skip_box)
        sl.setSpacing(6)
        h = QLabel("Bare table names, one per line.")
        h.setProperty("role", "muted")
        sl.addWidget(h)
        self.skip_text = QPlainTextEdit()
        self.skip_text.setFixedHeight(110)
        self.skip_text.setPlaceholderText("sysdiagrams\nSomeLegacyTable")
        sl.addWidget(self.skip_text)
        v.addWidget(skip_box)

        grant_box = QGroupBox("Grant Privileges")
        gl = QVBoxLayout(grant_box)
        gl.setSpacing(6)

        self.chk_grant = QCheckBox("Grant privileges on created tables")
        gl.addWidget(self.chk_grant)

        h2 = QLabel("Users / roles (one per line). They must already exist.")
        h2.setProperty("role", "muted")
        gl.addWidget(h2)

        self.grant_users = QPlainTextEdit()
        self.grant_users.setFixedHeight(80)
        self.grant_users.setPlaceholderText("app_user\nreadonly_user")
        gl.addWidget(self.grant_users)

        priv_row = QHBoxLayout()
        priv_row.addWidget(QLabel("Privileges:"))
        self.grant_priv = QLineEdit("ALL")
        self.grant_priv.setPlaceholderText(
            "ALL  or  SELECT, INSERT, UPDATE, DELETE")
        priv_row.addWidget(self.grant_priv, 1)
        gl.addLayout(priv_row)

        v.addWidget(grant_box)
        v.addStretch()

    def _pick_outdir(self):
        d = QFileDialog.getExistingDirectory(self, "Choose output directory")
        if d:
            self.out_edit.setText(d)

    def _build_tab_advanced(self):
        tab = QWidget()
        self.tabs.addTab(tab, "Advanced")

        v = QVBoxLayout(tab)
        v.setContentsMargins(10, 10, 10, 10)
        v.setSpacing(8)

        hint = QLabel(
            "Column-level transformations (JSON). Edit carefully — "
            "each section is validated when you run the migration.")
        hint.setProperty("role", "muted")
        hint.setWordWrap(True)
        v.addWidget(hint)

        self.adv_edits = {}
        sections = [
            ('table_merges', 'TABLE_MERGES',
             'Merge child tables into parents.'),
            ('tree_inheritance', 'TREE_INHERITANCE',
             'Propagate a tree-root value to all descendants.'),
            ('fk_redirects', 'FK_REDIRECTS',
             'Replace a column value via a lookup in another table.'),
            ('column_from_related', 'COLUMN_FROM_RELATED',
             'Populate a column from an already-loaded target table.'),
            ('column_renames', 'COLUMN_RENAMES',
             'Rename columns during migration.'),
            ('skip_columns', 'SKIP_COLUMNS',
             'Drop columns that do not exist in the target.'),
            ('post_create_ddl', 'POST_CREATE_DDL',
             'SQL statements to run after tables are created.'),
            ('post_load_ddl', 'POST_LOAD_DDL',
             'SQL statements to run after loading, before FKs are restored.'),
        ]

        grid = QGridLayout()
        grid.setSpacing(8)
        for i, (key, title, tooltip) in enumerate(sections):
            box = QGroupBox(title)
            bl = QVBoxLayout(box)
            bl.setSpacing(4)
            tip = QLabel(tooltip)
            tip.setProperty("role", "muted")
            tip.setWordWrap(True)
            bl.addWidget(tip)
            edit = QPlainTextEdit()
            edit.setPlaceholderText("{}")
            edit.setPlainText(json.dumps(self.config.get(key, {}), indent=2))
            edit.setFixedHeight(120)
            bl.addWidget(edit)
            self.adv_edits[key] = edit
            grid.addWidget(box, i // 2, i % 2)

        v.addLayout(grid)
        v.addStretch()

    def _build_tab_utilities(self):
        tab = QWidget()
        self.tabs.addTab(tab, "Utilities")

        v = QVBoxLayout(tab)
        v.setContentsMargins(14, 14, 14, 14)
        v.setSpacing(14)

        info = QLabel(
            "Fix identity / serial sequences on an existing PostgreSQL database. "
            "Use this when you've loaded data with explicit IDs and new INSERTs "
            "collide with rows that already exist. Uses the PostgreSQL "
            "connection from the Connections tab.")
        info.setWordWrap(True)
        info.setProperty("role", "muted")
        v.addWidget(info)

        schema_box = QGroupBox("Schemas to Scan")
        sl = QVBoxLayout(schema_box)
        sl.setSpacing(6)
        hint = QLabel(
            "One schema per line. Leave empty to use the schemas from the "
            "Schema Map on the Connections tab.")
        hint.setProperty("role", "muted")
        sl.addWidget(hint)
        self.util_schemas = QPlainTextEdit()
        self.util_schemas.setFixedHeight(90)
        self.util_schemas.setPlaceholderText("md\ngnr")
        sl.addWidget(self.util_schemas)
        v.addWidget(schema_box)

        btn_row = QHBoxLayout()
        self.btn_scan_seqs = QPushButton("Scan (dry run)")
        self.btn_scan_seqs.clicked.connect(
            lambda: self.on_fix_sequences(dry_run=True))
        self.btn_fix_seqs = QPushButton("Fix Sequences")
        self.btn_fix_seqs.setProperty("accent", True)
        self.btn_fix_seqs.clicked.connect(
            lambda: self.on_fix_sequences(dry_run=False))
        btn_row.addWidget(self.btn_scan_seqs)
        btn_row.addWidget(self.btn_fix_seqs)
        btn_row.addStretch()
        v.addLayout(btn_row)

        self.util_log = QPlainTextEdit()
        self.util_log.setReadOnly(True)
        self.util_log.setLineWrapMode(QPlainTextEdit.NoWrap)
        f = QFont("JetBrains Mono")
        f.setStyleHint(QFont.Monospace)
        f.setPointSize(9)
        self.util_log.setFont(f)
        v.addWidget(self.util_log, 1)

    def on_fix_sequences(self, dry_run=False):
        try:
            cfg = self._collect_config()
        except ValueError as e:
            QMessageBox.critical(self, "Config error", str(e))
            return

        self.util_log.clear()
        self.btn_fix_seqs.setEnabled(False)
        self.btn_scan_seqs.setEnabled(False)

        raw = self.util_schemas.toPlainText().strip()
        if raw:
            schemas = [s.strip() for s in raw.splitlines() if s.strip()]
        else:
            schemas = list(cfg['schema_map'].values())

        # Hand the work off to a QObject on a QThread. All UI updates go
        # through signals so they get marshalled onto the main thread —
        # direct widget access from a worker thread causes SIGSEGV.
        self._seq_worker = SequenceWorker(cfg, schemas, dry_run)
        self._seq_thread = QThread(self)
        self._seq_worker.moveToThread(self._seq_thread)
        self._seq_worker.log_msg.connect(self.util_log.appendPlainText)
        self._seq_worker.finished.connect(self._on_seq_finished)
        self._seq_thread.started.connect(self._seq_worker.run)
        self._seq_thread.start()

    def _on_seq_finished(self):
        self.btn_fix_seqs.setEnabled(True)
        self.btn_scan_seqs.setEnabled(True)
        if self._seq_thread:
            self._seq_thread.quit()
            self._seq_thread.wait(2000)
        self._seq_worker = None
        self._seq_thread = None

    def _build_tab_log(self):
        tab = QWidget()
        self.tabs.addTab(tab, "Log")

        v = QVBoxLayout(tab)
        v.setContentsMargins(10, 10, 10, 10)
        v.setSpacing(8)

        top = QHBoxLayout()
        self.log_filter = QLineEdit()
        self.log_filter.setPlaceholderText("Filter log…")
        clear = QPushButton("Clear")
        clear.clicked.connect(lambda: self.log_view.clear())
        top.addWidget(self.log_filter, 1)
        top.addWidget(clear)
        v.addLayout(top)

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QPlainTextEdit.NoWrap)
        f = QFont("JetBrains Mono")
        f.setStyleHint(QFont.Monospace)
        f.setPointSize(9)
        self.log_view.setFont(f)
        v.addWidget(self.log_view, 1)

    def _apply_theme(self, name):
        self.theme_name = name
        self.settings.setValue("theme", name)
        theme = LIGHT if name == 'light' else DARK
        QApplication.instance().setStyleSheet(build_stylesheet(theme))
        if name == 'light':
            self.a_light.setChecked(True)
        else:
            self.a_dark.setChecked(True)

    def _collect_config(self):
        c = json.loads(json.dumps(DEFAULT_CONFIG))

        c['direction'] = ('mssql_to_pg' if self.radio_m2p.isChecked()
                          else 'pg_to_mssql')

        for key, w in self.ss_inputs.items():
            c['sqlserver'][key] = w.value() if isinstance(w, QSpinBox) else w.text()
        c['sqlserver']['trust_cert'] = self.ss_trust.isChecked()

        for key, w in self.pg_inputs.items():
            c['postgres'][key] = w.value() if isinstance(w, QSpinBox) else w.text()

        c['migration']['convert_snake_case'] = self.chk_snake.isChecked()
        c['migration']['lowercase_names'] = self.chk_lower.isChecked()
        c['migration']['create_missing_tables'] = self.chk_create.isChecked()
        c['migration']['disable_fk_during_load'] = self.chk_fkoff.isChecked()
        c['migration']['schema_only'] = self.chk_schema_only.isChecked()
        c['migration']['output_dir'] = self.out_edit.text() or 'csv_export'

        c['migration']['skip_tables'] = [
            s.strip() for s in self.skip_text.toPlainText().splitlines()
            if s.strip()]
        c['migration']['grant_privileges'] = {
            'enabled': self.chk_grant.isChecked(),
            'users': [u.strip() for u in
                      self.grant_users.toPlainText().splitlines() if u.strip()],
            'privileges': self.grant_priv.text().strip() or 'ALL',
        }
        mapping = {}
        for line in self.schema_text.toPlainText().splitlines():
            line = line.strip()
            if not line or '=' not in line: continue
            k, v = line.split('=', 1)
            mapping[k.strip()] = v.strip()
        if not mapping:
            raise ValueError("Schema Map is empty — add at least one mapping.")
        c['schema_map'] = mapping

        for key, edit in self.adv_edits.items():
            raw = edit.toPlainText().strip()
            if not raw or raw == '{}':
                c[key] = {} if key not in ('post_create_ddl', 'post_load_ddl') else []
                continue
            try:
                c[key] = json.loads(raw)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in '{key}': {e}")

        return c

    def _set_status(self, text, kind=None):
        self.status_label.setText(text)
        self.status_label.setProperty("role", kind or "muted")
        self.status_label.style().unpolish(self.status_label)
        self.status_label.style().polish(self.status_label)

    def _append_log(self, text):
        self.log_view.appendPlainText(text)
        cursor = self.log_view.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_view.setTextCursor(cursor)

    def on_test(self):
        try:
            cfg = self._collect_config()
        except ValueError as e:
            QMessageBox.critical(self, "Config error", str(e))
            return
        self._set_status("Testing connections…")
        self.btn_test.setEnabled(False)

        # Run the connection test on a QThread and route every UI update
        # through signals — never touch widgets from a raw Python thread.
        self._test_worker = ConnectionTestWorker(cfg)
        self._test_thread = QThread(self)
        self._test_worker.moveToThread(self._test_thread)

        self._test_worker.log_msg.connect(self._append_log)
        self._test_worker.status.connect(self._set_status)
        self._test_worker.done.connect(self._on_test_done)
        self._test_worker.finished.connect(self._test_thread.quit)
        self._test_thread.started.connect(self._test_worker.run)
        self._test_thread.start()

    @Slot(bool, str)
    def _on_test_done(self, ok, err):
        self.btn_test.setEnabled(True)
        if not ok and err:
            QMessageBox.critical(self, "Connection failed", err)
        if self._test_thread:
            self._test_thread.wait(2000)
        self._test_worker = None
        self._test_thread = None

    def on_run(self):
        try:
            cfg = self._collect_config()
        except ValueError as e:
            QMessageBox.critical(self, "Config error", str(e))
            return

        self.log_view.clear()
        self.btn_run.setEnabled(False)
        self.btn_test.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.progress.setVisible(True)
        self._set_status("Running migration…")

        self.worker = MigrationWorker(cfg)
        self.worker_thread = QThread(self)
        self.worker.moveToThread(self.worker_thread)

        self.worker.log_msg.connect(self._append_log)
        self.worker.finished.connect(self._on_worker_finished)
        self.worker_thread.started.connect(self.worker.run)

        self.worker_thread.start()

    def _on_worker_finished(self, ok):
        self.btn_run.setEnabled(True)
        self.btn_test.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.progress.setVisible(False)
        self._set_status("Done" if ok else "Failed", "success" if ok else "danger")
        if self.worker_thread:
            self.worker_thread.quit()
            self.worker_thread.wait(2000)
        self.worker = None
        self.worker_thread = None

    def on_stop(self):
        if self.worker:
            self.worker.stop_flag.set()
            self._append_log("! Stop requested — waiting for current table…")
            self.btn_stop.setEnabled(False)

    def on_open_config(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open config", "", "JSON (*.json);;All (*.*)")
        if not path: return
        try:
            with open(path) as f:
                loaded = json.load(f)
            merged = json.loads(json.dumps(DEFAULT_CONFIG))
            for k, v in loaded.items():
                if isinstance(v, dict) and isinstance(merged.get(k), dict):
                    merged[k].update(v)
                else:
                    merged[k] = v
            self.config = merged
            self._load_config_into_ui()
            self._append_log(f"Loaded config: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Load failed", str(e))

    def on_save_config(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save config", "", "JSON (*.json)")
        if not path: return
        if not path.endswith('.json'): path += '.json'
        try:
            cfg = self._collect_config()
            with open(path, 'w') as f:
                json.dump(cfg, f, indent=2)
            self._append_log(f"Saved config: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))

    def _load_config_into_ui(self):
        c = self.config
        if c['direction'] == 'mssql_to_pg':
            self.radio_m2p.setChecked(True)
        else:
            self.radio_p2m.setChecked(True)

        for k, w in self.ss_inputs.items():
            v = c['sqlserver'].get(k, '')
            if isinstance(w, QSpinBox):
                w.setValue(int(v))
            else:
                w.setText(str(v))
        self.ss_trust.setChecked(c['sqlserver'].get('trust_cert', True))

        for k, w in self.pg_inputs.items():
            v = c['postgres'].get(k, '')
            if isinstance(w, QSpinBox):
                w.setValue(int(v))
            else:
                w.setText(str(v))

        gp = c['migration'].get('grant_privileges', {})
        self.chk_grant.setChecked(gp.get('enabled', False))
        self.grant_users.setPlainText("\n".join(gp.get('users', [])))
        self.grant_priv.setText(gp.get('privileges', 'ALL'))

        self.chk_snake.setChecked(c['migration']['convert_snake_case'])
        self.chk_lower.setChecked(c['migration']['lowercase_names'])
        self.chk_create.setChecked(c['migration']['create_missing_tables'])
        self.chk_fkoff.setChecked(c['migration']['disable_fk_during_load'])
        self.chk_schema_only.setChecked(c['migration'].get('schema_only', False))
        self.out_edit.setText(c['migration']['output_dir'])
        self.skip_text.setPlainText(
            "\n".join(c['migration'].get('skip_tables', [])))

        self.schema_text.setPlainText(
            "\n".join(f"{k} = {v}" for k, v in c['schema_map'].items()))

        if not self.util_schemas.toPlainText().strip():
            self.util_schemas.setPlainText(
                "\n".join(c['schema_map'].values()))

        for key, edit in self.adv_edits.items():
            edit.setPlainText(json.dumps(c.get(key, {}), indent=2))

    def on_about(self):
        QMessageBox.about(
            self, "About ETL Studio",
            "<h3>ETL Studio</h3>"
            "<p>Bidirectional SQL Server ↔ PostgreSQL migration "
            "with column-level transforms.</p>"
            "<p>Configure, test, and run migrations from a modern UI.</p>")


# ==================================================================
# ENTRY
# ==================================================================

def main():
    QApplication.setApplicationName("ETL Studio")
    QApplication.setOrganizationName("ETLStudio")
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    win = ETLWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
