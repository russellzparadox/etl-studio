#!/usr/bin/env python3
"""
SQL Server → PostgreSQL schema + data migration.

- Each source schema maps to its own target schema via SCHEMA_MAP
- Optional snake_case conversion for table and column names
- CREATE TABLE translation, TABLE_MERGES, FK_REDIRECTS,
  COLUMN_FROM_RELATED, TREE_INHERITANCE
- Cross-schema FK-safe load order
- POST_CREATE_DDL and POST_LOAD_DDL hooks
"""

import os
import re
import pandas as pd
from collections import defaultdict, deque
from sqlalchemy import create_engine, text, bindparam
import psycopg2

# ==================================================================
# 1. CONFIG
# ==================================================================

SQLSERVER_USER     = 'sa'
SQLSERVER_PASSWORD = 'AHm59wtu'
SQLSERVER_HOST     = 'localhost'
SQLSERVER_DB       = 'raes_system'

# NEW: source schema -> target schema
# First entry is considered the "primary" target schema.
SCHEMA_MAP = {
    'MD':   'md',
    'GNR':  'gnr',
}

# NEW: global snake_case conversion
CONVERT_TO_SNAKE_CASE = True
LOWERCASE_NAMES       = True       # used only if CONVERT_TO_SNAKE_CASE is False

PG_USER     = 'postgres'
PG_PASSWORD = ''
PG_HOST     = 'localhost'
PG_DB       = 'etl_test'

OUTPUT_DIR      = 'csv_export'
CREATE_MISSING_TABLES = True
SKIP_TABLES = {'sysdiagrams'}
LOAD_ORDER  = None

# Configs use bare table names (assumed first source schema) or 'schema.table'.
COLUMN_RENAMES = {}
SKIP_COLUMNS   = {}

TABLE_MERGES = {
    'CategoryProperty': {
        'merge_into':  'Category',
        'join_on':     [('CategoryID', 'id')],
        'on_conflict': 'keep_target',
        'on_multi':    'error',
    },
}

COLUMN_FROM_RELATED = {
    'CategoryMember': [
        {
            'from_table':  'Category',
            'from_column': 'entityid',
            'via_key':     'id',
            'via_fk':      'CategoryID',
            'to_column':   'entityid',
        }
    ],
}

TREE_INHERITANCE = {
    'Category': {
        'id_col':     'id',
        'parent_col': 'parentid',
        'columns':    ['entityid'],
    },
}

FK_REDIRECTS = {
    'CategoryPropertyColumn': [
        {
            'from_column':  'CategoryPropertyID',
            'via_table':    'CategoryProperty',
            'via_key':      'id',
            'value_column': 'CategoryID',
            'to_column':    'CategoryID',
        }
    ],
}

# SQL below uses real target-schema names. {schema} is replaced by the
# primary target schema (first value in SCHEMA_MAP).
POST_CREATE_DDL = []

POST_LOAD_DDL = [
    "ALTER TABLE md.category ALTER COLUMN entityid SET NOT NULL",
    """
    ALTER TABLE md.category
        DROP CONSTRAINT IF EXISTS fk_category_entity,
        ADD  CONSTRAINT fk_category_entity
             FOREIGN KEY (entityid) REFERENCES md.entity(id)
             ON UPDATE CASCADE ON DELETE RESTRICT
    """,
    """
    ALTER TABLE md.category
        DROP CONSTRAINT IF EXISTS uq_category_id_entityid,
        ADD  CONSTRAINT uq_category_id_entityid UNIQUE (id, entityid)
    """,
    """
    CREATE OR REPLACE FUNCTION md.category_same_entity()
    RETURNS TRIGGER AS $$
    DECLARE v_root_id bigint; v_expected bigint;
    BEGIN
        WITH RECURSIVE ancestors AS (
            SELECT id, parentid, entityid FROM md.category WHERE id = NEW.id
            UNION ALL
            SELECT c.id, c.parentid, c.entityid
            FROM md.category c JOIN ancestors a ON c.id = a.parentid
        )
        SELECT id, entityid INTO v_root_id, v_expected
        FROM ancestors WHERE parentid IS NULL LIMIT 1;
        IF v_expected IS NOT NULL AND NEW.entityid IS DISTINCT FROM v_expected THEN
            RAISE EXCEPTION 'entityid must match tree root % (expected %, got %)',
                v_root_id, v_expected, NEW.entityid;
        END IF;
        RETURN NEW;
    END; $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS trg_category_same_entity ON md.category;
    CREATE TRIGGER trg_category_same_entity
    BEFORE INSERT OR UPDATE ON md.category
    FOR EACH ROW EXECUTE FUNCTION md.category_same_entity();
    """,
    "ALTER TABLE md.categorymember ALTER COLUMN entityid SET NOT NULL",
    """
    ALTER TABLE md.categorymember
        DROP CONSTRAINT IF EXISTS fk_categorymember_category_entity,
        ADD  CONSTRAINT fk_categorymember_category_entity
             FOREIGN KEY (categoryid, entityid)
             REFERENCES md.category(id, entityid)
             ON UPDATE CASCADE ON DELETE RESTRICT
    """,
    """
    ALTER TABLE md.categorymember
        DROP CONSTRAINT IF EXISTS fk_categorymember_entity,
        ADD  CONSTRAINT fk_categorymember_entity
             FOREIGN KEY (entityid) REFERENCES md.entity(id)
             ON UPDATE CASCADE ON DELETE RESTRICT
    """,
]

DISABLE_FK_DURING_LOAD = True

SOURCE_SCHEMAS = list(SCHEMA_MAP.keys())
PRIMARY_TARGET_SCHEMA = next(iter(SCHEMA_MAP.values()))


# ==================================================================
# 2. NAME NORMALIZATION
# ==================================================================

_SNAKE_1 = re.compile(r'(.)([A-Z][a-z]+)')
_SNAKE_2 = re.compile(r'([a-z0-9])([A-Z])')


def to_snake_case(name):
    s1 = _SNAKE_1.sub(r'\1_\2', name)
    s2 = _SNAKE_2.sub(r'\1_\2', s1)
    return s2.lower()


def _normalize(name):
    if CONVERT_TO_SNAKE_CASE:
        return to_snake_case(name)
    if LOWERCASE_NAMES:
        return name.lower()
    return name


def _qident(name):
    return '"' + name.replace('"', '""') + '"'


def _resolve_table_key(name):
    """Bare name → 'schema.table' using the first source schema."""
    if '.' in name:
        return name
    return f'{SOURCE_SCHEMAS[0]}.{name}'


def target_schema_for(source_schema):
    if source_schema not in SCHEMA_MAP:
        raise ValueError(f"No target mapping for source schema "
                         f"'{source_schema}'. Update SCHEMA_MAP.")
    return SCHEMA_MAP[source_schema]


def target_table_for(table_key):
    """'schema.table' → normalized target table name (no schema)."""
    if '.' not in table_key:
        table_key = f'{SOURCE_SCHEMAS[0]}.{table_key}'
    _, table = table_key.split('.', 1)
    return _normalize(table)


def target_schema_for_table(table_key):
    schema = table_key.split('.', 1)[0]
    return target_schema_for(schema)


def _norm(name):
    return _normalize(name)


# ==================================================================
# 3. TYPE MAPPING
# ==================================================================

SQLSERVER_TO_PG_TYPES = {
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

PG_TYPES_NO_PARAMS = {
    'integer', 'bigint', 'smallint', 'boolean', 'double precision', 'real',
    'text', 'xml', 'bytea', 'date', 'time', 'timestamp', 'timestamptz', 'uuid',
}


def translate_type(sql_type, max_length, precision, scale, is_max):
    sql_type = sql_type.lower()
    if sql_type not in SQLSERVER_TO_PG_TYPES:
        raise ValueError(f"Unmapped SQL Server type: {sql_type}")
    base = SQLSERVER_TO_PG_TYPES[sql_type]
    if base in PG_TYPES_NO_PARAMS or base.startswith('numeric(') or base.startswith('varchar('):
        return base
    if base in ('varchar', 'char'):
        if is_max:
            return 'text'
        length = max_length // 2 if sql_type in ('nvarchar', 'nchar') else max_length
        return f'{base}({length})'
    if base == 'numeric':
        return f'numeric({precision},{scale})'
    return base


# ==================================================================
# 4. CONNECTIONS
# ==================================================================

sql_engine = create_engine(
    f"mssql+pyodbc://{SQLSERVER_USER}:{SQLSERVER_PASSWORD}"
    f"@{SQLSERVER_HOST}/{SQLSERVER_DB}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)
pg_conn = psycopg2.connect(
    dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST
)
pg_conn.autocommit = True
pg_cursor = pg_conn.cursor()


# ==================================================================
# 5. CONFIG RESOLUTION
# ==================================================================

def _resolve_all_configs():
    global TABLE_MERGES, TREE_INHERITANCE, FK_REDIRECTS
    global COLUMN_FROM_RELATED, COLUMN_RENAMES, SKIP_COLUMNS

    # Validate SCHEMA_MAP covers every source schema listed
    for s in SOURCE_SCHEMAS:
        if s not in SCHEMA_MAP:
            raise ValueError(f"SCHEMA_MAP missing entry for source schema '{s}'")

    TABLE_MERGES = {
        _resolve_table_key(k): {**v, 'merge_into': _resolve_table_key(v['merge_into'])}
        for k, v in TABLE_MERGES.items()
    }
    TREE_INHERITANCE = {_resolve_table_key(k): v for k, v in TREE_INHERITANCE.items()}
    FK_REDIRECTS = {
        _resolve_table_key(k): [
            {**r, 'via_table': _resolve_table_key(r['via_table'])} for r in v
        ]
        for k, v in FK_REDIRECTS.items()
    }
    COLUMN_FROM_RELATED = {
        _resolve_table_key(k): [
            {**s, 'from_table': _resolve_table_key(s['from_table'])} for s in v
        ]
        for k, v in COLUMN_FROM_RELATED.items()
    }
    COLUMN_RENAMES = {
        _resolve_table_key(k): {_normalize(ck): _normalize(cv) for ck, cv in v.items()}
        for k, v in COLUMN_RENAMES.items()
    }
    SKIP_COLUMNS = {
        _resolve_table_key(k): {_normalize(c) for c in v}
        for k, v in SKIP_COLUMNS.items()
    }


# ==================================================================
# 6. SQL SERVER INTROSPECTION
# ==================================================================

def get_source_tables(schemas):
    q = text("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN :schemas
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """).bindparams(bindparam("schemas", expanding=True))
    with sql_engine.connect() as c:
        rows = c.execute(q, {"schemas": list(schemas)}).fetchall()
    return [f'{r[0]}.{r[1]}' for r in rows]

def get_target_columns(table, schema):
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


def get_merge_sources(target_table_key):
    return [k for k, v in TABLE_MERGES.items() if v['merge_into'] == target_table_key]


def get_sqlserver_columns(table_key):
    schema, table = table_key.split('.', 1)
    q = text("""
        SELECT c.name AS column_name, t.name AS data_type,
               c.max_length, c.precision, c.scale,
               c.is_nullable, c.is_identity, c.is_computed,
               dc.definition AS default_definition
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.default_constraints dc
          ON dc.parent_object_id = c.object_id
         AND dc.parent_column_id = c.column_id
        WHERE c.object_id = OBJECT_ID(:qualified)
        ORDER BY c.column_id
    """)
    qualified = f'[{schema}].[{table}]'
    with sql_engine.connect() as conn:
        rows = conn.execute(q, {"qualified": qualified}).fetchall()
    return [dict(r._mapping) for r in rows]


def find_column_info(table_key, col_name):
    target = _normalize(col_name)
    for c in get_sqlserver_columns(table_key):
        if _normalize(c['column_name']) == target:
            return c
    for src in get_merge_sources(table_key):
        for c in get_sqlserver_columns(src):
            if _normalize(c['column_name']) == target:
                return c
    return None


def get_sqlserver_constraints(table_key):
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
    with sql_engine.connect() as conn:
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

    return {'primary_key': primary_key, 'uniques': uniques_list, 'foreign_keys': fks}


# ==================================================================
# 7. DEFAULT TRANSLATION
# ==================================================================

_DEFAULT_MAP = [
    (re.compile(r'^\(?getdate\(\)\)?$',        re.I), 'CURRENT_TIMESTAMP'),
    (re.compile(r'^\(?sysdatetime\(\)\)?$',     re.I), 'CURRENT_TIMESTAMP'),
    (re.compile(r'^\(?getutcdate\(\)\)?$',      re.I), "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
    (re.compile(r'^\(?sysutcdatetime\(\)\)?$',  re.I), "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
    (re.compile(r'^\(?newid\(\)\)?$',           re.I), 'gen_random_uuid()'),
    (re.compile(r'^\(?newsequentialid\(\)\)?$', re.I), 'gen_random_uuid()'),
    (re.compile(r'^\(?suser_sname\(\)\)?$',     re.I), 'CURRENT_USER'),
    (re.compile(r'^\(?user_name\(\)\)?$',       re.I), 'CURRENT_USER'),
]


def translate_default(sql_default, pg_type):
    if not sql_default:
        return None
    d = sql_default.strip()
    for pat, rep in _DEFAULT_MAP:
        if pat.match(d):
            return rep
    if d.startswith('(') and d.endswith(')'):
        d = d[1:-1].strip()
    if re.fullmatch(r'-?\d+(\.\d+)?', d):
        return d
    m = re.fullmatch(r"N?'(.*)'", d)
    if m:
        return "'" + m.group(1).replace("'", "''") + "'"
    if pg_type == 'boolean' and d in ('0', '1'):
        return 'TRUE' if d == '1' else 'FALSE'
    return None


# ==================================================================
# 8. CREATE TABLE GENERATION
# ==================================================================

def build_create_table(table_key):
    cols = get_sqlserver_columns(table_key)
    constraints = get_sqlserver_constraints(table_key)

    # FK redirects
    for r in FK_REDIRECTS.get(table_key, []):
        from_col = _normalize(r['from_column'])
        value_info = find_column_info(r['via_table'], r['value_column'])
        if value_info is None:
            print(f"    ! redirect: {r['value_column']} not found on {r['via_table']}")
            continue
        cols = [c for c in cols if _normalize(c['column_name']) != from_col]
        new_info = dict(value_info)
        new_info['column_name'] = _normalize(r['to_column'])
        new_info['is_nullable'] = True
        new_info['is_identity'] = False
        new_info['is_computed'] = False
        new_info['default_definition'] = None
        cols.append(new_info)

    # Merge-source columns
    merge_srcs = get_merge_sources(table_key)
    protected = set()
    for src in merge_srcs:
        for child, _ in TABLE_MERGES[src]['join_on']:
            protected.add(_normalize(child))

    seen = {_normalize(c['column_name']) for c in cols}
    for src in merge_srcs:
        for c in get_sqlserver_columns(src):
            cname = _normalize(c['column_name'])
            if cname in seen or cname in protected:
                continue
            c = dict(c)
            c['is_nullable'] = True
            c['is_identity'] = False
            c['is_computed'] = False
            cols.append(c)
            seen.add(cname)

    # COLUMN_FROM_RELATED additions
    for spec in COLUMN_FROM_RELATED.get(table_key, []):
        to_col = _normalize(spec['to_column'])
        if to_col in seen:
            continue
        from_info = find_column_info(spec['from_table'], spec['from_column'])
        if from_info is None:
            print(f"    ! col-from-related: '{spec['from_column']}' not on "
                  f"{spec['from_table']}")
            continue
        c = dict(from_info)
        c['column_name'] = to_col
        c['is_nullable'] = True
        c['is_identity'] = False
        c['is_computed'] = False
        c['default_definition'] = None
        cols.append(c)
        seen.add(to_col)

    target_name = target_table_for(table_key)
    target_schema = target_schema_for_table(table_key)
    qualified = f'{_qident(target_schema)}.{_qident(target_name)}'

    column_defs, warnings = [], []
    for c in cols:
        cname = _normalize(c['column_name'])
        if c['is_computed']:
            warnings.append(f"computed column '{cname}' skipped")
            continue
        is_max = (c['max_length'] == -1)
        pg_type = translate_type(c['data_type'], c['max_length'],
                                 c['precision'], c['scale'], is_max)
        parts = [_qident(cname), pg_type]
        if c['is_identity']:
            parts.append('GENERATED BY DEFAULT AS IDENTITY')
        if not c['is_nullable']:
            parts.append('NOT NULL')
        if c['default_definition']:
            expr = translate_default(c['default_definition'], pg_type)
            if expr:
                parts.append(f'DEFAULT {expr}')
            else:
                warnings.append(
                    f"untranslated default on '{cname}': {c['default_definition']}")
        column_defs.append('    ' + ' '.join(parts))

    existing_names = {_normalize(cd['column_name']) for cd in cols}

    pk = constraints['primary_key']
    if pk:
        present = [_normalize(c) for c in pk['columns']
                   if _normalize(c) in existing_names]
        if len(present) == len(pk['columns']):
            pk_cols = ', '.join(_qident(c) for c in present)
            column_defs.append(
                f'    CONSTRAINT {_qident(pk["name"])} PRIMARY KEY ({pk_cols})')
        else:
            warnings.append(f"PK {pk['name']} skipped (redirect removed a key column)")

    for u in constraints['uniques']:
        present = [_normalize(c) for c in u['columns']
                   if _normalize(c) in existing_names]
        if len(present) == len(u['columns']):
            u_cols = ', '.join(_qident(c) for c in present)
            column_defs.append(f'    CONSTRAINT {_qident(u["name"])} UNIQUE ({u_cols})')

    ddl = (f'CREATE TABLE IF NOT EXISTS {qualified} (\n'
           + ',\n'.join(column_defs) + '\n);')

    for w in warnings:
        print(f"    ! {table_key}: {w}")

    return ddl, constraints


def _map_fk_action(action):
    if not action:
        return None
    action = action.upper()
    return {'NO_ACTION': 'NO ACTION', 'SET_NULL': 'SET NULL',
            'SET_DEFAULT': 'SET DEFAULT'}.get(
        action, action if action in ('CASCADE', 'RESTRICT') else None)


def ensure_target_schema(source_tables):
    # Create every target schema upfront
    for target_schema in set(SCHEMA_MAP.values()):
        pg_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {_qident(target_schema)}')
    print(f"Target schemas ensured: {sorted(set(SCHEMA_MAP.values()))}\n")

    merged_away = set(TABLE_MERGES.keys())
    all_constraints = {}

    # Track (target_schema, target_table) uniqueness
    seen_targets = {}

    for t in source_tables:
        if t in merged_away:
            print(f"  · {t}: merged into {TABLE_MERGES[t]['merge_into']}, no table created")
            continue

        target_schema = target_schema_for_table(t)
        target_name   = target_table_for(t)
        key = (target_schema, target_name)

        if key in seen_targets:
            print(f"  ✗ {t}: target name '{target_schema}.{target_name}' "
                  f"already used by {seen_targets[key]}")
            continue
        seen_targets[key] = t

        pg_cursor.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (target_schema, target_name))
        exists = pg_cursor.fetchone() is not None

        try:
            ddl, constraints = build_create_table(t)
        except Exception as e:
            print(f"    ✗ {t}: cannot generate DDL: {e}")
            continue

        all_constraints[t] = constraints

        if exists:
            print(f"  · {t}: {target_schema}.{target_name} exists, skipping CREATE")
            continue

        try:
            pg_cursor.execute(ddl)
            print(f"  + {t}: created as {target_schema}.{target_name}")
        except Exception as e:
            print(f"  ✗ {t}: CREATE failed: {str(e).splitlines()[0]}")

    print("\nAdding foreign keys...")
    redirected_from = {t: {_normalize(r['from_column'])
                           for r in FK_REDIRECTS.get(t, [])}
                       for t in all_constraints}

    for t, constraints in all_constraints.items():
        target_schema = target_schema_for_table(t)
        target_name   = target_table_for(t)

        for fk in constraints['foreign_keys']:
            ref_key = fk['ref_key']
            if ref_key in merged_away:
                print(f"  · {t}.{fk['name']}: skipped (references merged table)")
                continue
            if ref_key not in all_constraints:
                print(f"  · {t}.{fk['name']}: skipped (ref {ref_key} not in migration)")
                continue
            if any(_normalize(c) in redirected_from.get(t, set()) for c in fk['columns']):
                print(f"  · {t}.{fk['name']}: skipped (columns redirected)")
                continue

            # ---- Resolve reference target names ----
            ref_schema = ref_key.split('.', 1)[0]
            if ref_schema not in SCHEMA_MAP:
                print(f"  · {t}.{fk['name']}: skipped (schema '{ref_schema}' "
                      f"not in SCHEMA_MAP)")
                continue
            ref_target_schema = target_schema_for(ref_schema)
            ref_target_table  = target_table_for(ref_key)

            fk_name = _norm(fk['name'])
            pg_cursor.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_schema = %s AND table_name = %s
                  AND constraint_name = %s AND constraint_type = 'FOREIGN KEY'
            """, (target_schema, target_name, fk_name))
            if pg_cursor.fetchone():
                continue

            cols = ', '.join(_qident(_normalize(c)) for c in fk['columns'])
            ref_cols = ', '.join(_qident(_normalize(c)) for c in fk['ref_columns'])
            ddl = (f'ALTER TABLE {_qident(target_schema)}.{_qident(target_name)} '
                   f'ADD CONSTRAINT {_qident(fk_name)} FOREIGN KEY ({cols}) '
                   f'REFERENCES {_qident(ref_target_schema)}.{_qident(ref_target_table)} '
                   f'({ref_cols})')
            on_del = _map_fk_action(fk['on_delete'])
            on_upd = _map_fk_action(fk['on_update'])
            if on_del: ddl += f' ON DELETE {on_del}'
            if on_upd: ddl += f' ON UPDATE {on_upd}'
            try:
                pg_cursor.execute(ddl)
                print(f"  + {t}.{fk_name} → {ref_key}")
            except Exception as e:
                print(f"  ✗ {t}.{fk_name}: {str(e).splitlines()[0]}")

    if POST_CREATE_DDL:
        print("\nRunning post-create DDL...")
        for i, stmt in enumerate(POST_CREATE_DDL, 1):
            sql = stmt.replace('{schema}', _qident(PRIMARY_TARGET_SCHEMA))
            first_line = sql.strip().splitlines()[0][:70]
            try:
                pg_cursor.execute(sql)
                print(f"  + [{i}] OK: {first_line}")
            except Exception as e:
                print(f"  ✗ [{i}] FAILED: {first_line}")
                print(f"           {str(e).splitlines()[0]}")


def run_post_load_ddl():
    if not POST_LOAD_DDL:
        return
    print("\nRunning post-load DDL...")
    for i, stmt in enumerate(POST_LOAD_DDL, 1):
        sql = stmt.replace('{schema}', _qident(PRIMARY_TARGET_SCHEMA))
        first_line = sql.strip().splitlines()[0][:70]
        try:
            pg_cursor.execute(sql)
            print(f"  + [{i}] OK: {first_line}")
        except Exception as e:
            print(f"  ✗ [{i}] FAILED: {first_line}")
            print(f"           {str(e).splitlines()[0]}")


# ==================================================================
# 9. LOAD ORDER (global — crosses target schemas)
# ==================================================================

def get_load_order(source_tables):
    name_map = {}
    for t in source_tables:
        name_map[(target_schema_for_table(t), target_table_for(t))] = t
    target_keys = set(name_map.keys())

    # Query FKs from every target schema, unify into (schema, table) keys
    fk_rows = []
    for target_schema in set(SCHEMA_MAP.values()):
        pg_cursor.execute("""
            SELECT DISTINCT
                cn.nspname AS child_schema,  child.relname  AS child_table,
                pn.nspname AS parent_schema, parent.relname AS parent_table
            FROM pg_constraint c
            JOIN pg_class     child  ON child.oid  = c.conrelid
            JOIN pg_namespace cn     ON cn.oid     = child.relnamespace
            JOIN pg_class     parent ON parent.oid = c.confrelid
            JOIN pg_namespace pn     ON pn.oid     = parent.relnamespace
            WHERE c.contype = 'f' AND cn.nspname = %s
        """, (target_schema,))
        fk_rows.extend(pg_cursor.fetchall())

    parents_of = defaultdict(set)
    for cs, ct, ps, pt in fk_rows:
        child  = (cs, ct.lower())
        parent = (ps, pt.lower())
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
        print(f"    ! circular FK dependency among: {remaining}")
        ordered.extend(remaining)

    return [name_map[t] for t in ordered]


# ==================================================================
# 10. TYPE COERCION
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


def coerce_for_target(df, target_cols):
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
# 11. MERGE / REDIRECT / RELATED / TREE
# ==================================================================

def _read_sqlserver_table(table_key):
    schema, table = table_key.split('.', 1)
    df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", sql_engine)
    if CONVERT_TO_SNAKE_CASE or LOWERCASE_NAMES:
        df.columns = [_normalize(c) for c in df.columns]
    return df


def apply_merge(df_target, df_extra, cfg, merge_src):
    pairs = cfg['join_on']
    child_keys  = [_normalize(c) for c, _ in pairs]
    parent_keys = [_normalize(p) for _, p in pairs]

    for c in child_keys:
        if c not in df_extra.columns:
            raise RuntimeError(f"merge {merge_src}: '{c}' missing from {merge_src}")
    for p in parent_keys:
        if p not in df_target.columns:
            raise RuntimeError(
                f"merge {merge_src}: '{p}' missing from {cfg['merge_into']}")

    if df_extra.duplicated(subset=child_keys).any():
        if cfg['on_multi'] == 'error':
            dupes = df_extra[df_extra.duplicated(subset=child_keys, keep=False)]
            sample = dupes[child_keys].head(3).to_dict('records')
            raise RuntimeError(
                f"merge {merge_src}: multiple rows per key {child_keys}. "
                f"Examples: {sample}")
        elif cfg['on_multi'] == 'take_first':
            df_extra = df_extra.drop_duplicates(subset=child_keys, keep='first')

    conflicts = (set(df_target.columns) & set(df_extra.columns)) - set(child_keys)
    if conflicts:
        print(f"    ! merge {merge_src}: duplicate columns "
              f"{sorted(conflicts)} kept from {cfg['merge_into']}")
        df_extra = df_extra.drop(columns=list(conflicts))

    if set(child_keys) == set(parent_keys):
        merged = df_target.merge(df_extra, on=parent_keys, how='left',
                                 suffixes=('', '_extra'))
    else:
        merged = df_target.merge(df_extra, left_on=parent_keys,
                                 right_on=child_keys, how='left',
                                 suffixes=('', '_extra'))
        drop_after = [c for c in child_keys
                      if c in merged.columns and c not in df_target.columns]
        if drop_after:
            merged = merged.drop(columns=drop_after)
    return merged


def apply_fk_redirects(df, redirects, table):
    for r in redirects:
        from_col  = _normalize(r['from_column'])
        via_key   = _normalize(r['via_key'])
        value_col = _normalize(r['value_column'])
        to_col    = _normalize(r['to_column'])

        if from_col not in df.columns:
            print(f"    ! FK redirect {table}: '{from_col}' not in source")
            continue

        lookup = _read_sqlserver_table(r['via_table'])
        if via_key not in lookup.columns or value_col not in lookup.columns:
            print(f"    ! FK redirect {table}: {r['via_table']} missing "
                  f"'{via_key}' or '{value_col}'")
            continue

        mapping = (lookup.drop_duplicates(subset=[via_key])
                         .set_index(via_key)[value_col])
        df[to_col] = df[from_col].map(mapping)
        df = df.drop(columns=[from_col])
        print(f"    + FK redirect: {from_col} -> {to_col} (via {r['via_table']})")
    return df


def apply_column_from_related(df, specs, table):
    for spec in specs:
        from_target = target_table_for(spec['from_table'])
        from_schema = target_schema_for_table(spec['from_table'])
        from_col    = _normalize(spec['from_column'])
        via_key     = _normalize(spec['via_key'])
        via_fk      = _normalize(spec['via_fk'])
        to_col      = _normalize(spec['to_column'])

        if via_fk not in df.columns:
            print(f"    ! col-from-related {table}: '{via_fk}' not in source")
            continue

        try:
            pg_cursor.execute(
                f'SELECT {_qident(via_key)}, {_qident(from_col)} '
                f'FROM {_qident(from_schema)}.{_qident(from_target)}'
            )
            mapping = dict(pg_cursor.fetchall())
        except Exception as e:
            print(f"    ! col-from-related {table}: cannot query "
                  f"{from_schema}.{from_target}: {str(e).splitlines()[0]}")
            continue

        df[to_col] = df[via_fk].map(mapping)
        non_null = df[to_col].notna().sum()
        print(f"    + col-from-related: '{to_col}' <- "
              f"{from_schema}.{from_target}.{from_col} via {via_fk} "
              f"({non_null}/{len(df)} rows)")

        missing = df[df[to_col].isna() & df[via_fk].notna()]
        if len(missing):
            sample = missing[[via_fk]].head(3).to_dict('records')
            print(f"    ! {len(missing)} rows in {table} reference "
                  f"{via_fk} values not found in "
                  f"{from_schema}.{from_target}. Examples: {sample}")
    return df


def apply_tree_inheritance(df, cfg, table):
    id_col     = _normalize(cfg['id_col'])
    parent_col = _normalize(cfg['parent_col'])
    cols       = [_normalize(c) for c in cfg['columns']]

    if id_col not in df.columns or parent_col not in df.columns:
        print(f"    ! tree inheritance {table}: missing {id_col} or {parent_col}")
        return df

    id_to_parent = {}
    for _id, _pid in zip(df[id_col], df[parent_col]):
        if pd.notna(_id):
            id_to_parent[_id] = _pid if pd.notna(_pid) else None

    for col in cols:
        if col not in df.columns:
            print(f"    ! tree inheritance {table}: '{col}' missing")
            continue

        col_values = dict(zip(df[id_col], df[col]))
        memo = {}

        def find_root_value(node, _seen=None):
            if pd.isna(node):
                return None
            if node in memo:
                return memo[node]
            if _seen is None:
                _seen = set()
            if node in _seen:
                memo[node] = None
                return None
            _seen.add(node)
            v = col_values.get(node)
            if pd.notna(v):
                memo[node] = v
                return v
            parent = id_to_parent.get(node)
            result = find_root_value(parent, _seen) if parent is not None else None
            memo[node] = result
            return result

        df[col] = df[id_col].map(find_root_value)
        non_null = df[col].notna().sum()
        print(f"    + tree inheritance: '{col}' populated for "
              f"{non_null}/{len(df)} rows")

    return df


# ==================================================================
# 12. EXTRACT / LOAD
# ==================================================================

NA_REP = '\\N'


def extract_table(table_key, csv_path, target_cols):
    df = _read_sqlserver_table(table_key)

    if table_key in FK_REDIRECTS:
        df = apply_fk_redirects(df, FK_REDIRECTS[table_key], table_key)

    for merge_src in get_merge_sources(table_key):
        cfg = TABLE_MERGES[merge_src]
        print(f"    + merging {merge_src} into {table_key}")
        df_extra = _read_sqlserver_table(merge_src)
        df = apply_merge(df, df_extra, cfg, merge_src)

    if table_key in TREE_INHERITANCE:
        df = apply_tree_inheritance(df, TREE_INHERITANCE[table_key], table_key)

    if table_key in COLUMN_FROM_RELATED:
        df = apply_column_from_related(df, COLUMN_FROM_RELATED[table_key], table_key)

    if table_key in COLUMN_RENAMES:
        df = df.rename(columns=COLUMN_RENAMES[table_key])

    skips = SKIP_COLUMNS.get(table_key, set())
    keep, dropped = [], []
    for c in df.columns:
        if c in skips or c not in target_cols:
            dropped.append(c)
        else:
            keep.append(c)
    if dropped:
        print(f"    ! dropping columns not present in target: {dropped}")
    df = df[keep]

    if not list(df.columns):
        raise RuntimeError(f"No columns of {table_key} match the target table")

    df = coerce_for_target(df, target_cols)
    df.to_csv(csv_path, index=False, encoding='utf-8', na_rep=NA_REP)
    return len(df), list(df.columns)


def load_table(table_key, csv_path, columns):
    target_schema = target_schema_for_table(table_key)
    target_name   = target_table_for(table_key)
    qualified = f'{_qident(target_schema)}.{_qident(target_name)}'
    col_list = ", ".join(_qident(c) for c in columns)
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        pg_cursor.copy_expert(
            f"COPY {qualified} ({col_list}) "
            f"FROM STDIN WITH CSV HEADER NULL '{NA_REP}'", f)
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        return sum(1 for _ in f) - 1


# ==================================================================
# 13. ORCHESTRATION
# ==================================================================

def main():
    _resolve_all_configs()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_tables = get_source_tables(SOURCE_SCHEMAS)

    def _table_part(key):
        return key.split('.', 1)[1]

    tables = [t for t in all_tables if _table_part(t) not in SKIP_TABLES]

    merged_away = set(TABLE_MERGES.keys())
    tables = [t for t in tables if t not in merged_away]

    if CREATE_MISSING_TABLES:
        print("Ensuring target schema and tables...")
        ensure_target_schema([t for t in all_tables
                              if _table_part(t) not in SKIP_TABLES])
        print()

    if LOAD_ORDER:
        source_set = set(tables)
        order_idx = {_resolve_table_key(t): i for i, t in enumerate(LOAD_ORDER)}
        tables = sorted(source_set, key=lambda t: (order_idx.get(t, 10**6), t))
        print("Using manual LOAD_ORDER.")
    else:
        print("Auto-detecting load order from FK constraints...")
        tables = get_load_order(tables)

    print(f"\nMigrating {len(tables)} tables")
    print(f"  source schemas: {SOURCE_SCHEMAS}")
    print(f"  target schemas: {sorted(set(SCHEMA_MAP.values()))}")
    print(f"\nLoad order:\n  " + "\n  ".join(
        f'{t} → {target_schema_for_table(t)}.{target_table_for(t)}' for t in tables
    ) + "\n")

    if DISABLE_FK_DURING_LOAD:
        try:
            pg_cursor.execute("SET session_replication_role = 'replica'")
            print("FK checks disabled for this session.\n")
        except Exception as e:
            print(f"! Could not disable FK checks: {e}\n")

    succeeded, failed = [], []

    for table_key in tables:
        print(f"--- {table_key} ---")
        # Filename: schema.table -> schema__table.csv
        csv_name = table_key.replace('.', '__') + '.csv'
        csv_path = os.path.join(OUTPUT_DIR, csv_name)
        try:
            target_name = target_table_for(table_key)
            target_schema = target_schema_for_table(table_key)
            target_cols = get_target_columns(target_name, target_schema)
            if not target_cols:
                raise RuntimeError(
                    f"target table {target_schema}.{target_name} not found")
            exported, columns = extract_table(table_key, csv_path, target_cols)
            loaded = load_table(table_key, csv_path, columns)
            print(f"  ✓ exported {exported} rows, loaded {loaded} rows\n")
            succeeded.append((table_key, loaded))
        except Exception as e:
            msg = str(e).strip().replace('\n', ' | ')
            print(f"  ✗ FAILED: {msg}\n")
            failed.append((table_key, msg))

    run_post_load_ddl()

    if DISABLE_FK_DURING_LOAD:
        try:
            pg_cursor.execute("SET session_replication_role = 'origin'")
            print("\nFK enforcement restored.")
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


def check_fk_integrity():
    """Check FK integrity across all target schemas."""
    broken = []
    for target_schema in set(SCHEMA_MAP.values()):
        pg_cursor.execute("""
            SELECT tc.table_name, kcu.column_name,
                   ccu.table_schema, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
        """, (target_schema,))
        for (tbl, col, rsch, rtbl, rcol) in pg_cursor.fetchall():
            pg_cursor.execute(f"""
                SELECT COUNT(*) FROM "{target_schema}"."{tbl}" c
                LEFT JOIN "{rsch}"."{rtbl}" p ON c."{col}" = p."{rcol}"
                WHERE c."{col}" IS NOT NULL AND p."{rcol}" IS NULL
            """)
            cnt = pg_cursor.fetchone()[0]
            if cnt:
                broken.append((target_schema, tbl, col, rsch, rtbl, cnt))

    if not broken:
        print("✓ All FKs satisfied.")
    else:
        print("✗ Orphaned rows detected:")
        for sch, tbl, col, rsch, rtbl, cnt in broken:
            print(f"  {sch}.{tbl}.{col} -> {rsch}.{rtbl}: {cnt} orphan row(s)")


if __name__ == "__main__":
    main()
    # check_fk_integrity()