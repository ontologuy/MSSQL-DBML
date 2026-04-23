import argparse
import csv
import os
import sys
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.environ['MSSQL_SERVER']};"
    f"DATABASE={os.environ['MSSQL_DATABASE']};"
    f"UID={os.environ['MSSQL_USERNAME']};"
    f"PWD={os.environ['MSSQL_PASSWORD']};"
    "TrustServerCertificate=yes;"
)

OUTPUT_DIR = Path("output")
INCLUDE_VIEWS = False


def get_connection():
    return pyodbc.connect(CONNECTION_STRING)


def load_base_tables(path="tables.csv"):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    headers = rows[0].keys() if rows else []
    id_col = next(k for k in headers if k.strip().upper() == "ID")
    schema_col = next(k for k in headers if "schema" in k.lower())
    table_col = next(k for k in headers if "table" in k.lower())
    return [(row[id_col].strip(), row[schema_col].strip(), row[table_col].strip()) for row in rows]


# ── MSSQL queries ──────────────────────────────────────────────────────────────

FK_QUERY = """
SELECT
    fk.name                                                           AS fk_name,
    OBJECT_SCHEMA_NAME(fk.parent_object_id)                          AS from_schema,
    OBJECT_NAME(fk.parent_object_id)                                 AS from_table,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id)             AS from_col,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id)                      AS to_schema,
    OBJECT_NAME(fk.referenced_object_id)                             AS to_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id)     AS to_col
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
WHERE fk.parent_object_id    = OBJECT_ID(?)
   OR fk.referenced_object_id = OBJECT_ID(?)
"""

VIEW_DEPS_QUERY = """
SELECT DISTINCT
    OBJECT_SCHEMA_NAME(d.referencing_id) AS view_schema,
    OBJECT_NAME(d.referencing_id)        AS view_name
FROM sys.sql_expression_dependencies d
WHERE d.referenced_id = OBJECT_ID(?)
  AND OBJECTPROPERTY(d.referencing_id, 'IsView') = 1
"""

PK_QUERY = """
SELECT c.name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
"""

COLUMN_INFO_QUERY = """
SELECT c.name, t.name AS type_name, c.max_length, c.precision, c.scale, c.is_nullable
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(?)
  AND c.name IN ({placeholders})
ORDER BY c.column_id
"""

COLUMN_ALL_QUERY = """
SELECT c.name, t.name AS type_name, c.max_length, c.precision, c.scale, c.is_nullable
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(?)
ORDER BY c.column_id
"""


# ── type formatting ────────────────────────────────────────────────────────────

def format_type(type_name, max_length, precision, scale):
    t = type_name.lower()
    if t in ("nvarchar", "nchar"):
        if max_length == -1:
            return f"{type_name}(max)"
        return f"{type_name}({max_length // 2})"
    if t in ("varchar", "char", "binary", "varbinary"):
        if max_length == -1:
            return f"{type_name}(max)"
        return f"{type_name}({max_length})"
    if t in ("decimal", "numeric"):
        return f"{type_name}({precision},{scale})"
    return type_name


# ── per-table helpers ──────────────────────────────────────────────────────────

def fetch_fk_relationships(cursor, schema, table):
    obj_id = f"{schema}.{table}"
    cursor.execute(FK_QUERY, obj_id, obj_id)
    rows = cursor.fetchall()
    return [
        {
            "fk_name": r.fk_name,
            "from_schema": r.from_schema,
            "from_table": r.from_table,
            "from_col": r.from_col,
            "to_schema": r.to_schema,
            "to_table": r.to_table,
            "to_col": r.to_col,
        }
        for r in rows
    ]


def fetch_dependent_views(cursor, schema, table):
    cursor.execute(VIEW_DEPS_QUERY, f"{schema}.{table}")
    return [(r.view_schema, r.view_name) for r in cursor.fetchall()]


def fetch_pk_columns(cursor, schema, table):
    cursor.execute(PK_QUERY, f"{schema}.{table}")
    return {r.name for r in cursor.fetchall()}


def fetch_column_info(cursor, schema, table, col_names):
    if not col_names:
        return {}
    placeholders = ",".join("?" * len(col_names))
    query = COLUMN_INFO_QUERY.format(placeholders=placeholders)
    cursor.execute(query, f"{schema}.{table}", *col_names)
    return {
        r.name: {
            "type": format_type(r.type_name, r.max_length, r.precision, r.scale),
            "nullable": bool(r.is_nullable),
        }
        for r in cursor.fetchall()
    }


def fetch_all_column_info(cursor, schema, table):
    cursor.execute(COLUMN_ALL_QUERY, f"{schema}.{table}")
    return {
        r.name: {
            "type": format_type(r.type_name, r.max_length, r.precision, r.scale),
            "nullable": bool(r.is_nullable),
        }
        for r in cursor.fetchall()
    }


# ── DBML generation ────────────────────────────────────────────────────────────

def qualified(schema, table):
    return f"{schema}.{table}"


def dbml_table_block(schema, table, columns_info, pk_cols, is_base=False, is_view=False):
    lines = [f'Table "{qualified(schema, table)}" {{']
    for col_name, info in columns_info.items():
        attrs = []
        if col_name in pk_cols:
            attrs.append("pk")
        if not info["nullable"]:
            attrs.append("not null")
        attr_str = f" [{', '.join(attrs)}]" if attrs else ""
        lines.append(f"  {col_name} {info['type']}{attr_str}")
    notes = []
    if is_base:
        notes.append("base table")
    if is_view:
        notes.append("VIEW")
    if notes:
        lines.append(f"  Note: '{', '.join(notes)}'")
    lines.append("}")
    return "\n".join(lines)


def generate_dbml(base_schema, base_table, tables_data, views_data, relationships):
    sections = []

    sections.append(
        f"Project {{\n"
        f"  database_type: 'MSSQL'\n"
        f"  Note: 'Base table: {qualified(base_schema, base_table)}'\n"
        f"}}"
    )

    for (schema, table), (columns_info, pk_cols, is_base) in tables_data.items():
        sections.append(dbml_table_block(schema, table, columns_info, pk_cols, is_base=is_base))

    for (schema, view), (columns_info, pk_cols) in views_data.items():
        sections.append(dbml_table_block(schema, view, columns_info, pk_cols, is_view=True))

    for rel in relationships:
        from_q = qualified(rel["from_schema"], rel["from_table"])
        to_q = qualified(rel["to_schema"], rel["to_table"])
        sections.append(f'Ref: "{from_q}".{rel["from_col"]} > "{to_q}".{rel["to_col"]}')

    return "\n\n".join(sections) + "\n"


# ── main diagram builder ───────────────────────────────────────────────────────

def build_diagram(cursor, base_schema, base_table, depth=1, all_columns=False):
    # BFS: expand FK neighbours up to `depth` levels
    visited = {(base_schema, base_table)}
    frontier = {(base_schema, base_table)}
    all_fks: list[dict] = []
    seen_fk_names: set[str] = set()

    for _ in range(depth):
        next_frontier: set[tuple] = set()
        for (s, t) in frontier:
            for r in fetch_fk_relationships(cursor, s, t):
                if r["fk_name"] not in seen_fk_names:
                    seen_fk_names.add(r["fk_name"])
                    all_fks.append(r)
                for neighbor in ((r["from_schema"], r["from_table"]), (r["to_schema"], r["to_table"])):
                    if neighbor not in visited:
                        next_frontier.add(neighbor)
                        visited.add(neighbor)
        frontier = next_frontier
        if not frontier:
            break

    diagram_table_keys = visited

    # Deduplicate by column-level tuple — the DB may have multiple FK constraints
    # with different names mapping the same column pair
    seen_col_rels: set[tuple] = set()
    diagram_fks = []
    for r in all_fks:
        if (r["from_schema"], r["from_table"]) not in diagram_table_keys:
            continue
        if (r["to_schema"], r["to_table"]) not in diagram_table_keys:
            continue
        col_key = (r["from_schema"], r["from_table"], r["from_col"], r["to_schema"], r["to_table"], r["to_col"])
        if col_key not in seen_col_rels:
            seen_col_rels.add(col_key)
            diagram_fks.append(r)

    # Collect PK columns (always needed for annotations)
    pk_map: dict[tuple, set] = {}
    for key in diagram_table_keys:
        pk_map[key] = fetch_pk_columns(cursor, *key)

    tables_data = {}
    if all_columns:
        for key in diagram_table_keys:
            col_info = fetch_all_column_info(cursor, *key)
            is_base = key == (base_schema, base_table)
            tables_data[key] = (col_info, pk_map[key], is_base)
    else:
        # Keys-only: include PK columns + columns involved in FK relationships
        needed_cols: dict[tuple, set] = {k: set() for k in diagram_table_keys}
        for r in diagram_fks:
            needed_cols[(r["from_schema"], r["from_table"])].add(r["from_col"])
            needed_cols[(r["to_schema"], r["to_table"])].add(r["to_col"])
        for key in diagram_table_keys:
            needed_cols[key].update(pk_map[key])

        for key in diagram_table_keys:
            cols = sorted(needed_cols[key])
            col_info = fetch_column_info(cursor, *key, cols) if cols else {}
            is_base = key == (base_schema, base_table)
            tables_data[key] = (col_info, pk_map[key], is_base)

    if not INCLUDE_VIEWS:
        return tables_data, {}, diagram_fks

    view_keys = fetch_dependent_views(cursor, base_schema, base_table)
    known_col_names: set[str] = set()
    for col_info, _, _ in tables_data.values():
        known_col_names.update(col_info.keys())

    views_data = {}
    for vkey in view_keys:
        v_schema, v_name = vkey
        if all_columns:
            col_info = fetch_all_column_info(cursor, v_schema, v_name)
        else:
            matching_cols = sorted(known_col_names)
            col_info = fetch_column_info(cursor, v_schema, v_name, matching_cols) if matching_cols else {}
        if col_info:
            views_data[(v_schema, v_name)] = (col_info, set())

    return tables_data, views_data, diagram_fks


def run():
    parser = argparse.ArgumentParser(description="Generate DBML diagrams from an MSSQL schema.")
    parser.add_argument(
        "--all-columns",
        action="store_true",
        default=False,
        help="Include all columns in each table (default: keys and FK columns only)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        metavar="N",
        help="How many FK relationship levels to traverse (default: 1)",
    )
    args = parser.parse_args()

    if args.depth < 1:
        parser.error("--depth must be at least 1")

    base_tables = load_base_tables()
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"Connecting to {os.environ['MSSQL_SERVER']} / {os.environ['MSSQL_DATABASE']} ...")
    print(f"Options: depth={args.depth}, all_columns={args.all_columns}")
    conn = get_connection()
    cursor = conn.cursor()

    for table_id, schema, table in base_tables:
        print(f"\nProcessing {schema}.{table} ...", end=" ", flush=True)
        try:
            tables_data, views_data, fk_rows = build_diagram(
                cursor, schema, table, depth=args.depth, all_columns=args.all_columns
            )
            dbml = generate_dbml(schema, table, tables_data, views_data, fk_rows)
            out_path = OUTPUT_DIR / f"{table_id}-{schema}-{table}.dbml"
            out_path.write_text(dbml, encoding="utf-8")
            print(
                f"done — {len(tables_data)} table(s), "
                f"{len(views_data)} view(s), "
                f"{len(fk_rows)} relationship(s) → {out_path}"
            )
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)

    cursor.close()
    conn.close()
    print("\nAll diagrams written to output/")


if __name__ == "__main__":
    run()
