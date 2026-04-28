import argparse
import csv
import os
import sys
from datetime import date
from pathlib import Path

import pyodbc
from dotenv import load_dotenv


def resolve_default_output_dir():
    base = Path("MSSQL2DBML")
    today = date.today().strftime("%Y-%m-%d")
    suffix = 1
    while True:
        candidate = base / f"{today}-{suffix:04d}"
        if not candidate.exists():
            return candidate
        suffix += 1


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

SCHEMA_TABLES_QUERY = """
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ?
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME
"""

TABLE_EXISTS_QUERY = """
SELECT 1
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  AND TABLE_TYPE = 'BASE TABLE'
"""

TABLE_CASE_SEARCH_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
  AND TABLE_TYPE = 'BASE TABLE'
"""

SCHEMA_EXISTS_QUERY = """
SELECT 1
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ?
  AND TABLE_TYPE = 'BASE TABLE'
"""

SCHEMA_CASE_SEARCH_QUERY = """
SELECT DISTINCT TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLES
WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
  AND TABLE_TYPE = 'BASE TABLE'
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


def fetch_schema_tables(cursor, schema):
    cursor.execute(SCHEMA_TABLES_QUERY, schema)
    return [r.TABLE_NAME for r in cursor.fetchall()]


def table_exists(cursor, schema, table):
    cursor.execute(TABLE_EXISTS_QUERY, schema, table)
    return cursor.fetchone() is not None


def schema_exists(cursor, schema):
    cursor.execute(SCHEMA_EXISTS_QUERY, schema)
    return cursor.fetchone() is not None


def find_table_ci(cursor, schema, table):
    cursor.execute(TABLE_CASE_SEARCH_QUERY, schema, table)
    return [(r.TABLE_SCHEMA, r.TABLE_NAME) for r in cursor.fetchall()]


def find_schema_ci(cursor, schema):
    cursor.execute(SCHEMA_CASE_SEARCH_QUERY, schema)
    return [r.TABLE_SCHEMA for r in cursor.fetchall()]


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


def generate_dbml(base_schema, base_table, tables_data, views_data, relationships, project_note=None):
    sections = []

    note = project_note if project_note is not None else f"Base table: {qualified(base_schema, base_table)}"
    sections.append(
        f"Project {{\n"
        f"  database_type: 'MSSQL'\n"
        f"  Note: '{note}'\n"
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

def build_diagram(cursor, base_schema, base_table, depth=1, all_columns=True, restrict_schema=None, include_views=False):
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
                        if restrict_schema is None or neighbor[0] == restrict_schema:
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

    if not include_views:
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


def build_schema_diagram(cursor, schema, depth=1, all_columns=True, restrict_schema=None, include_views=False):
    schema_table_names = fetch_schema_tables(cursor, schema)
    base_keys = {(schema, t) for t in schema_table_names}
    visited = set(base_keys)
    frontier = set(base_keys)
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
                        if restrict_schema is None or neighbor[0] == restrict_schema:
                            next_frontier.add(neighbor)
                            visited.add(neighbor)
        frontier = next_frontier
        if not frontier:
            break

    diagram_table_keys = visited

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

    pk_map: dict[tuple, set] = {}
    for key in diagram_table_keys:
        pk_map[key] = fetch_pk_columns(cursor, *key)

    tables_data = {}
    if all_columns:
        for key in diagram_table_keys:
            col_info = fetch_all_column_info(cursor, *key)
            tables_data[key] = (col_info, pk_map[key], key in base_keys)
    else:
        needed_cols: dict[tuple, set] = {k: set() for k in diagram_table_keys}
        for r in diagram_fks:
            needed_cols[(r["from_schema"], r["from_table"])].add(r["from_col"])
            needed_cols[(r["to_schema"], r["to_table"])].add(r["to_col"])
        for key in diagram_table_keys:
            needed_cols[key].update(pk_map[key])

        for key in diagram_table_keys:
            cols = sorted(needed_cols[key])
            col_info = fetch_column_info(cursor, *key, cols) if cols else {}
            tables_data[key] = (col_info, pk_map[key], key in base_keys)

    if not include_views:
        return tables_data, {}, diagram_fks

    views_data = {}
    known_col_names: set[str] = set()
    for col_info, _, _ in tables_data.values():
        known_col_names.update(col_info.keys())

    for (s, t) in base_keys:
        for vkey in fetch_dependent_views(cursor, s, t):
            if vkey in views_data:
                continue
            v_schema, v_name = vkey
            if all_columns:
                col_info = fetch_all_column_info(cursor, v_schema, v_name)
            else:
                matching_cols = sorted(known_col_names)
                col_info = fetch_column_info(cursor, v_schema, v_name, matching_cols) if matching_cols else {}
            if col_info:
                views_data[vkey] = (col_info, set())

    return tables_data, views_data, diagram_fks


def resolve_schema_name(cursor, schema, quiet, accept_all):
    """Return (resolved_schema, accept_all) or (None, accept_all) if unresolvable/rejected."""
    matches = find_schema_ci(cursor, schema)
    if not matches:
        print(f"ERROR: schema '{schema}' not found.", file=sys.stderr)
        return None, accept_all

    if schema in matches:
        return schema, accept_all

    if len(matches) > 1:
        if quiet:
            print(f"ERROR: schema '{schema}' is ambiguous — matches: {', '.join(matches)}", file=sys.stderr)
            return None, accept_all
        print(f"Schema '{schema}' not found. Multiple case-insensitive matches:")
        for i, m in enumerate(matches, 1):
            print(f"  {i}. {m}")
        while True:
            ans = input("Enter number to select, or N to skip: ").strip()
            if ans.upper() == "N":
                return None, accept_all
            try:
                idx = int(ans) - 1
                if 0 <= idx < len(matches):
                    return matches[idx], accept_all
            except ValueError:
                pass
            print("Invalid input, try again.")

    resolved = matches[0]
    if quiet or accept_all:
        print(f"Note: schema '{schema}' → '{resolved}' (case corrected)")
        return resolved, accept_all

    while True:
        ans = input(
            f"Schema '{schema}' cannot be found, however '{resolved}' exists. "
            f"Use '{resolved}'? (Y)es/(N)o/(A)ll: "
        ).strip().upper()
        if ans in ("Y", "YES"):
            return resolved, accept_all
        if ans in ("N", "NO"):
            return None, accept_all
        if ans in ("A", "ALL"):
            return resolved, True
        print("Please enter Y, N, or A.")


def resolve_table_name(cursor, schema, table, quiet, accept_all):
    """Return ((resolved_schema, resolved_table), accept_all) or (None, accept_all)."""
    given = f"{schema}.{table}"

    matches = find_table_ci(cursor, schema, table)
    if not matches:
        print(f"ERROR: '{given}' not found.", file=sys.stderr)
        return None, accept_all

    if (schema, table) in matches:
        return (schema, table), accept_all

    if len(matches) > 1:
        if quiet:
            names = ", ".join(f"{s}.{t}" for s, t in matches)
            print(f"ERROR: '{given}' is ambiguous — matches: {names}", file=sys.stderr)
            return None, accept_all
        print(f"'{given}' not found. Multiple case-insensitive matches:")
        for i, (ms, mt) in enumerate(matches, 1):
            print(f"  {i}. {ms}.{mt}")
        while True:
            ans = input("Enter number to select, or N to skip: ").strip()
            if ans.upper() == "N":
                return None, accept_all
            try:
                idx = int(ans) - 1
                if 0 <= idx < len(matches):
                    return matches[idx], accept_all
            except ValueError:
                pass
            print("Invalid input, try again.")

    resolved_schema, resolved_table = matches[0]
    resolved = f"{resolved_schema}.{resolved_table}"

    if quiet or accept_all:
        print(f"Note: '{given}' → '{resolved}' (case corrected)")
        return (resolved_schema, resolved_table), accept_all

    while True:
        ans = input(
            f"'{given}' cannot be found, however '{resolved}' exists. "
            f"Generate diagram for '{resolved}'? (Y)es/(N)o/(A)ll: "
        ).strip().upper()
        if ans in ("Y", "YES"):
            return (resolved_schema, resolved_table), accept_all
        if ans in ("N", "NO"):
            return None, accept_all
        if ans in ("A", "ALL"):
            return (resolved_schema, resolved_table), True
        print("Please enter Y, N, or A.")


def run():
    parser = argparse.ArgumentParser(
        prog="generate_diagrams.py",
        description=(
            "Generate DrawDB-compatible .dbml files from a Microsoft SQL Server database.\n"
            "\n"
            "Three modes of operation:\n"
            "  (default)          one .dbml per table listed in tables.csv\n"
            "  --schema           one combined .dbml for all tables in a schema\n"
            "  --schema --table   one .dbml for a single named table"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  %(prog)s                              use tables.csv, default options\n"
            "  %(prog)s -k --depth 3                 FK/PK columns only, follow 3 FK hops\n"
            "  %(prog)s -v                           include dependent views\n"
            "  %(prog)s -s dbo                       combined diagram for dbo schema\n"
            "  %(prog)s -s dbo -o                    dbo schema, no cross-schema FKs\n"
            "  %(prog)s -s dbo -o -k                 same, FK/PK columns only\n"
            "  %(prog)s -s dbo -t Orders             single-table diagram for dbo.Orders\n"
            "  %(prog)s -s dbo -q                    auto-correct case mismatches\n"
            "  %(prog)s -d ./my-diagrams             write output to ./my-diagrams/\n"
            "  %(prog)s -s dbo -d ./my-diagrams -q   schema mode, custom dir, quiet"
        ),
    )
    parser.add_argument(
        "--keys-only", "-k",
        action="store_true",
        default=False,
        help="include only FK and PK columns in each table (default: all columns)",
    )
    parser.add_argument(
        "--include-views", "-v",
        action="store_true",
        default=False,
        help="include views that depend on the base table(s)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        metavar="N",
        help="FK hops to follow from each base table (default: 1)",
    )
    parser.add_argument(
        "--schema", "-s",
        metavar="SCHEMA",
        help="generate a combined diagram for all tables in SCHEMA",
    )
    parser.add_argument(
        "--table", "-t",
        metavar="TABLE",
        help="generate a diagram for a single TABLE; requires --schema / -s",
    )
    parser.add_argument(
        "--complete-schema-only", "-o",
        action="store_true",
        default=False,
        help="with --schema, exclude FK relationships that cross into other schemas",
    )
    parser.add_argument(
        "--output", "-d",
        metavar="DIR",
        default=None,
        help="write output to DIR (created if absent; default: MSSQL2DBML/YYYY-MM-DD-NNNN)",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        default=False,
        help=(
            "auto-correct unambiguous case mismatches without prompting; "
            "skip items with no match or multiple matches and log as errors"
        ),
    )
    args = parser.parse_args()

    if args.depth < 1:
        parser.error("--depth must be at least 1")
    if args.complete_schema_only and not args.schema:
        parser.error("-o / --complete-schema-only requires -s / --schema")
    if args.table and not args.schema:
        parser.error("-t / --table requires -s / --schema")
    if args.table and args.complete_schema_only:
        parser.error("-t / --table and -o / --complete-schema-only cannot be used together")

    output_dir = Path(args.output) if args.output else resolve_default_output_dir()
    quiet = args.quiet
    accept_all = False

    load_dotenv()
    missing = [v for v in ("MSSQL_SERVER", "MSSQL_DATABASE", "MSSQL_USERNAME", "MSSQL_PASSWORD")
               if not os.environ.get(v)]
    if missing:
        parser.error(f"missing required environment variable(s): {', '.join(missing)}")

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ['MSSQL_SERVER']};"
        f"DATABASE={os.environ['MSSQL_DATABASE']};"
        f"UID={os.environ['MSSQL_USERNAME']};"
        f"PWD={os.environ['MSSQL_PASSWORD']};"
        "TrustServerCertificate=yes;"
    )

    print(f"Connecting to {os.environ['MSSQL_SERVER']} / {os.environ['MSSQL_DATABASE']} ...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    print(f"Options: depth={args.depth}, keys_only={args.keys_only}, "
          f"include_views={args.include_views}, complete_schema_only={args.complete_schema_only}, quiet={quiet}")

    if args.table:
        # ── single-table mode ──────────────────────────────────────────────────
        result, _ = resolve_table_name(cursor, args.schema, args.table, quiet, accept_all)
        if result is None:
            cursor.close()
            conn.close()
            sys.exit(1)
        schema, table = result
        print(f"\nProcessing {schema}.{table} ...", end=" ", flush=True)
        try:
            tables_data, views_data, fk_rows = build_diagram(
                cursor, schema, table, depth=args.depth, all_columns=not args.keys_only,
                include_views=args.include_views,
            )
            dbml = generate_dbml(schema, table, tables_data, views_data, fk_rows)
            output_dir.mkdir(parents=True, exist_ok=True)
            out_path = output_dir / f"{schema}-{table}.dbml"
            out_path.write_text(dbml, encoding="utf-8")
            print(
                f"done — {len(tables_data)} table(s), "
                f"{len(views_data)} view(s), "
                f"{len(fk_rows)} relationship(s) → {out_path}"
            )
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)

    elif args.schema:
        # ── schema mode ───────────────────────────────────────────────────────
        resolved_schema, _ = resolve_schema_name(cursor, args.schema, quiet, accept_all)
        if resolved_schema is None:
            cursor.close()
            conn.close()
            sys.exit(1)
        restrict_schema = resolved_schema if args.complete_schema_only else None
        print(f"\nBuilding combined diagram for schema '{resolved_schema}' ...", end=" ", flush=True)
        try:
            tables_data, views_data, fk_rows = build_schema_diagram(
                cursor, resolved_schema, depth=args.depth, all_columns=not args.keys_only,
                restrict_schema=restrict_schema, include_views=args.include_views,
            )
            dbml = generate_dbml(
                resolved_schema, None, tables_data, views_data, fk_rows,
                project_note=f"Schema: {resolved_schema}",
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            out_path = output_dir / f"{resolved_schema}.dbml"
            out_path.write_text(dbml, encoding="utf-8")
            print(
                f"done — {len(tables_data)} table(s), "
                f"{len(views_data)} view(s), "
                f"{len(fk_rows)} relationship(s) → {out_path}"
            )
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)

    else:
        # ── CSV mode ──────────────────────────────────────────────────────────
        base_tables = load_base_tables()
        for table_id, schema, table in base_tables:
            result, accept_all = resolve_table_name(cursor, schema, table, quiet, accept_all)
            if result is None:
                continue
            schema, table = result
            print(f"\nProcessing {schema}.{table} ...", end=" ", flush=True)
            try:
                tables_data, views_data, fk_rows = build_diagram(
                    cursor, schema, table, depth=args.depth, all_columns=not args.keys_only,
                    include_views=args.include_views,
                )
                dbml = generate_dbml(schema, table, tables_data, views_data, fk_rows)
                output_dir.mkdir(parents=True, exist_ok=True)
                out_path = output_dir / f"{table_id}-{schema}-{table}.dbml"
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
    print(f"\nAll diagrams written to {output_dir}/")


if __name__ == "__main__":
    run()
