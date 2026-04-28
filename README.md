# MSSQL → DrawDB Diagram Generator

Generates DrawDB-compatible `.dbml` files from a Microsoft SQL Server database. DrawDB's native MSSQL DDL import is broken, so this tool queries the live schema directly and produces DBML instead.

Three modes of operation:

| Mode | How | Output |
|------|-----|--------|
| Default | tables listed in `tables.csv` | one `.dbml` per table |
| `--schema` | all tables in a named schema | one combined `.dbml` |
| `--schema --table` | a single named table | one `.dbml` |

## Setup

### 1. Install Microsoft ODBC Driver 18

```bash
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 3. Configure credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

### 4. List your tables (default mode only)

Edit `tables.csv` with your base tables — one row per table:

```
ID,Schema,Table Name
1,dbo,Orders
2,dbo,Customers
```

If you're unsure about case, run `fix_table_names.py` first, or use `--quiet` to auto-correct at runtime (see below).

## Docker

A pre-built image is published to GitHub Container Registry on every push to `main`:

```
ghcr.io/ontologuy/mssql-dbml:latest
```

All CLI flags work identically inside the container. Mount a host directory to receive the output files, and pass credentials via `--env-file`.

### Default mode (tables.csv)

```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/tables.csv:/app/tables.csv \
  -v $(pwd)/diagrams:/output \
  ghcr.io/ontologuy/mssql-dbml \
  -d /output
```

### Schema mode

```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/diagrams:/output \
  ghcr.io/ontologuy/mssql-dbml \
  -s dbo -d /output
```

### Single-table mode

```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/diagrams:/output \
  ghcr.io/ontologuy/mssql-dbml \
  -s dbo -t Orders -d /output
```

### Build the image locally

```bash
docker build -t mssql-dbml .
docker run --rm --env-file .env -v $(pwd)/diagrams:/output mssql-dbml -s dbo -d /output
```

## Usage

### Default mode — tables.csv

```bash
# All columns, 1 hop deep (default)
.venv/bin/python3 generate_diagrams.py

# FK/PK columns only
.venv/bin/python3 generate_diagrams.py --keys-only

# Follow FK relationships 3 levels deep
.venv/bin/python3 generate_diagrams.py --depth 3

# FK/PK columns only, 2 hops, auto-correct any case mismatches
.venv/bin/python3 generate_diagrams.py --keys-only --depth 2 --quiet
```

Output files are named `<id>-<schema>-<table>.dbml` (e.g. `1-dbo-Orders.dbml`).

### Schema mode — combined diagram

```bash
# One combined diagram for all tables in the dbo schema
.venv/bin/python3 generate_diagrams.py -s dbo

# Same, but follow FK relationships 3 hops deep
.venv/bin/python3 generate_diagrams.py -s dbo --depth 3

# Exclude FK relationships that cross into other schemas
.venv/bin/python3 generate_diagrams.py -s dbo -o

# FK/PK columns only, schema-only FKs
.venv/bin/python3 generate_diagrams.py -s dbo -o --keys-only

# Auto-correct case mismatches in the schema name
.venv/bin/python3 generate_diagrams.py -s DBO -q
```

Output is a single file named `<schema>.dbml` (e.g. `dbo.dbml`).

### Single-table mode

```bash
# Diagram for dbo.Orders and its FK neighbours
.venv/bin/python3 generate_diagrams.py -s dbo -t Orders

# Same, FK/PK columns only, 2 hops
.venv/bin/python3 generate_diagrams.py -s dbo -t Orders --keys-only --depth 2

# Auto-correct if the table name has wrong case
.venv/bin/python3 generate_diagrams.py -s dbo -t orders -q
```

Output is a single file named `<schema>-<table>.dbml` (e.g. `dbo-Orders.dbml`).

### Specifying an output folder

```bash
# Write to a specific folder (created if it doesn't exist)
.venv/bin/python3 generate_diagrams.py -d /path/to/my-diagrams

# Schema mode with custom output dir
.venv/bin/python3 generate_diagrams.py -s dbo -d /path/to/my-diagrams
```

By default, output goes to a new subdirectory under `MSSQL2DBML/` in the current working directory, named by today's date with an incrementing suffix — e.g. `MSSQL2DBML/2026-04-24-0001`. Each run creates the next available suffix so previous runs are never overwritten. The folder is only created if at least one file is successfully written.

### Case mismatch handling

If a schema or table name is not found exactly, the tool searches case-insensitively and prompts:

```
'Dbo.TaBle' cannot be found, however 'dbo.table' exists.
Generate diagram for 'dbo.table'? (Y)es/(N)o/(A)ll:
```

- **(Y)es** — use the corrected name for this item
- **(N)o** — skip this item
- **(A)ll** — accept all remaining corrections automatically for this run

Use `--quiet` / `-q` to skip the prompt entirely: unambiguous mismatches are corrected automatically and logged; items with no match or multiple matches are skipped and logged as errors.

## Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--keys-only` | `-k` | Include only FK and PK columns (default: all columns) |
| `--include-views` | `-v` | Include views that depend on the base table(s) |
| `--depth N` | | FK hops to follow from each base table (default: 1) |
| `--schema SCHEMA` | `-s` | Generate a combined diagram for all tables in SCHEMA |
| `--table TABLE` | `-t` | Generate a diagram for a single TABLE; requires `--schema` |
| `--complete-schema-only` | `-o` | With `--schema`, exclude cross-schema FK relationships |
| `--output DIR` | `-d` | Write output to DIR (created if absent) |
| `--quiet` | `-q` | Auto-correct case mismatches; skip ambiguous/missing items |

## Files

| File | Purpose |
|------|---------|
| `generate_diagrams.py` | Main script |
| `fix_table_names.py` | Standalone utility to correct case mismatches in `tables.csv` against the live database |
| `tables.csv` | List of base tables to diagram (used in default mode) |
| `.env` | DB credentials (not committed) |
| `.env.example` | Credentials template |
| `requirements.txt` | Python dependencies |
| `Dockerfile` | Container image definition |
| `.github/workflows/docker-publish.yml` | Publishes image to GHCR on push to `main` |
| `output/` | Generated DBML files (not committed) |
