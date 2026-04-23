# MSSQL → DrawDB Diagram Generator

Generates `.dbml` files from a Microsoft SQL Server database for import into [DrawDB](https://drawdb.app). DrawDB's native MSSQL DDL import is broken, so this tool queries the live schema directly and produces DBML instead.

For each table listed in `tables.csv`, the script discovers FK relationships (inbound and outbound) and writes a `.dbml` file per table. Two flags control what gets included:

- **Column mode** — either just the columns that participate in PKs/FKs (default, keeps large tables readable), or every column in each table (`--all-columns`)
- **Traversal depth** — how many FK hops to follow outward from the base table (`--depth N`, default: 1)

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

### 4. List your tables

Edit `tables.csv` with your base tables — one row per table:

```
ID,Schema,Table Name
1,dbo,Orders
2,dbo,Customers
```

If you're unsure about case, run `fix_table_names.py` first (see below).

## Usage

```bash
# Keys/FK columns only, 1 level deep (original behavior)
.venv/bin/python3 generate_diagrams.py

# Include every column in each table
.venv/bin/python3 generate_diagrams.py --all-columns

# Follow FK relationships 3 levels deep
.venv/bin/python3 generate_diagrams.py --depth 3

# Both flags combined
.venv/bin/python3 generate_diagrams.py --all-columns --depth 2
```

Output `.dbml` files are written to `output/` named `<id>-<schema>-<table>.dbml`. Import each into DrawDB via **Import → DBML**.

To correct case mismatches between `tables.csv` and the live database before generating:

```bash
.venv/bin/python3 fix_table_names.py
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `INCLUDE_VIEWS` | `False` | Set to `True` in `generate_diagrams.py` to include views that depend on the base table |

## Files

| File | Purpose |
|------|---------|
| `generate_diagrams.py` | Main script |
| `fix_table_names.py` | Corrects case mismatches in `tables.csv` against the live database |
| `tables.csv` | List of base tables to diagram |
| `.env` | DB credentials (not committed) |
| `.env.example` | Credentials template |
| `requirements.txt` | Python dependencies |
| `output/` | Generated DBML files (not committed) |
