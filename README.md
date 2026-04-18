# MSSQL → DrawDB Diagram Generator

Generates `.dbml` files from a Microsoft SQL Server database for import into [DrawDB](https://drawdb.app). DrawDB's native MSSQL DDL import is broken, so this tool generates DBML format instead.

For each table listed in `tables.csv`, the script discovers all FK relationships (inbound and outbound), related tables, and writes a `.dbml` file containing only the columns that participate in those relationships. Tables with 500+ columns are kept readable this way.

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

## Usage

Optionally fix any case mismatches between your CSV and the database:

```bash
.venv/bin/python3 fix_table_names.py
```

Then generate the diagrams:

```bash
.venv/bin/python3 generate_diagrams.py
```

Output `.dbml` files are written to `output/` named `<id>-<schema>-<table>.dbml`. Import each into DrawDB via **Import → DBML**.

## Configuration

At the top of `generate_diagrams.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `INCLUDE_VIEWS` | `False` | Set to `True` to include dependent views in diagrams |

## Files

| File | Purpose |
|------|---------|
| `generate_diagrams.py` | Main script |
| `fix_table_names.py` | Corrects case mismatches in tables.csv against the live database |
| `tables.csv` | List of base tables to diagram |
| `.env` | DB credentials (not committed) |
| `.env.example` | Credentials template |
| `requirements.txt` | Python dependencies |
| `output/` | Generated DBML files (not committed) |
