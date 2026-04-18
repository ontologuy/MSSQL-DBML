"""
Checks each table in tables.csv against the database and corrects case mismatches.
Only updates entries where the name matches case-insensitively but differs in case.
"""
import csv
import os
from pathlib import Path

from dotenv import load_dotenv
import pyodbc

load_dotenv()

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.environ['MSSQL_SERVER']};"
    f"DATABASE={os.environ['MSSQL_DATABASE']};"
    f"UID={os.environ['MSSQL_USERNAME']};"
    f"PWD={os.environ['MSSQL_PASSWORD']};"
    "TrustServerCertificate=yes;"
)

CSV_PATH = Path("tables.csv")

LOOKUP_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
"""


def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    schema_col = next(k for k in headers if "schema" in k.lower())
    table_col = next(k for k in headers if "table" in k.lower())

    changes = 0
    for row in rows:
        schema = row[schema_col].strip()
        table = row[table_col].strip()
        cursor.execute(LOOKUP_QUERY, schema, table)
        result = cursor.fetchone()
        if result:
            db_schema, db_table = result.TABLE_SCHEMA, result.TABLE_NAME
            if db_schema != schema or db_table != table:
                print(f"  Fix: {schema}.{table} → {db_schema}.{db_table}")
                row[schema_col] = db_schema
                row[table_col] = db_table
                changes += 1
        else:
            print(f"  WARN: {schema}.{table} not found in database")

    cursor.close()
    conn.close()

    if changes:
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n{changes} correction(s) written to {CSV_PATH}")
    else:
        print("No case corrections needed.")


if __name__ == "__main__":
    main()
