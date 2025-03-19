#!/usr/bin/env python3
import os
import csv
import sqlite3
from dbfread import DBF

# Adjust these paths as needed
PREFIX = "6045"  # e.g. your store prefix => Astoreid
STR_DBF_PATH = "/tmp/extracted/6045/Data/str.dbf"
JNL_DBF_PATH = "/tmp/extracted/6045/Data/jnl.dbf"
SQLITE_DB = "temp_jnl.db"
OUTPUT_CSV = "./reports/monthly_sales_report.csv"


def read_store_name_from_strdbf(str_dbf_path):
    """
    Reads str.dbf (if it exists), returns the store name from the first row's 'NAME' field.
    Otherwise returns a fallback like 'UnknownStore'.
    """
    if not os.path.isfile(str_dbf_path):
        print(f"Warning: Missing str.dbf: {str_dbf_path}")
        return "UnknownStore"

    table = DBF(str_dbf_path, load=True)
    for record in table:
        # If there's a 'NAME' field, return it as the store name
        name_val = record.get("NAME", None)
        if name_val:
            return str(name_val)
        break  # only read the first row
    return "UnknownStore"


def import_jnl_to_sqlite(jnl_dbf_path, sqlite_db):
    """
    Reads jnl.dbf using dbfread, inserts rows into a SQLite table 'jnl_data'.
    We'll store an auto-increment row_num so we can detect consecutive lines (950->980).
    """
    if not os.path.isfile(jnl_dbf_path):
        print(f"Error: jnl.dbf not found: {jnl_dbf_path}")
        return 0

    # Read the DBF
    table = DBF(jnl_dbf_path, load=True)

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()

    # Drop old table if exists
    cur.execute("DROP TABLE IF EXISTS jnl_data;")

    # Create table with minimal columns
    create_sql = """
        CREATE TABLE jnl_data (
            row_num INTEGER PRIMARY KEY AUTOINCREMENT,
            LINE TEXT,
            PRICE TEXT,
            DATE TEXT,
            DESCRIPT TEXT
        );
    """
    cur.execute(create_sql)

    # Insert
    insert_sql = """
        INSERT INTO jnl_data (LINE, PRICE, DATE, DESCRIPT)
        VALUES (?, ?, ?, ?);
    """

    row_count = 0
    for record in table:
        line_val = str(record.get("LINE", ""))
        price_val = str(record.get("PRICE", ""))
        date_val = str(record.get("DATE", ""))
        descript_val = str(record.get("DESCRIPT", ""))
        cur.execute(insert_sql, (line_val, price_val, date_val, descript_val))
        row_count += 1

    conn.commit()
    conn.close()
    print(f"Imported {row_count} rows from {jnl_dbf_path} into {sqlite_db}:jnl_data")
    return row_count


def generate_report(prefix, store_name, sqlite_db, output_csv):
    """
    1) Self-join jnl_data on row_num+1 to find (LINE=950)->(LINE=980).
    2) Group by (a.DATE, b.DESCRIPT) => sum PRICE, count occurrences.
    3) Write final CSV with columns: Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    """
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()

    # We'll interpret PRICE as a float. a=950 row, b=980 row => consecutive lines.
    sql = """
    SELECT
        a.DATE as date,
        b.DESCRIPT as Type,
        SUM(CAST(a.PRICE as REAL)) as sale_amount,
        COUNT(*) as sale_count
    FROM jnl_data a
    JOIN jnl_data b
      ON b.row_num = a.row_num + 1
     AND b.LINE='980'
    WHERE a.LINE='950'
    GROUP BY a.DATE, b.DESCRIPT
    ORDER BY a.DATE, b.DESCRIPT;
    """
    cur.execute(sql)
    rows = cur.fetchall()

    # We'll produce the final CSV with columns:
    # Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Astoreid",
                "Storename",
                "date",
                "Type",
                "sale_amount",
                "sale_count",
                "currency",
            ]
        )
        for date_val, type_val, amount_val, count_val in rows:
            row = [
                prefix,  # Astoreid
                store_name,  # Storename
                date_val,  # date
                type_val,  # Type
                amount_val,  # sale_amount
                count_val,  # sale_count
                "USD",  # currency
            ]
            writer.writerow(row)

    conn.close()
    print(f"Wrote final CSV to {output_csv}")


def main():
    # 1) Read store name from str.dbf
    store_name = read_store_name_from_strdbf(STR_DBF_PATH)

    # 2) Import jnl.dbf => SQLite
    imported = import_jnl_to_sqlite(JNL_DBF_PATH, SQLITE_DB)
    if imported == 0:
        print("No jnl data imported, aborting.")
        return

    # 3) Generate final aggregated report => CSV
    generate_report(PREFIX, store_name, SQLITE_DB, OUTPUT_CSV)

    # 4) (Optional) remove the temp DB
    # os.remove(SQLITE_DB)


if __name__ == "__main__":
    main()
