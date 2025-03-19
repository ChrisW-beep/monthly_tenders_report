#!/usr/bin/env python3
import os
import csv
import sqlite3
from dbfread import DBF

# Adjust these paths as needed
PREFIX = "6045"  # e.g. your store prefix => Astoreid
# STR_DBF_PATH = "/tmp/extracted/6045/data/str.dbf"
# JNL_DBF_PATH = "/tmp/extracted/6045/data/jnl.dbf"
SQLITE_DB = "temp_jnl.db"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"


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


def find_case_insensitive(folder, filename):
    # e.g. filename="jnl.dbf" => look for any case version
    for f in os.listdir(folder):
        if f.lower() == filename.lower():
            return os.path.join(folder, f)
    return None


def import_jnl_to_sqlite(jnl_dbf_path, sqlite_db):
    """
    Reads jnl.dbf using dbfread, inserts rows into a SQLite table 'jnl_data'.
    We'll store an auto-increment row_num so we can detect consecutive lines (950->980).
    """
    if not os.path.isfile(jnl_dbf_path):
        print(f"Error: jnl.dbf not found: {jnl_dbf_path}")
        return 0

    # Read the DBF
    table = DBF(jnl_dbf_path, load=False)

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("PRAGMA cache_size = 5000;")


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
    prefix = PREFIX  # Use the global PREFIX variable
    parent_path = f"/tmp/extracted/{prefix}"

    # 1) Find "Data" subfolder ignoring case
    data_folder = find_case_insensitive(
        parent_path, "data"
    )  # Fix incorrect function call
    if not data_folder:
        print(f"Error: no 'data' subfolder found (ignoring case) in {parent_path}")
        return

    # 2) Find jnl.dbf inside that subfolder
    jnl_path = find_case_insensitive(data_folder, "jnl.dbf")
    if not jnl_path:
        print("No jnl data found, aborting.")
        return

    # 3) Find str.dbf (for store name)
    str_path = find_case_insensitive(data_folder, "str.dbf")
    store_name = read_store_name_from_strdbf(str_path) if str_path else "UnknownStore"

    # 4) Import jnl.dbf into SQLite
    imported = import_jnl_to_sqlite(jnl_path, SQLITE_DB)
    if imported == 0:
        print("No jnl data imported, aborting.")
        return

    # 5) Generate final aggregated report => CSV
    generate_report(prefix, store_name, SQLITE_DB, OUTPUT_CSV)

    # 6) (Optional) remove the temp DB
    os.remove(SQLITE_DB)  # Fix indentation


if __name__ == "__main__":
    main()
