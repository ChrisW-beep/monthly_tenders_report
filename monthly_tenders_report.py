#!/usr/bin/env python3
import os
import csv
import sqlite3
from dbfread import DBF

SQLITE_DB = "temp_jnl.db"
EXTRACTED_ROOT = "/tmp/extracted"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"


def read_store_name_from_strdbf(str_dbf_path):
    if not os.path.isfile(str_dbf_path):
        return "UnknownStore"

    table = DBF(str_dbf_path, load=True)
    for record in table:
        name_val = record.get("NAME", None)
        if name_val:
            return str(name_val)
        break
    return "UnknownStore"


def find_case_insensitive(folder, filename):
    for f in os.listdir(folder):
        if f.lower() == filename.lower():
            return os.path.join(folder, f)
    return None


def import_jnl_to_sqlite(jnl_dbf_path, sqlite_db):
    if not os.path.isfile(jnl_dbf_path):
        return 0

    table = DBF(jnl_dbf_path, load=True)
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS jnl_data;")
    cur.execute("""
        CREATE TABLE jnl_data (
            row_num INTEGER PRIMARY KEY AUTOINCREMENT,
            LINE TEXT,
            PRICE TEXT,
            DATE TEXT,
            DESCRIPT TEXT
        );
    """)

    insert_sql = "INSERT INTO jnl_data (LINE, PRICE, DATE, DESCRIPT) VALUES (?, ?, ?, ?);"

    row_count = 0
    for record in table:
        cur.execute(
            insert_sql,
            (
                str(record.get("LINE", "")),
                str(record.get("PRICE", "")),
                str(record.get("DATE", "")),
                str(record.get("DESCRIPT", "")),
            ),
        )
        row_count += 1

    conn.commit()
    conn.close()
    return row_count


def generate_report(prefix, store_name, sqlite_db, csv_writer):
    conn = sqlite3.connect(sqlite_db)
    cur = conn.cursor()

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

    for date_val, type_val, amount_val, count_val in rows:
        row = [
            prefix,
            store_name,
            date_val,
            type_val,
            amount_val,
            count_val,
            "USD",
        ]
        csv_writer.writerow(row)

    conn.close()


def process_prefix(prefix_dir, prefix, csv_writer):
    data_folder = find_case_insensitive(prefix_dir, "Data")
    if not data_folder:
        print(f"Skipping {prefix}: Data folder not found")
        return

    jnl_path = find_case_insensitive(data_folder, "jnl.dbf")
    if not jnl_path:
        print(f"Skipping {prefix}: jnl.dbf not found")
        return

    str_path = find_case_insensitive(data_folder, "str.dbf")
    store_name = read_store_name_from_strdbf(str_path) if str_path else "UnknownStore"

    imported = import_jnl_to_sqlite(jnl_path, SQLITE_DB)
    if imported == 0:
        print(f"Skipping {prefix}: no data imported")
        return

    generate_report(prefix, store_name, SQLITE_DB, csv_writer)

    os.remove(SQLITE_DB)


def main():
    prefixes = [
        d for d in os.listdir(EXTRACTED_ROOT) if os.path.isdir(os.path.join(EXTRACTED_ROOT, d))
    ]

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            ["Astoreid", "Storename", "date", "Type", "sale_amount", "sale_count", "currency"]
        )

        for prefix in prefixes:
            prefix_dir = os.path.join(EXTRACTED_ROOT, prefix)
            print(f"Processing prefix: {prefix}")
            process_prefix(prefix_dir, prefix, csv_writer)

    print(f"Report successfully written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
