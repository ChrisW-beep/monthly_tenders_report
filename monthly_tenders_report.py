#!/usr/bin/env python3
import os
import sys
import csv
import sqlite3

from dbfread import DBF

# Default values
DEFAULT_EXTRACTED_ROOT = "tmp/extracted"
DEFAULT_PREFIX = "6045"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"


def read_store_name_from_strdbf(str_dbf_path):
    """
    Reads str.dbf (if it exists), returns the store name from the first row's 'NAME' field.
    Otherwise returns a fallback like 'UnknownStore'.
    """
    if not os.path.isfile(str_dbf_path):
        print(f"Warning: Missing str.dbf: {str_dbf_path}")
        return "UnknownStore"
    try:
        table = DBF(str_dbf_path, load=True)
        for record in table:
            name_val = record.get("NAME", None)
            if name_val:
                return str(name_val)
            break  # Only check the first row
    except Exception as e:
        print(f"Error reading {str_dbf_path}: {e}")
    return "UnknownStore"


def find_case_insensitive(folder, filename):
    """
    Searches for a file (or folder) in the given folder, case-insensitively.
    """
    try:
        for f in os.listdir(folder):
            if f.lower() == filename.lower():
                return os.path.join(folder, f)
    except Exception as e:
        print(f"Error listing {folder}: {e}")
    return None


def import_jnl_to_sqlite(jnl_dbf_path, sqlite_db):
    """
    Reads jnl.dbf (using lazy loading) and inserts its rows into a SQLite table named 'jnl_data'.
    """
    if not os.path.isfile(jnl_dbf_path):
        print(f"Error: jnl.dbf not found: {jnl_dbf_path}")
        return 0
    try:
        table = DBF(jnl_dbf_path, load=False)
    except Exception as e:
        print(f"Error opening DBF file {jnl_dbf_path}: {e}")
        return 0

    try:
        conn = sqlite3.connect(sqlite_db)
        cur = conn.cursor()
        cur.execute("PRAGMA temp_store = MEMORY;")
        cur.execute("PRAGMA cache_size = 5000;")
        cur.execute("DROP TABLE IF EXISTS jnl_data;")
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
        print(
            f"Imported {row_count} rows from {jnl_dbf_path} into {sqlite_db}: jnl_data"
        )
        return row_count
    except Exception as e:
        print(f"Database error: {e}")
        return 0


def generate_report_rows(prefix, store_name, sqlite_db):
    """
    Executes the report query on the imported jnl_data and returns a list of rows.
    Each row includes: Astoreid, Storename, date, Type, sale_amount, sale_count, currency.
    """
    rows = []
    try:
        conn = sqlite3.connect(sqlite_db)
        cur = conn.cursor()
        sql = """
            WITH sales AS (
    SELECT
        a.DATE AS date,
        b.DESCRIPT AS Type,
        (CAST(a.PRICE AS REAL) - CAST(c.PRICE AS REAL)) AS sale_amount,
        1 AS sale_count
    FROM jnl_data a
    INNER JOIN jnl_data b
        ON b.row_num = a.row_num + 1
        AND b.LINE = '980'
    INNER JOIN jnl_data c
        ON c.row_num = a.row_num - 1
        AND c.LINE = '941'
    WHERE a.LINE = '950'
)

SELECT
    date,
    Type,
    SUM(sale_amount) AS sale_amount,
    SUM(sale_count) AS sale_count
FROM sales
GROUP BY date, Type
ORDER BY date, Type;

        """
        cur.execute(sql)
        for row in cur.fetchall():
            # Each row: (date, Type, sale_amount, sale_count)
            rows.append([prefix, store_name, row[0], row[1], row[2], row[3], "USD"])
        conn.close()
    except Exception as e:
        print(f"Error generating report rows for prefix {prefix}: {e}")
    return rows


def process_prefix(parent_path, prefix):
    """
    Processes a single prefix folder under extracted_root.
    Looks for the 'data' subfolder, reads DBF files, imports data, and returns report rows.
    """
    data_folder = find_case_insensitive(parent_path, "data")
    if not data_folder:
        print(f"Skipping prefix {prefix}: no 'data' folder found in {parent_path}")
        return []

    # Find required DBF files in the data folder
    jnl_path = find_case_insensitive(data_folder, "jnl.dbf")
    if not jnl_path:
        print(f"Skipping prefix {prefix}: jnl.dbf not found in {data_folder}")
        return []
    str_path = find_case_insensitive(data_folder, "str.dbf")
    store_name = read_store_name_from_strdbf(str_path) if str_path else "UnknownStore"

    # Use a temporary SQLite DB file unique to this prefix
    sqlite_db = f"temp_jnl_{prefix}.db"
    imported = import_jnl_to_sqlite(jnl_path, sqlite_db)
    if imported == 0:
        print(f"Skipping prefix {prefix}: no data imported from {jnl_path}")
        if os.path.exists(sqlite_db):
            os.remove(sqlite_db)
        return []

    rows = generate_report_rows(prefix, store_name, sqlite_db)

    if os.path.exists(sqlite_db):
        try:
            os.remove(sqlite_db)
        except Exception as e:
            print(f"Error removing temporary database {sqlite_db}: {e}")

    return rows


def main():
    # Determine the extracted root folder from the first argument (or use default)
    if len(sys.argv) >= 2:
        extracted_root = sys.argv[1]
    else:
        extracted_root = DEFAULT_EXTRACTED_ROOT

    # Optionally, if a specific prefix is provided as second argument, process only that prefix.
    if len(sys.argv) >= 3:
        prefixes = [sys.argv[2]]
    else:
        # Process all directories (assumed to be prefixes) under extracted_root.
        try:
            prefixes = sorted(
                [
                    d
                    for d in os.listdir(extracted_root)
                    if os.path.isdir(os.path.join(extracted_root, d))
                ]
            )
        except Exception as e:
            print(f"Error listing directories in {extracted_root}: {e}")
            sys.exit(1)

    print(f"Processing prefixes: {prefixes}")
    combined_rows = []

    for prefix in prefixes:
        parent_path = os.path.join(extracted_root, prefix)
        print(f"Processing prefix '{prefix}' in folder: {parent_path}")
        rows = process_prefix(parent_path, prefix)
        if rows:
            combined_rows.extend(rows)
        else:
            print(f"No report data for prefix '{prefix}'.")

    # Write the combined rows to the final CSV report.
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    try:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
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
            writer.writerows(combined_rows)
        print(f"Wrote final combined CSV report to {OUTPUT_CSV}")
    except Exception as e:
        print(f"Error writing CSV report: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
