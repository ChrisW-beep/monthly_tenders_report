#!/usr/bin/env python3
import sys
import csv
from dbfread import DBF

def is_junk_row(record):
    values = list(record.values())
    return all(str(v).strip() in ["", "0", "0.0"] for v in values)

def convert(dbf_path, csv_path):
    try:
        table = DBF(dbf_path, encoding='latin1', ignore_missing_memofile=True)
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(table.field_names)
            for record in table:
                if is_junk_row(record):
                    continue  # Skip junk rows
                writer.writerow(list(record.values()))
    except Exception as e:
        print(f"Conversion failed explicitly: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_dbf_to_csv.py input.dbf output.csv")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
