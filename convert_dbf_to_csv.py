#!/usr/bin/env python3
import sys
import csv
from dbfread import DBF


def convert(dbf_path, csv_path):
    table = DBF(dbf_path, encoding="latin1", ignore_missing_memofile=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(table.field_names)
        for record in table:
            writer.writerow(list(record.values()))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_dbf_to_csv.py input.dbf output.csv")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
