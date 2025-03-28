#!/usr/bin/env python3
import sys
import csv
from dbfread import DBF, FieldParser
from datetime import date

class SafeFieldParser(FieldParser):
    def parseD(self, field, data):
        try:
            return super().parseD(field, data)
        except Exception:
            return None  # or "" or "1900-01-01" as fallback

def is_junk_row(record):
    values = list(record.values())
    return all(str(v).strip() in ["", "0", "0.0"] for v in values)

def convert(dbf_path, csv_path):
    try:
        table = DBF(
            dbf_path,
            encoding='latin1',
            ignore_missing_memofile=True,
            parserclass=SafeFieldParser
        )

        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(table.field_names)
            for record in table:
                if is_junk_row(record):
                    continue
                try:
                    writer.writerow(list(record.values()))
                except Exception as row_error:
                    print(f"⚠️ Skipping bad record: {row_error}")
                    continue
    except Exception as e:
        print(f"❌ Conversion failed explicitly: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_dbf_to_csv.py input.dbf output.csv")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
