#!/usr/bin/env python3
import sys
import csv
import decimal
from dbfread import DBF, FieldParser

from dbfread import DBF, FieldParser

class SafeFieldParser(FieldParser):
    def parseD(self, field, data):  # Date field
        try:
            return super().parseD(field, data)
        except Exception:
            return None

    def parseN(self, field, data):  # Numeric field
        try:
            return super().parseN(field, data)
        except Exception:
            return None

    def parseF(self, field, data):  # Float field
        try:
            return super().parseF(field, data)
        except Exception:
            return None

    def parseL(self, field, data):  # Logical field (e.g., VOIDED)
        try:
            return super().parseL(field, data)
        except Exception:
            return None  # Skip malformed logical


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

            written = 0
            skipped = 0

            for record in table:
                try:
                    if record.get("_deleted", False):
                        skipped += 1
                        continue

                    row_values = []
                    for value in record.values():
                        try:
                            if isinstance(value, bytes):
                                value = value.decode('latin1', errors='ignore').strip()
                            elif isinstance(value, decimal.Decimal):
                                value = float(value)
                        except:
                            value = ""
                        row_values.append(value)

                    writer.writerow(row_values)
                    written += 1

                except Exception as row_err:
                    print(f"⚠️ Skipping bad record in {dbf_path}: {row_err}", flush=True)
                    skipped += 1
                    continue

            print(f"✅ Finished {dbf_path}: written={written}, skipped={skipped}", flush=True)

    except Exception as e:
        print(f"❌ Conversion failed explicitly for {dbf_path}: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_dbf_to_csv.py input.dbf output.csv")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
