#!/usr/bin/env python3
import sys
import csv
from dbfread import DBF, FieldParser

class SafeFieldParser(FieldParser):
    def parseD(self, field, data):
        try:
            return super().parseD(field, data)
        except Exception:
            return None

    def parseN(self, field, data):
        try:
            return super().parseN(field, data)
        except Exception:
            return 0

    def parseF(self, field, data):
        try:
            return super().parseF(field, data)
        except Exception:
            return 0.0

def convert(dbf_path, csv_path):
    try:
       table = DBF(
        dbf_path,
        encoding='latin1',
        ignore_missing_memofile=True,
        parserclass=SafeFieldParser,
        ignore_deletion_flag=False  # This will EXCLUDE deleted records
    )

        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(table.field_names)

            for record in table:
                try:
                    clean_values = []
                    for value in record.values():
                        if isinstance(value, bytes):
                            try:
                                clean_values.append(value.decode('latin1').strip())
                            except:
                                clean_values.append("")
                        else:
                            clean_values.append(value)
                    writer.writerow(clean_values)
                except Exception as row_error:
                    print(f"⚠️ Skipping bad record in {dbf_path}: {row_error}")
                    continue
    except Exception as e:
        print(f"❌ Conversion failed explicitly for {dbf_path}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_dbf_to_csv.py input.dbf output.csv")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
