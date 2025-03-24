#!/usr/bin/env python3
import os
import csv
import pyarrow
import pandas as pd

EXTRACTED_ROOT = "/tmp/extracted"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"

def process_prefix(prefix, data_folder, csv_writer):
    jnl_csv = os.path.join(data_folder, "jnl.csv")
    str_csv = os.path.join(data_folder, "str.csv")

    if not os.path.exists(jnl_csv):
        raise FileNotFoundError(f"{prefix}: jnl.csv not found")

    store_name = "UnknownStore"
    if os.path.exists(str_csv):
        df_str = pd.read_csv(str_csv)
        if 'NAME' in df_str.columns and not df_str.empty:
            store_name = df_str.iloc[0]['NAME']

    df_jnl = pd.read_csv(jnl_csv, dtype=str).fillna("")

    # Add shifted LINE and DESCRIPT to detect consecutive 950->980 rows
    df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
    df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)

    df_filtered = df_jnl[
        (df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980")
    ]

    df_filtered["PRICE"] = pd.to_numeric(df_filtered["PRICE"], errors="coerce").fillna(0)
    report = (
        df_filtered.groupby(["DATE", "DESCRIPT_next"])
        .agg(sale_amount=("PRICE", "sum"), sale_count=("PRICE", "count"))
        .reset_index()
    )

    for _, row in report.iterrows():
        csv_writer.writerow(
            [
                prefix,
                store_name,
                row["DATE"],
                row["DESCRIPT_next"],
                row["sale_amount"],
                row["sale_count"],
                "USD",
            ]
        )

def main():
    prefixes = [d for d in os.listdir(EXTRACTED_ROOT) if os.path.isdir(os.path.join(EXTRACTED_ROOT, d))]

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            ["Astoreid", "Storename", "date", "Type", "sale_amount", "sale_count", "currency"]
        )

        for prefix in prefixes:
            data_folder = os.path.join(EXTRACTED_ROOT, prefix, "Data")
            print(f"Processing prefix: {prefix}", flush=True)
            process_prefix(prefix, data_folder, csv_writer)

    print(f"Report written to {OUTPUT_CSV}", flush=True)

if __name__ == "__main__":
    main()
