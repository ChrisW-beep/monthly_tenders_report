#!/usr/bin/env python3
import os
import csv
import pyarrow
import pandas as pd

EXTRACTED_ROOT = "/tmp/extracted"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"

import os
import pandas as pd

def find_csv_case_insensitive(directory, filename):
    filename_lower = filename.lower()
    for f in os.listdir(directory):
        if f.lower() == filename_lower:
            return os.path.join(directory, f)
    return None

def process_prefix(prefix, data_folder, csv_writer):
    jnl_csv = find_csv_case_insensitive(data_folder, "jnl.csv")
    str_csv = find_csv_case_insensitive(data_folder, "str.csv")

    if not jnl_csv:
        raise FileNotFoundError(f"{prefix}: jnl.csv not found")

    store_name = "UnknownStore"
    if str_csv:
        df_str = pd.read_csv(str_csv)
        if 'NAME' in df_str.columns and not df_str.empty:
            store_name = df_str.iloc[0]['NAME']

    df_jnl = pd.read_csv(jnl_csv, dtype=str).fillna("")
    # Convert PRICE column to numeric
    df_jnl["PRICE"] = pd.to_numeric(df_jnl["PRICE"], errors="coerce").fillna(0)
    
    # Create shifted columns for next and previous lines
    df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
    df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)
    df_jnl["LINE_prev"] = df_jnl["LINE"].shift(1)
    df_jnl["PRICE_prev"] = df_jnl["PRICE"].shift(1)

    # Filter for rows where current line is 950 and next line is 980
    df_filtered = df_jnl[(df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980")].copy()
    
    # Subtract the PRICE of the previous row if it is from line 941
    df_filtered["adj_PRICE"] = df_filtered["PRICE"] - df_filtered["PRICE_prev"].where(df_filtered["LINE_prev"] == "941", 0)
    
    # Aggregate using the adjusted price
    report = (
        df_filtered.groupby(["DATE", "DESCRIPT_next"])
        .agg(sale_amount=("adj_PRICE", "sum"), sale_count=("adj_PRICE", "count"))
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
