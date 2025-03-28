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
    merchant_id = "UNKNOWN"
    ini_path = find_csv_case_insensitive(data_folder, "spirits.ini")
    if ini_path and os.path.isfile(ini_path):
        with open(ini_path) as f:
            for line in f:
                if line.strip().startswith("DCMERCHANTID="):
                    merchant_id = line.strip().split("=", 1)[1]
                    break

    if str_csv:
        df_str = pd.read_csv(str_csv)
        if 'NAME' in df_str.columns and not df_str.empty:
            store_name = df_str.iloc[0]['NAME']

    df_jnl = pd.read_csv(jnl_csv, dtype=str).fillna("")
    df_jnl["PRICE"] = pd.to_numeric(df_jnl["PRICE"], errors="coerce").fillna(0)

    # --- Filter for the previous month ---
    import datetime
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    prev_month_last_day = first_of_this_month - datetime.timedelta(days=1)
    prev_month = prev_month_last_day.month
    prev_year = prev_month_last_day.year

    df_jnl["DATE_parsed"] = pd.to_datetime(df_jnl["DATE"], errors="coerce")
    df_jnl = df_jnl[
        (df_jnl["DATE_parsed"].dt.year == prev_year) &
        (df_jnl["DATE_parsed"].dt.month == prev_month)
    ]
    # ---------------------------------------

    df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
    df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)
    df_jnl["LINE_prev"] = df_jnl["LINE"].shift(1)
    df_jnl["PRICE_prev"] = df_jnl["PRICE"].shift(1)

    df_filtered = df_jnl[
        (df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980")
    ].copy()

    
    df_filtered["adj_PRICE"] = df_filtered["PRICE"]
    

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
                merchant_id  # New column
            ]
        )




def main():
    prefixes = [d for d in os.listdir(EXTRACTED_ROOT) if os.path.isdir(os.path.join(EXTRACTED_ROOT, d))]

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            ["Astoreid", "Storename", "date", "Type", "sale_amount", "sale_count", "currency","merchant_id"]
        )

        for prefix in prefixes:
            data_folder = os.path.join(EXTRACTED_ROOT, prefix, "Data")
            print(f"Processing prefix: {prefix}", flush=True)
            process_prefix(prefix, data_folder, csv_writer)

    print(f"Report written to {OUTPUT_CSV}", flush=True)

if __name__ == "__main__":
    main()
