#!/usr/bin/env python3
import os
import csv
import boto3
import io
import pandas as pd
from configparser import ConfigParser
from datetime import datetime, timedelta

BUCKET_NAME = "spiritsbackups"
PREFIX_BASE = "processed_csvs/"
OUTPUT_CSV = "./reports/monthly_tenders_report.csv"

s3 = boto3.client("s3")

def stream_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), dtype=str).fillna("")

def extract_dcmerchantid(prefix):
    ini_key = f"{PREFIX_BASE}{prefix}/spirits.ini"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=ini_key)
        content = obj["Body"].read().decode("utf-8")
        for line in content.splitlines():
            if line.startswith("DCMERCHANTID="):
                value = line.split("=", 1)[1].strip()
                return f'"{value}"'  # ✅ wrap in quotes to preserve as string
    except s3.exceptions.NoSuchKey:
        return '"N/A"'
    return '"N/A"'

def extract_dcprocessor(prefix):
    ini_key = f"{PREFIX_BASE}{prefix}/spirits.ini"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=ini_key)
        content = obj["Body"].read().decode("utf-8")
        for line in content.splitlines():
            if line.startswith("DCPROCESSOR="):
                value = line.split("=", 1)[1].strip()
                return f'"{value}"'  # ✅ wrap in quotes to preserve as string
    except s3.exceptions.NoSuchKey:
        return '"N/A"'
    return '"N/A"'

def extract_cardinterface(prefix):
    ini_key = f"{PREFIX_BASE}{prefix}/spirits.ini"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=ini_key)
        content = obj["Body"].read().decode("utf-8")
        for line in content.splitlines():
            if line.lower().startswith("cardinterface="):
                value = line.split("=", 1)[1].strip()
                return f'"{value}"'
    except s3.exceptions.NoSuchKey:
        return '"N/A"'
    return '"N/A"'



def process_prefix(prefix, csv_writer):
    try:
        print(f"🔄 Starting prefix {prefix}", flush=True)

        jnl_key = f"{PREFIX_BASE}{prefix}/jnl.csv"
        str_key = f"{PREFIX_BASE}{prefix}/str.csv"

        print("📥 Streaming jnl.csv from S3...", flush=True)
        df_jnl = stream_csv_from_s3(jnl_key)
        df_jnl["PRICE"] = pd.to_numeric(df_jnl["PRICE"], errors="coerce").fillna(0)

        store_name = "UnknownStore"
        try:
            print("📥 Streaming str.csv from S3...", flush=True)
            df_str = stream_csv_from_s3(str_key)
            if "NAME" in df_str.columns and not df_str.empty:
                store_name = df_str.iloc[0]["NAME"]
        except Exception:
            print("⚠️ Could not load store name", flush=True)

        today = datetime.today()
        prev_month_last_day = today.replace(day=1) - timedelta(days=1)
        prev_month = prev_month_last_day.month
        prev_year = prev_month_last_day.year

        print("📅 Filtering records for previous month...", flush=True)
        df_jnl["DATE_parsed"] = pd.to_datetime(df_jnl["DATE"], errors="coerce")
        df_jnl = df_jnl[
            (df_jnl["DATE_parsed"].dt.year == prev_year) &
            (df_jnl["DATE_parsed"].dt.month == prev_month)
        ]

        df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
        df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)

        print("📊 Filtering for LINE 950 + LINE 980 pairs...", flush=True)
        df_filtered = df_jnl[
            (df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980")
        ].copy()

        df_filtered["adj_PRICE"] = df_filtered["PRICE"]
        df_filtered["is_return"] = df_filtered["adj_PRICE"] < 0

        # Classify sale and reversal amounts
        df_filtered["sale_amount"] = df_filtered.apply(lambda row: row["adj_PRICE"] if not row["is_return"] else 0, axis=1)
        df_filtered["sale_count"] = df_filtered.apply(lambda row: 1 if not row["is_return"] else 0, axis=1)
        df_filtered["reversal_amount"] = df_filtered.apply(lambda row: abs(row["adj_PRICE"]) if row["is_return"] else 0, axis=1)
        df_filtered["reversal_count"] = df_filtered.apply(lambda row: 1 if row["is_return"] else 0, axis=1)

        print("📈 Grouping by DATE and DESCRIPT_next...", flush=True)
        report = (
            df_filtered.groupby(["DATE", "DESCRIPT_next"])
            .agg(
                sale_amount=("sale_amount", "sum"),
                sale_count=("sale_count", "sum"),
                reversal_amount=("reversal_amount", "sum"),
                reversal_count=("reversal_count", "sum")
            )
            .reset_index()
        )

        print("🔍 Extracting INI values...", flush=True)
        merchant_id = extract_dcmerchantid(prefix)
        ccprocessor = extract_dcprocessor(prefix)
        cardinterface = extract_cardinterface(prefix)

        print(f"📝 Writing rows for {prefix}...", flush=True)
        for _, row in report.iterrows():
            csv_writer.writerow([
                prefix,
                store_name,
                merchant_id,
                ccprocessor,
                row["DATE"],
                row["DESCRIPT_next"],
                row["sale_amount"],
                row["sale_count"],
                row["reversal_amount"],
                row["reversal_count"],
                cardinterface,
                "USD",
            ])

        print(f"✅ Finished prefix {prefix}", flush=True)

    except Exception as e:
        print(f"❌ Failed to process {prefix}: {e}", flush=True)


    
def main():
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX_BASE, Delimiter="/")

    prefixes = []
    for page in result:
        if "CommonPrefixes" in page:
            for p in page["CommonPrefixes"]:
                prefix = p["Prefix"].split("/")[-2]
                prefixes.append(prefix)

    failed_prefixes = []

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([
            "Astoreid", "Storename", "MerchantID", "CCProcessor", "date", "Type",
            "sale_amount", "sale_count", "reversal_amount", "reversal_count", "CardInterface", "currency"
        ])

        for prefix in sorted(prefixes):
            print(f"▶️ Processing {prefix}", flush=True)
            try:
                process_prefix(prefix, csv_writer)
            except Exception as e:
                failed_prefixes.append(prefix)
                print(f"❌ Failed to process {prefix}: {e}", flush=True)

    # Write failure log
    if failed_prefixes:
        fail_log_path = "./reports/failed_prefixes.log"
        with open(fail_log_path, "w") as fail_log:
            for prefix in failed_prefixes:
                fail_log.write(f"{prefix}\n")
        print(f"🚨 {len(failed_prefixes)} prefixes failed. See {fail_log_path}", flush=True)
    else:
        print("✅ All prefixes processed successfully.", flush=True)

    print(f"✅ Report written to {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
