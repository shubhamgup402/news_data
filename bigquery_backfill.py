#!/usr/bin/env python3
"""
bigquery_backfill.py
Query GDELT BigQuery gkg_partitioned (daily partitions) for a keyword/date range,
save daily CSVs and upload to a Google Cloud Storage bucket.

Notes:
 - Uses service account JSON via GOOGLE_APPLICATION_CREDENTIALS env (the GitHub Action sets this).
 - Uses daily partitioning to reduce query scan size.
"""

import os
import argparse
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import pandas as pd
from tqdm import tqdm
import tempfile
import sys
import time

def make_client(project):
    return bigquery.Client(project=project)

def upload_file_to_gcs(storage_client, bucket_name, local_path, dest_path):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{dest_path}")

def query_day(client, keyword, day_str, location="US", timeout=600):
    """
    Query a single day partition. Returns a pandas DataFrame.
    """
    kw_param = f"%{keyword.lower()}%"
    sql = """
    SELECT
      DocumentIdentifier AS url,
      _PARTITIONTIME AS partition_time,
      DATE(_PARTITIONTIME) AS date,
      V2Tone,
      V2Themes,
      V2Locations,
      AllNames,
      V2Persons,
      V2Organizations,
      SourceCommonName,
      TranslationInfo
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE DATE(_PARTITIONTIME) = @day
      AND (
         LOWER(IFNULL(DocumentIdentifier,'')) LIKE @kw
      OR LOWER(IFNULL(AllNames,'')) LIKE @kw
      OR LOWER(IFNULL(V2Organizations,'')) LIKE @kw
      OR LOWER(IFNULL(V2Persons,'')) LIKE @kw
      OR LOWER(IFNULL(V2Themes,'')) LIKE @kw
      )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("day", "DATE", day_str),
            bigquery.ScalarQueryParameter("kw", "STRING", kw_param),
        ],
        use_query_cache=True
    )
    q = client.query(sql, job_config=job_config, location=location)
    q = q.result(timeout=timeout)
    try:
        df = q.to_dataframe()
    except Exception:
        # fallback: stream rows manually
        rows = list(q)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
    return df

def backfill(keyword, start_date, end_date, project, bucket_name, max_days_per_run=7, location="US", sleep_seconds=1):
    # clients
    bq = make_client(project)
    storage_client = storage.Client(project=project)

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    cur = start
    days_processed = 0
    total_days = (end - start).days + 1
    print(f"Backfill plan: {total_days} days from {start} -> {end}. This run will process up to {max_days_per_run} days.")

    pbar = tqdm(total=min(total_days, max_days_per_run), desc="days")
    while cur <= end and days_processed < max_days_per_run:
        day_str = cur.strftime("%Y-%m-%d")
        print(f"\nQuerying {day_str} for '{keyword}' ...")
        try:
            df = query_day(bq, keyword, day_str, location=location)
        except Exception as e:
            print(f"Query failed for {day_str}: {e}", file=sys.stderr)
            df = pd.DataFrame()
        if df.empty:
            print(f"No results for {day_str}.")
        else:
            # save and upload
            with tempfile.TemporaryDirectory() as tmpdir:
                fname = f"{keyword.replace(' ','_')}_{day_str}.csv"
                local_path = f"{tmpdir}/{fname}"
                df.to_csv(local_path, index=False)
                dest_path = f"gdelt/{keyword.replace(' ','_')}/{day_str}.csv"
                upload_file_to_gcs(storage_client, bucket_name, local_path, dest_path)
                print(f"Saved {len(df)} rows for {day_str}.")
        cur = cur + timedelta(days=1)
        days_processed += 1
        pbar.update(1)
        time.sleep(sleep_seconds)  # polite pause
    pbar.close()
    print(f"Run finished. Days processed: {days_processed}. Next start date would be {cur} if you resume.")
    return cur.strftime("%Y-%m-%d")  # next day to resume from

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", required=True, help="Search keyword (e.g., 'Reliance' or 'Reliance Industries')")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD (e.g., 2016-12-01)")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD (e.g., 2025-08-16)")
    parser.add_argument("--project", required=True, help="GCP project id (for billing & clients)")
    parser.add_argument("--bucket", required=True, help="GCS bucket to upload CSVs")
    parser.add_argument("--max-days", type=int, default=7, help="Max number of days to process in this run (default 7)")
    parser.add_argument("--location", default="US", help="BigQuery location (default US)")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds sleep between day-queries")
    args = parser.parse_args()

    # GOOGLE_APPLICATION_CREDENTIALS must point to the service account JSON in environment (set by GitHub Action)
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS env var not set. Provide SA JSON file and set this env var.", file=sys.stderr)
        sys.exit(1)

    next_resume = backfill(
        keyword=args.keyword,
        start_date=args.start,
        end_date=args.end,
        project=args.project,
        bucket_name=args.bucket,
        max_days_per_run=args.max_days,
        location=args.location,
        sleep_seconds=args.sleep
    )

    print(f"Next resume-from date (if you want to continue): {next_resume}")

if __name__ == "__main__":
    main()
