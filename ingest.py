#!/usr/bin/env python3
"""
One-time ingestion of FRED economic data (30-Year Mortgage, Fed Funds Rate)
into Snowflake.

Connection: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
"""

import os
from datetime import datetime

import pandas as pd
import pyfredapi as pf
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# Series to ingest
SERIES_IDS = ["MORTGAGE30US", "FEDFUNDS"]

# Snowflake config (override via env)
DEFAULT_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DEFAULT_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FRED")
DEFAULT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
DEFAULT_TABLE = os.getenv("SNOWFLAKE_TABLE", "fred_observations")


def _sanitize_account(account: str) -> str:
    """Extract account identifier from URL or raw value. Snowflake expects no https:// or .snowflakecomputing.com."""
    if not account:
        return account
    s = account.strip().lower()
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix) :]
            break
    for suffix in (".snowflakecomputing.com", ":443"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s.strip("/")


def fetch_series(series_id: str) -> pd.DataFrame:
    """Fetch observations for a FRED series."""
    df = pf.get_series(series_id=series_id)
    df = df[["date", "value"]].copy()
    df["series_id"] = series_id
    return df[["series_id", "date", "value"]]


def main():
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise SystemExit("ERROR: Set FRED_API_KEY environment variable")

    account = _sanitize_account(os.getenv("SNOWFLAKE_ACCOUNT") or "")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not all([account, user, password]):
        raise SystemExit(
            "ERROR: Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
        )

    # Snowflake often requires account.region (e.g. xy12345.us-east-1)
    region = _sanitize_account(os.getenv("SNOWFLAKE_REGION") or "")
    if region and "." not in account:
        account = f"{account}.{region}"

    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE)
    database = os.getenv("SNOWFLAKE_DATABASE", DEFAULT_DATABASE)
    schema = os.getenv("SNOWFLAKE_SCHEMA", DEFAULT_SCHEMA)
    table = os.getenv("SNOWFLAKE_TABLE", DEFAULT_TABLE)

    # Fetch all series
    dfs = []
    for sid in SERIES_IDS:
        print(f"Fetching {sid}...")
        df = fetch_series(sid)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Ensure date is date type, value is float
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined["value"] = pd.to_numeric(combined["value"], errors="coerce")
    combined = combined.dropna(subset=["value"])

    # Add audit column
    combined["created_at"] = datetime.utcnow()

    print(f"Loaded {len(combined)} observations into DataFrame")

    # Connect to Snowflake (omit database/schema in case they don't exist)
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
        )
    except Exception as e:
        err = str(e)
        if "404" in err and "login" in err.lower():
            raise SystemExit(
                "ERROR: Snowflake login 404. Ensure SNOWFLAKE_ACCOUNT includes region, "
                "e.g. YG93571.us-east-1 or use SNOWFLAKE_REGION=us-east-1 (or your region). "
                "Find your account URL in Snowsight: Admin > Accounts."
            ) from e
        raise

    try:
        cursor = conn.cursor()

        # Create database and schema if they don't exist
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
        cursor.execute(f'USE DATABASE "{database}"')
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cursor.execute(f'USE SCHEMA "{schema}"')

        table_id = f"{database}.{schema}.{table}"
        print(f"Loading to {table_id}...")

        success, num_chunks, num_rows, _ = write_pandas(
            conn,
            combined,
            table,
            database=database,
            schema=schema,
            overwrite=True,
            auto_create_table=True,
        )

        if success:
            print(f"Done. Loaded {num_rows} rows to {table_id}")
        else:
            raise SystemExit("ERROR: write_pandas failed")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
