# FRED to Snowflake Ingestion

One-time pipeline to ingest Federal Reserve Economic Data (FRED) into Snowflake. Fetches **30-Year Fixed Mortgage Rate** (MORTGAGE30US) and **Effective Federal Funds Rate** (FEDFUNDS).

## Prerequisites

1. **FRED API Key** – Free at [fredaccount.stlouisfed.org](https://fredaccount.stlouisfed.org)
2. **Snowflake account** – With a warehouse available
3. **Credentials** – Account identifier, username, password

## Setup

```bash
# Create virtual environment (optional)
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# Install dependencies (app + ingest)
pip install -r requirements.txt -r requirements-ingest.txt

# Copy template and add your keys (the script loads from .env)
cp .env.example .env
# Edit .env with your FRED_API_KEY and Snowflake credentials
```

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `FRED_API_KEY` | Yes | FRED API key |
| `SNOWFLAKE_ACCOUNT` | Yes | Account locator, full identifier (e.g. `lm74771.eu-west-2.aws`), or full URL from Snowsight |
| `SNOWFLAKE_REGION` | If account has no region | Cloud region (e.g. `us-east-1`). Required when using locator-only account. |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | No | Warehouse (default: `COMPUTE_WH`) |
| `SNOWFLAKE_DATABASE` | No | Database (default: `FRED`) |
| `SNOWFLAKE_SCHEMA` | No | Schema (default: `PUBLIC`) |
| `SNOWFLAKE_TABLE` | No | Table name (default: `fred_observations`) |

## Run

```bash
python3 ingest.py
```

The script will:

1. Fetch MORTGAGE30US and FEDFUNDS from the FRED API
2. Create the database and schema if they do not exist
3. Load observations into `{database}.{schema}.fred_observations`
4. Truncate and replace the table on each run

## Snowflake Schema

| Column | Type | Description |
|--------|------|-------------|
| series_id | STRING | e.g., MORTGAGE30US, FEDFUNDS |
| date | DATE | Observation date |
| value | FLOAT | Numeric value |
| created_at | TIMESTAMP | Load timestamp |

## Account identifier and region

Snowflake requires the account identifier to include the region for many accounts. Use either:

- **Full format**: `SNOWFLAKE_ACCOUNT=xy12345.us-east-1`
- **Locator + region**: `SNOWFLAKE_ACCOUNT=YG93571` and `SNOWFLAKE_REGION=us-east-1`

Find your region in Snowsight: **Admin** > **Accounts** > Account URL. A 404 on login usually means the region is missing.

For connection options and troubleshooting, see the [Snowflake Python Connector documentation](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect).

---

## Streamlit App (Mortgage Portfolio Stability Index)

The `app.py` dashboard federates FRED data (from Snowflake) with BurstBank product profile via Starburst Galaxy, comparing officer mortgage rates to the live 30-year market rate.

### Prerequisites

- Starburst Galaxy account with Snowflake and BurstBank sample catalogs configured
- FRED data loaded to Snowflake (run `ingest.py` first)

### Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `STARBURST_HOST` | Yes | Galaxy host (e.g. `bustbankdemo.northeurope.starburst.io`) |
| `STARBURST_USER` | Yes | Username with role (e.g. `user@burstbank.com/accountadmin`) |
| `STARBURST_PASSWORD` | Yes | Starburst password |

### Run locally

```bash
streamlit run app.py
```

### Deploy to Streamlit Cloud

`requirements.txt` pins **Streamlit ≥1.40** and **PyStarburst ≥0.10** so the resolver does not fall back to PyStarburst 0.7 (which pulled Streamlit 1.19 and broke on newer Python). PyStarburst 0.11 supports Python 3.10–3.13; on **Python 3.14**, pip installs **0.10.x** instead.

If installs still fail on the default runtime, use **Advanced settings** → **Python 3.12** or **3.13**, then redeploy.
