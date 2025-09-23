This repo ingests daily OHLCV market data from Alpha Vantage into your Supabase tables using Python.

## Prerequisites
- Supabase project with `instruments` and `ohlcv_data` tables
- Alpha Vantage API key
- Python 3.10+
- Linux/macOS shell (for examples below)

## Quick Start

### 1) Clone the repository
```bash
git clone https://github.com/your-org/your-repo.git 
cd /your_path/Supabase
```

### 2) Create and activate a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3) Install dependencies
```bash
pip install -r python/requirements.txt
```

### 4) Configure environment variables
Create a `.env` file in `python/` (you can copy from the example):
```bash
cp python/.env.example python/.env
```
Then edit `python/.env` and set your values:
```
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
# Optional overrides
FETCH_OVERLAP_DAYS=1
ALPHA_VANTAGE_BATCH_SIZE=50
ALPHA_VANTAGE_SLEEP_SECONDS=2
MODE=daily
# Provide symbols either as comma-separated list or single symbol
SYMBOLS=BTCUSDT,ETHUSDT,AAPL,TSLA,SPY,QQQ,IEF,GLD,IBM

```

### 5) Run a one-off ingestion (multiple symbols)
```bash
cd python
python3 ingested_multiple_symbols.py
```
- The script reads `SYMBOLS` or `SYMBOL` from `.env`.
- It will resolve proxies for unsupported Alpha Vantage tickers (e.g., `^GSPC` → `SPY`).

## Tables Used
- `instruments`: stores instrument metadata (symbol, type, provider, currency)
- `ohlcv_data`: stores daily bars keyed by `(instrument_id, date)`

The Python ingestor will create missing `instruments` rows as needed and then upsert daily OHLCV rows.

## for manual run
to run script manually
```bash
python3 python/ingested_multiple_symbols.py
```


## Scheduling with cron (automation)
To run ingestion automatically every day at 19:00 (7 PM) server time, add one of the crontab entries below. Both log to `cron_log.txt`.

Edit your crontab:
```bash
crontab -e
```

- If your repo path is `Vector` (as per your provided command):
```bash
0 19 * * * cd /path_to_your_project/Vector/python && /home/path_to_your_project/Vector/venv/bin/python3 ingested_multiple_symbols.py >> cron_log.txt 2>&1
```
Notes:
- Make sure the virtualenv exists and contains the installed requirements.
- Ensure `.env` exists at `python/.env` with valid values.
- Logs will accumulate in the repo’s `python/cron_log.txt`.

## Troubleshooting
- If you see 401/permission errors: re-check `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`.
- If Alpha Vantage rate limits: increase `ALPHA_VANTAGE_SLEEP_SECONDS`.
- If missing tables/columns: confirm your Supabase schema matches the expected table names.

## Development Notes
- Main entry point for batch ingestion: `python/ingested_multiple_symbols.py`
- Provider implementation: `python/providers/alpha_vantage.py`
- Types: `python/providers/types.py`
