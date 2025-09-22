import os
import asyncio
from datetime import datetime, timedelta
from datetime import datetime, timezone

from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
from supabase import create_client, Client

from providers.alpha_vantage import AlphaVantageProvider
from providers.types import CandleDaily



def iso_date(dt: datetime) -> str:
	return dt.strftime("%Y-%m-%d")


def resolve_alpha_vantage_symbol(input_symbol: str) -> Tuple[str, str]:
	"""
	Return (api_symbol, note). For unsupported tickers, map to closest ETF proxy.
	- Crypto pairs (e.g., BTC-USD, ETH-USD) are passed through
	- Indices/futures map to ETFs where possible
	"""
	s = input_symbol.strip()
	upper = s.upper()
	# Crypto pairs supported by our provider
	if "-" in upper and upper.endswith("-USD"):
		return upper, "crypto"
	# Known proxies for Alpha Vantage support
	proxy_map = {
		"^GSPC": "SPY",   # S&P 500 → SPY ETF
		"^IXIC": "QQQ",   # NASDAQ Composite → QQQ ETF
		"^TNX": "IEF",    # 10Y yield → 7-10Y Treasury ETF proxy
		"GC=F": "GLD",    # Gold futures → GLD ETF
	}
	if upper in proxy_map:
		return proxy_map[upper], f"proxy_for:{upper}"
	# Skip other Yahoo-style indices/futures if any
	if upper.startswith("^") or ("=F" in upper):
		return "", "skip_unsupported"
	# Default passthrough for equities like AAPL, TSLA, IBM
	return upper, "equity"


def aggregate_intraday_to_daily(intraday: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
	"""
	Aggregate a list of dicts with keys ts, open, high, low, close, volume into
	daily bars keyed by date YYYY-MM-DD.
	"""
	by_date: Dict[str, Dict[str, Any]] = {}
	for c in intraday:
		# ts is e.g. '2025-09-22 13:00:00'
		date_key = c["ts"][0:10]
		bar = by_date.get(date_key)
		if not bar:
			by_date[date_key] = {
				"open": c["open"],
				"high": c["high"],
				"low": c["low"],
				"close": c["close"],
				"volume": c.get("volume") or 0,
				"first_ts": c["ts"],
				"last_ts": c["ts"],
			}
		else:
			bar["high"] = max(bar["high"], c["high"])
			bar["low"] = min(bar["low"], c["low"])
			# Open stays from earliest ts; Close from latest ts
			if c["ts"] < bar["first_ts"]:
				bar["first_ts"] = c["ts"]
				bar["open"] = c["open"]
			if c["ts"] > bar["last_ts"]:
				bar["last_ts"] = c["ts"]
				bar["close"] = c["close"]
			bar["volume"] = (bar.get("volume") or 0) + (c.get("volume") or 0)
	return by_date


async def ingest_alpha_vantage(display_symbol: str, api_symbol: str | None = None, run_type: str = "daily") -> Dict[str, Any]:
	load_dotenv()
	url = os.environ.get("SUPABASE_URL")
	key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
	if not url or not key:
		raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

	client: Client = create_client(url, key)
	provider = AlphaVantageProvider()

	instrument_symbol = display_symbol
	fetch_symbol = api_symbol or display_symbol

	# First, let's find or create the instrument record using the DISPLAY symbol
	instruments_resp = client.table("instruments").select("*").eq("symbol", instrument_symbol).limit(1).execute()
	
	instrument_id = None
	if instruments_resp.data and len(instruments_resp.data) > 0:
		instrument_id = instruments_resp.data[0]["id"]
		print(f"✅ Found existing instrument_id: {instrument_id} for symbol: {instrument_symbol}")
	else:
		# Create new instrument record with proper fields
		print(f"📝 Creating new instrument record for symbol: {instrument_symbol}")
		inst_type = (
			"crypto" if ("-" in instrument_symbol and instrument_symbol.upper().endswith("-USD"))
			else "index" if instrument_symbol.startswith("^")
			else "future" if ("=F" in instrument_symbol)
			else "equity"
		)
		new_instrument = client.table("instruments").insert({
			"symbol": instrument_symbol,
			"name": instrument_symbol,
			"type": inst_type,
			"provider": "Alpha Vantage",
			"currency": "USD"
		}).execute()
		
		if new_instrument.data:
			instrument_id = new_instrument.data[0]["id"]
			print(f"✅ Created instrument_id: {instrument_id} for symbol: {instrument_symbol}")
		else:
			raise RuntimeError(f"Failed to create instrument record for {instrument_symbol}")

	mode = os.environ.get("MODE", run_type).lower()  # 'daily' or 'hours'


	# if mode in ("hours", "intraday"):
	# 	# Intraday mode: derive since_ts from last stored daily date, else 2022-01-01
	# 	interval = os.environ.get("INTRADAY_INTERVAL", "60min")
	# 	last_daily_resp = client.table("ohlcv_data").select("date").eq("instrument_id", instrument_id).order("date", desc=True).limit(1).execute()
	# 	if last_daily_resp.data and len(last_daily_resp.data) > 0:
	# 		last_date = datetime.fromisoformat(str(last_daily_resp.data[0]["date"]))
	# 		# Start from the beginning of that day to ensure we capture the full day if needed
	# 		since_dt = datetime(year=last_date.year, month=last_date.month, day=last_date.day)
	# 		print(f"⏱️ Intraday mode: last daily record {last_date.date()}, fetching from {since_dt}")
	# 	else:
	# 		# No daily data yet → start from 2022-01-01 00:00:00
	# 		since_dt = datetime.strptime("2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
	# 		print(f"⏱️ Intraday mode: no previous data, fetching from {since_dt}")
	# 	until_dt = datetime.utcnow()
	# 	since_ts = since_dt.strftime("%Y-%m-%d %H:%M:%S")
	# 	until_ts = until_dt.strftime("%Y-%m-%d %H:%M:%S")
	# 	intraday = await provider.fetch_intraday(fetch_symbol, interval=interval, since_ts=since_ts, until_ts=until_ts)
	# 	# Convert to simple dicts for aggregator
	# 	intraday_dicts = [{
	# 		"ts": c.ts,
	# 		"open": c.open,
	# 		"high": c.high,
	# 		"low": c.low,
	# 		"close": c.close,
	# 		"volume": c.volume,
	# 	} for c in intraday]
	# 	daily_map = aggregate_intraday_to_daily(intraday_dicts)
	# 	upserts = 0
	# 	for date_key, v in daily_map.items():
	# 		row = {
	# 			"instrument_id": instrument_id,
	# 			"instrument_symbol": instrument_symbol,
	# 			"date": date_key,
	# 			"open": v["open"],
	# 			"high": v["high"],
	# 			"low": v["low"],
	# 			"close": v["close"],
	# 			"volume": v.get("volume") or 0,
	# 		}
	# 		try:
	# 			client.table("ohlcv_data").upsert(row, on_conflict="instrument_id,date").execute()
	# 			upserts += 1
	# 		except Exception as e:
	# 			print(f"❌ Error upserting {date_key}: {e}")
	# 	print(f"✅ Intraday aggregated → daily upserts: {upserts} under {instrument_symbol}")
	# 	return {"ok": True, "upserts": upserts, "mode": mode}



	# Daily mode below
	# Check existing data to determine date range
	# Use a small overlap (default 1 day) to avoid missing late updates, rely on upsert to dedupe
	overlap_days = int(os.environ.get("FETCH_OVERLAP_DAYS", "1"))
	since: str | None = None
	
	resp = client.table("ohlcv_data").select("date").eq("instrument_id", instrument_id).order("date", desc=True).limit(1).execute()
	if resp.data and len(resp.data) > 0:
		last_date = datetime.fromisoformat(str(resp.data[0]["date"]))
		since_dt = last_date - timedelta(days=overlap_days)
		since = iso_date(since_dt)
		print(f"📅 Last data date: {last_date.strftime('%Y-%m-%d')}, fetching from: {since}")
	else:
		# Set default start date to January 1, 2022
		since = "2022-01-01"


		print(f"📅 No existing data found, fetching from default start date: {since}")

	today = iso_date(datetime.utcnow())

	

	# Fetch using the API symbol (could be a proxy like SPY for ^GSPC)
	candles: List[CandleDaily] = await provider.fetch_daily(fetch_symbol, since, today)
	upserts = 0

	print(f"📊 Fetched {len(candles)} candles from Alpha Vantage for {fetch_symbol} (storing under {instrument_symbol})")

	# Process candles in batches to avoid issues
	batch_size = int(os.environ.get("ALPHA_VANTAGE_BATCH_SIZE", "50"))
	
	for i in range(0, len(candles), batch_size):
		batch = candles[i:i + batch_size]
		rows = []
		print(f"📦 Processing batch {i//batch_size + 1}/{(len(candles) + batch_size - 1)//batch_size}")
		
		for c in batch:
			if not c.date:
				continue
			rows.append(
				{
				"instrument_id": instrument_id,
				"instrument_symbol": instrument_symbol,
				"date": c.date,
				"open": c.open,
				"high": c.high,
				"low": c.low,
				"close": c.close,
				"volume": c.volume or 0,
			}

			)
		if not rows:
			continue
                

			
		try:
			# Atomic upsert avoids duplicates on (instrument_id, date)
			client.table("ohlcv_data").upsert(rows, on_conflict="instrument_id,date").execute()
			# upserts += 1
			print(f"Upserted {len(rows)} candles for {instrument_symbol}")

		except Exception as e:
			# print(f"❌ Error upserting {c.date}: {e}")
			print(f"Error upserting batch for {instrument_symbol}: {e}")

			# continue

	print(f"✅ Successfully processed {upserts} records under {instrument_symbol}")

	return {"ok": True, "upserts": upserts, "mode": "daily"}


if __name__ == "__main__":
	load_dotenv()

	# Accept a comma-separated list of symbols via SYMBOLS, otherwise fallback to single SYMBOL
	symbols_env = os.environ.get("SYMBOLS", "")
	if symbols_env.strip():
		symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
	else:
		single_symbol = os.environ.get("SYMBOL", "").strip()
		symbols = [single_symbol] if single_symbol else []

	if not symbols:
		print("⚠️ No symbols provided. Set SYMBOLS='AAPL,TSLA' or SYMBOL='AAPL'.")
		exit(1)

	print(f"🚀 Starting ingestion for symbols: {symbols}")

	async def run_all():
		results: Dict[str, Any] = {}
		for idx, orig_sym in enumerate(symbols, start=1):
			api_sym, note = resolve_alpha_vantage_symbol(orig_sym)
			if note == "skip_unsupported" or not api_sym:
				print(f"⏭️ Skipping unsupported for Alpha Vantage: {orig_sym}")
				continue
			if api_sym != orig_sym:
				print(f"🔁 Mapping {orig_sym} → {api_sym} ({note})")

			print(f"➡️ [{idx}/{len(symbols)}] Processing {orig_sym} (fetch {api_sym})")
			try:
				res = await ingest_alpha_vantage(display_symbol=orig_sym, api_symbol=api_sym)
				results[orig_sym] = {"api_symbol": api_sym, **res}
				# Respect Alpha Vantage free tier: up to 5 requests/min → ~12-15s gap
				sleep_time = int(os.environ.get("ALPHA_VANTAGE_SLEEP_SECONDS", "2"))
				await asyncio.sleep(sleep_time)
			except Exception as e:
				print(f"❌ Failed {orig_sym} (via {api_sym}): {e}")
				results[orig_sym] = {"ok": False, "error": str(e), "api_symbol": api_sym}
		return results

	final_results = asyncio.run(run_all())
	print(f"🎉 Completed: {final_results}")
