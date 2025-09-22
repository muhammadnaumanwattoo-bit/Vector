import os
from typing import List, Optional
import httpx
from .types import CandleDaily, MarketDataProvider, CandleIntraday
from datetime import datetime, timezone



class AlphaVantageProvider:
	code = "alpha_vantage"
	_base_url = "https://www.alphavantage.co/query"

	def __init__(self, api_key: str | None = None):
		self.api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
		if not self.api_key:
			raise RuntimeError("ALPHA_VANTAGE_API_KEY is required")

	async def fetch_daily(self, symbol: str, since: Optional[str] = None, until: Optional[str] = None) -> List[CandleDaily]:
		# Detect crypto pair e.g. BTC-USD, ETH-USD and use DIGITAL_CURRENCY_DAILY
		is_crypto_pair = "-" in symbol and symbol.upper().endswith("-USD")

		if is_crypto_pair:
			base, quote = symbol.upper().split("-", 1)
			params = {
				"function": "DIGITAL_CURRENCY_DAILY",
				"symbol": base,
				"market": quote,
				"apikey": self.api_key,
			}
			async with httpx.AsyncClient(timeout=30) as client:
				res = await client.get(self._base_url, params=params, headers={"accept": "application/json"})
				res.raise_for_status()
				data = res.json()

				if "Error Message" in data:
					raise RuntimeError(f"AlphaVantage API Error: {data['Error Message']}")
				if "Note" in data:
					raise RuntimeError(f"AlphaVantage Rate Limit: {data['Note']}")
				if "Information" in data:
					raise RuntimeError(f"AlphaVantage Info: {data['Information']}")

				series = data.get("Time Series (Digital Currency Daily)")
				if not isinstance(series, dict):
					raise RuntimeError("Unexpected AlphaVantage payload: missing Time Series (Digital Currency Daily)")

				rows: List[CandleDaily] = []
				for date, v in series.items():
					# Use USD fields (suffix (USD))
					row = CandleDaily(
						date=date,
						open=float(v.get("1a. open (USD)", v.get("1b. open (USD)", 0)) or 0),
						high=float(v.get("2a. high (USD)", v.get("2b. high (USD)", 0)) or 0),
						low=float(v.get("3a. low (USD)", v.get("3b. low (USD)", 0)) or 0),
						close=float(v.get("4a. close (USD)", v.get("4b. close (USD)", 0)) or 0),
						volume=int(float(v.get("5. volume", 0))) if v.get("5. volume") else None,
					)
					rows.append(row)

				filtered = [r for r in rows if (since is None or r.date >= since) and (until is None or r.date <= until)]
				filtered.sort(key=lambda r: r.date)
				return filtered

		# Default to equities/indices via TIME_SERIES_DAILY
		params = {
			"function": "TIME_SERIES_DAILY",
			"symbol": symbol,
			"apikey": self.api_key,
		}
		# Smart outputsize selection
		if since:
			since_date = datetime.strptime(since, "%Y-%m-%d")
			days_diff = (datetime.now() - since_date).days
			if days_diff <= 100:
				params["outputsize"] = "compact"
			else:
				params["outputsize"] = "full"
		else:
			params["outputsize"] = "full"
		
		async with httpx.AsyncClient(timeout=30) as client:
			res = await client.get(self._base_url, params=params, headers={"accept": "application/json"})
			res.raise_for_status()
			data = res.json()

			# Check for API errors
			if "Error Message" in data:
				raise RuntimeError(f"AlphaVantage API Error: {data['Error Message']}")
			if "Note" in data:
				raise RuntimeError(f"AlphaVantage Rate Limit: {data['Note']}")
			if "Information" in data:
				raise RuntimeError(f"AlphaVantage Info: {data['Information']}")

			# Get the time series data
			series = data.get("Time Series (Daily)")
			if not isinstance(series, dict):
				raise RuntimeError("Unexpected AlphaVantage payload: missing Time Series (Daily)")

			rows: List[CandleDaily] = []
			for date, v in series.items():
				row = CandleDaily(
					date=date,
					open=float(v.get("1. open", 0)),
					high=float(v.get("2. high", 0)),
					low=float(v.get("3. low", 0)),
					close=float(v.get("4. close", 0)),
					volume=int(v.get("5. volume", 0)) if v.get("5. volume") else None,
				)
				rows.append(row)

			# Filter by date range if specified
			filtered = [r for r in rows if (since is None or r.date >= since) and (until is None or r.date <= until)]
			filtered.sort(key=lambda r: r.date)
			return filtered

	async def fetch_intraday(self, symbol: str, interval: str = "60min", since_ts: Optional[str] = None, until_ts: Optional[str] = None) -> List[CandleIntraday]:
		# Detect crypto pair
		is_crypto_pair = "-" in symbol and symbol.upper().endswith("-USD")
		if is_crypto_pair:
			base, quote = symbol.upper().split("-", 1)
			params = {
				"function": "CRYPTO_INTRADAY",
				"symbol": base,
				"market": quote,
				"interval": interval,
				"apikey": self.api_key,
			}
			async with httpx.AsyncClient(timeout=30) as client:
				res = await client.get(self._base_url, params=params, headers={"accept": "application/json"})
				res.raise_for_status()
				data = res.json()
				if "Error Message" in data:
					raise RuntimeError(f"AlphaVantage API Error: {data['Error Message']}")
				if "Note" in data:
					raise RuntimeError(f"AlphaVantage Rate Limit: {data['Note']}")
				if "Information" in data:
					raise RuntimeError(f"AlphaVantage Info: {data['Information']}")
				series = data.get(f"Time Series Crypto ({interval})")
				if not isinstance(series, dict):
					raise RuntimeError("Unexpected AlphaVantage payload: missing Time Series Crypto")
				rows: List[CandleIntraday] = []
				for ts, v in series.items():
					row = CandleIntraday(
						ts=ts,
						open=float(v.get("1. open", 0)),
						high=float(v.get("2. high", 0)),
						low=float(v.get("3. low", 0)),
						close=float(v.get("4. close", 0)),
						volume=int(float(v.get("5. volume", 0))) if v.get("5. volume") else None,
					)
					rows.append(row)
				filtered = [r for r in rows if (since_ts is None or r.ts >= since_ts) and (until_ts is None or r.ts <= until_ts)]
				filtered.sort(key=lambda r: r.ts)
				return filtered

		# Equities/ETFs intraday
		params = {
			"function": "TIME_SERIES_INTRADAY",
			"symbol": symbol,
			"interval": interval,
			"apikey": self.api_key,
		}
		# Alpha Vantage limits: 'compact' ~ last 100 points
		params["outputsize"] = "compact"
		async with httpx.AsyncClient(timeout=30) as client:
			res = await client.get(self._base_url, params=params, headers={"accept": "application/json"})
			res.raise_for_status()
			data = res.json()
			if "Error Message" in data:
				raise RuntimeError(f"AlphaVantage API Error: {data['Error Message']}")
			if "Note" in data:
				raise RuntimeError(f"AlphaVantage Rate Limit: {data['Note']}")
			if "Information" in data:
				raise RuntimeError(f"AlphaVantage Info: {data['Information']}")
			series = data.get(f"Time Series ({interval})")
			if not isinstance(series, dict):
				raise RuntimeError("Unexpected AlphaVantage payload: missing Time Series for intraday")
			rows: List[CandleIntraday] = []
			for ts, v in series.items():
				row = CandleIntraday(
					ts=ts,
					open=float(v.get("1. open", 0)),
					high=float(v.get("2. high", 0)),
					low=float(v.get("3. low", 0)),
					close=float(v.get("4. close", 0)),
					volume=int(float(v.get("5. volume", 0))) if v.get("5. volume") else None,
				)
				rows.append(row)
			filtered = [r for r in rows if (since_ts is None or r.ts >= since_ts) and (until_ts is None or r.ts <= until_ts)]
			filtered.sort(key=lambda r: r.ts)
			return filtered
