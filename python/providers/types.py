from dataclasses import dataclass
from typing import List, Protocol, Optional


@dataclass
class CandleDaily:
	date: str
	open: float
	high: float
	low: float
	close: float
	adjusted_close: Optional[float] = None
	volume: Optional[int] = None
	dividend_amount: Optional[float] = None
	split_coefficient: Optional[float] = None


@dataclass
class CandleIntraday:
	ts: str
	open: float
	high: float
	low: float
	close: float
	volume: Optional[int] = None


class MarketDataProvider(Protocol):
	code: str

	async def fetch_daily(self, symbol: str, since: Optional[str] = None, until: Optional[str] = None) -> List[CandleDaily]:
		... 