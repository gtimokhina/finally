from __future__ import annotations

import math
from datetime import datetime, timezone

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache

MOCK_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "MSFT": 415.00,
    "GOOGL": 175.00,
    "NVDA": 870.00,
    "META": 500.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "JPM":  205.00,
    "V":    275.00,
    "NFLX": 680.00,
}


class MockMarketProvider(MarketDataProvider):
    """
    Returns static prices with a tiny deterministic sine-wave drift.
    Activated when LLM_MOCK=true for fast, reproducible E2E tests.
    """

    POLL_INTERVAL = 0.5

    def __init__(self, cache: PriceCache) -> None:
        super().__init__(cache)
        self._tick = 0

    def poll_interval_seconds(self) -> float:
        return self.POLL_INTERVAL

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        self._tick += 1
        now = datetime.now(timezone.utc)
        updates = []
        for ticker in tickers:
            base = MOCK_PRICES.get(ticker, 100.0)
            price = round(base * (1 + 0.001 * math.sin(self._tick * 0.1)), 4)
            prev  = round(base * (1 + 0.001 * math.sin((self._tick - 1) * 0.1)), 4)
            updates.append(PriceUpdate(
                ticker=ticker,
                price=price,
                prev_price=prev,
                change_pct=round((price - base) / base * 100, 4),
                timestamp=now,
            ))
        return updates
