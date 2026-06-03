from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PriceUpdate:
    ticker: str
    price: float
    prev_price: float    # previous known price (drives green/red flash direction)
    change_pct: float    # % change vs. open / prev-close
    timestamp: datetime  # UTC


class MarketDataProvider(ABC):
    """
    Abstract base for all price sources.

    Subclasses implement _fetch_prices() and poll_interval_seconds().
    The run() loop calls them on the configured interval and writes
    results into the shared PriceCache.
    """

    def __init__(self, cache: "PriceCache") -> None:  # noqa: F821
        self._cache = cache

    @abstractmethod
    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        """
        Return fresh PriceUpdate objects for the given tickers.
        Return [] on any error — caller retains stale cache values.
        """
        ...

    @abstractmethod
    def poll_interval_seconds(self) -> float:
        """Seconds between poll cycles."""
        ...

    async def run(self, get_tickers) -> None:
        """
        Background task entry point. Call once at app startup via
        asyncio.create_task(provider.run(get_tickers)).

        get_tickers: async () -> list[str]
            Called each cycle to get the current watchlist. Allows
            tickers to be added/removed at runtime without restarting.
        """
        while True:
            tickers = await get_tickers()
            if tickers:
                updates = await self._fetch_prices(tickers)
                for u in updates:
                    self._cache.set(u)
            await asyncio.sleep(self.poll_interval_seconds())
