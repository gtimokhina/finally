from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CachedPrice:
    ticker: str
    price: float
    prev_price: float
    change_pct: float
    updated_at: datetime


class PriceCache:
    """
    In-memory store of the latest price per ticker.

    asyncio is single-threaded, so a plain dict is safe without locking as
    long as all access is from the same event loop. If threading is ever
    introduced, wrap mutations in asyncio.Lock.
    """

    def __init__(self) -> None:
        self._data: dict[str, CachedPrice] = {}

    def set(self, update: "PriceUpdate") -> None:  # noqa: F821
        self._data[update.ticker] = CachedPrice(
            ticker=update.ticker,
            price=update.price,
            prev_price=update.prev_price,
            change_pct=update.change_pct,
            updated_at=update.timestamp,
        )

    def get(self, ticker: str) -> Optional[CachedPrice]:
        return self._data.get(ticker)

    def get_all(self) -> dict[str, CachedPrice]:
        """Returns a shallow copy — safe to iterate while cache updates."""
        return dict(self._data)

    def snapshot(self) -> list[CachedPrice]:
        """All current prices as a list."""
        return list(self._data.values())

    def has(self, ticker: str) -> bool:
        return ticker in self._data
