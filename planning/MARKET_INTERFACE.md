# Market Data Interface — Unified Python Design

This document specifies the unified Python interface for market data in FinAlly. The interface is provider-agnostic: the backend selects the Massive API implementation when `MASSIVE_API_KEY` is set, otherwise it uses the built-in simulator.

See `Massive_API.md` for full endpoint reference and `MARKET_SIMULATOR.md` for simulator internals.

---

## Design Goals

- **Single abstraction** — all downstream code (SSE streaming, price cache, portfolio valuation) calls one interface; it never knows which provider is active.
- **Non-blocking** — price polling runs as an `asyncio` background task; it does not block the FastAPI event loop.
- **In-memory cache** — a shared `PriceCache` holds the latest price per ticker and is read-locked for concurrent SSE clients.
- **Graceful degradation** — if the Massive API call fails, the cache retains stale prices rather than crashing.
- **Testable** — the abstract base class can be easily mocked or subclassed in tests.

---

## Abstract Interface

```python
# backend/market/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PriceUpdate:
    ticker: str
    price: float
    prev_price: float        # previous known price (for flash direction)
    change_pct: float        # percentage change vs. previous day close
    timestamp: datetime


class MarketDataProvider(ABC):
    """
    Supplies a continuous stream of price updates for a dynamic set of tickers.

    Subclasses implement _fetch_prices(); the base class runs the polling loop
    and writes updates into the shared PriceCache.
    """

    def __init__(self, cache: "PriceCache") -> None:
        self._cache = cache

    @abstractmethod
    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        """
        Fetch the latest prices for the given tickers.
        Returns an empty list on failure (caller retains stale cache values).
        """
        ...

    @abstractmethod
    def poll_interval_seconds(self) -> float:
        """How often to poll, in seconds."""
        ...

    async def run(self, get_tickers) -> None:
        """
        Background task entry point.
        get_tickers: zero-argument async callable that returns list[str].
        """
        import asyncio
        while True:
            tickers = await get_tickers()
            if tickers:
                updates = await self._fetch_prices(tickers)
                for update in updates:
                    self._cache.set(update)
            await asyncio.sleep(self.poll_interval_seconds())
```

---

## Price Cache

```python
# backend/market/cache.py
import asyncio
from datetime import datetime
from dataclasses import dataclass, field
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
    Thread-safe in-memory store of the latest price per ticker.
    Written by the background polling task, read by SSE stream handlers.
    """

    def __init__(self) -> None:
        self._data: dict[str, CachedPrice] = {}
        self._lock = asyncio.Lock()

    def set(self, update: "PriceUpdate") -> None:
        """Upsert a price update (called from the background task)."""
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
        return dict(self._data)

    def snapshot(self) -> list[CachedPrice]:
        """Return all current prices as a list, safe to iterate."""
        return list(self._data.values())
```

---

## Massive API Provider

```python
# backend/market/massive.py
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache

logger = logging.getLogger(__name__)

BASE_URL = "https://api.polygon.io"
SNAPSHOT_URL = f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers"


class MassiveMarketProvider(MarketDataProvider):
    """
    Polls the Massive (Polygon.io) snapshot endpoint for live prices.
    One HTTP call retrieves all watched tickers simultaneously.
    """

    # Seconds between polls; free tier allows 5 req/min → 15 s minimum.
    # Paid tiers can safely use 2–5 s.
    FREE_TIER_INTERVAL = 15.0
    PAID_TIER_INTERVAL = 5.0

    def __init__(self, api_key: str, cache: PriceCache) -> None:
        super().__init__(cache)
        self._api_key = api_key
        self._client = httpx.AsyncClient(timeout=10.0)
        # Store previous close prices for change_pct calculation
        self._prev_close: dict[str, float] = {}

    def poll_interval_seconds(self) -> float:
        return self.FREE_TIER_INTERVAL

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        try:
            resp = await self._client.get(
                SNAPSHOT_URL,
                params={
                    "tickers": ",".join(tickers),
                    "apiKey": self._api_key,
                },
            )
            resp.raise_for_status()
            body = resp.json()

            if body.get("status") != "OK":
                logger.warning("Massive API non-OK status: %s", body.get("error"))
                return []

            return [
                self._parse_snapshot(snap)
                for snap in body.get("tickers", [])
            ]

        except (httpx.HTTPError, ValueError) as exc:
            logger.error("Massive API fetch error: %s", exc)
            return []

    def _parse_snapshot(self, snap: dict) -> PriceUpdate:
        ticker = snap["ticker"]

        # Prefer lastTrade.p for real-time price; fall back to day.c
        last_trade = snap.get("lastTrade") or {}
        day = snap.get("day") or {}
        prev_day = snap.get("prevDay") or {}

        current_price = last_trade.get("p") or day.get("c") or 0.0
        prev_close = prev_day.get("c") or current_price

        # Cache prev_close for tickers we've seen before
        if ticker not in self._prev_close:
            self._prev_close[ticker] = prev_close
        prev_price_for_flash = self._cache.get(ticker)
        prev_price = prev_price_for_flash.price if prev_price_for_flash else current_price

        change_pct = snap.get("todaysChangePerc") or (
            ((current_price - prev_close) / prev_close * 100) if prev_close else 0.0
        )

        return PriceUpdate(
            ticker=ticker,
            price=current_price,
            prev_price=prev_price,
            change_pct=round(change_pct, 4),
            timestamp=datetime.now(timezone.utc),
        )

    async def close(self) -> None:
        await self._client.aclose()
```

---

## Simulator Provider

```python
# backend/market/simulator.py
# Full implementation details in MARKET_SIMULATOR.md.
# This stub shows the interface contract.

import asyncio
from datetime import datetime, timezone

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache


class SimulatorMarketProvider(MarketDataProvider):
    """
    Generates synthetic prices using geometric Brownian motion.
    No external dependencies; runs fully in-process.
    """

    POLL_INTERVAL = 0.5  # 500 ms — same cadence as SSE push rate

    def poll_interval_seconds(self) -> float:
        return self.POLL_INTERVAL

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        # Advances GBM state and returns new prices.
        # See MARKET_SIMULATOR.md for full implementation.
        ...
```

---

## Provider Factory

```python
# backend/market/factory.py
import os
from .cache import PriceCache
from .massive import MassiveMarketProvider
from .simulator import SimulatorMarketProvider
from .base import MarketDataProvider


def create_market_provider(cache: PriceCache) -> MarketDataProvider:
    """
    Returns the appropriate provider based on environment configuration.

    - MASSIVE_API_KEY set and non-empty  →  MassiveMarketProvider
    - MASSIVE_API_KEY absent or empty    →  SimulatorMarketProvider
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveMarketProvider(api_key=api_key, cache=cache)
    return SimulatorMarketProvider(cache=cache)
```

---

## FastAPI Integration

```python
# backend/main.py  (relevant excerpts)
from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from market.cache import PriceCache
from market.factory import create_market_provider

# Shared singletons (created once at startup)
price_cache = PriceCache()
market_provider = create_market_provider(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background polling task
    task = asyncio.create_task(
        market_provider.run(get_tickers=_get_watchlist_tickers)
    )
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


async def _get_watchlist_tickers() -> list[str]:
    """Reads current watchlist from DB; called each poll cycle."""
    # DB query returns list of ticker strings
    ...


# SSE endpoint reads directly from the cache
@app.get("/api/stream/prices")
async def stream_prices():
    async def event_generator():
        import json
        while True:
            prices = price_cache.snapshot()
            for p in prices:
                payload = {
                    "ticker": p.ticker,
                    "price": p.price,
                    "prev_price": p.prev_price,
                    "change_pct": p.change_pct,
                    "timestamp": p.updated_at.isoformat(),
                }
                yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

---

## Directory Layout

```
backend/
└── market/
    ├── __init__.py
    ├── base.py          # PriceUpdate dataclass + MarketDataProvider ABC
    ├── cache.py         # PriceCache
    ├── factory.py       # create_market_provider()
    ├── massive.py       # MassiveMarketProvider
    └── simulator.py     # SimulatorMarketProvider (see MARKET_SIMULATOR.md)
```

---

## Data Flow Summary

```
Background task (0.5 s or 15 s)
    │
    ▼
MarketDataProvider._fetch_prices(tickers)
    │  [Massive: HTTP → api.polygon.io]
    │  [Simulator: GBM math in-process]
    ▼
PriceCache.set(PriceUpdate)
    │
    ├──► GET /api/stream/prices  (SSE, reads cache every 0.5 s)
    │        │
    │        ▼
    │    Frontend EventSource → price flash animations
    │
    └──► GET /api/portfolio      (reads cache for current prices)
         GET /api/watchlist      (reads cache for current prices)
```

---

## Testing

Both providers share the same `MarketDataProvider` ABC, so tests can:

1. Subclass `MarketDataProvider` with fixed return values to test the SSE stream and portfolio endpoints without network or GBM state.
2. Test `MassiveMarketProvider._parse_snapshot()` with fixture JSON from real API responses.
3. Test `SimulatorMarketProvider` for price drift, volatility bounds, and event injection.

```python
# tests/market/test_factory.py
import os, pytest
from market.factory import create_market_provider
from market.cache import PriceCache
from market.massive import MassiveMarketProvider
from market.simulator import SimulatorMarketProvider

def test_selects_massive_when_key_set(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test_key_123")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, MassiveMarketProvider)

def test_selects_simulator_when_key_absent(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)
```
