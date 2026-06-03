# Market Data Backend — Implementation Design

This document is the implementation reference for FinAlly's market data subsystem. It covers all three layers: the unified provider interface, the GBM simulator, and the Massive (Polygon.io) API client. Read this before touching any file in `backend/market/`.

Cross-references:
- `MARKET_INTERFACE.md` — abstract interface contract
- `MARKET_SIMULATOR.md` — GBM mathematics
- `Massive_API.md` — Massive REST endpoint reference

---

## 1. Directory Layout

```
backend/
└── market/
    ├── __init__.py       # exports: PriceCache, create_market_provider
    ├── base.py           # PriceUpdate dataclass + MarketDataProvider ABC
    ├── cache.py          # PriceCache — in-memory price store
    ├── simulator.py      # SimulatorMarketProvider (GBM)
    ├── massive.py        # MassiveMarketProvider (Polygon.io REST)
    └── factory.py        # create_market_provider() — env-driven selection
```

`backend/market/__init__.py`:
```python
from .cache import PriceCache
from .factory import create_market_provider

__all__ = ["PriceCache", "create_market_provider"]
```

---

## 2. Core Data Types (`base.py`)

`PriceUpdate` is the single type exchanged between all layers. Every provider produces it; the cache consumes it; the SSE stream serialises it.

```python
# backend/market/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PriceUpdate:
    ticker: str
    price: float           # current last price
    prev_price: float      # previous price (one tick ago — drives flash direction)
    change_pct: float      # % change vs. open/prev-close (for daily change display)
    timestamp: datetime    # UTC


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
        import asyncio

        while True:
            tickers = await get_tickers()
            if tickers:
                updates = await self._fetch_prices(tickers)
                for u in updates:
                    self._cache.set(u)
            await asyncio.sleep(self.poll_interval_seconds())
```

**Why `get_tickers` is a callable:** The watchlist is mutable at runtime. Passing a callable (not a snapshot list) ensures each poll cycle sees the current watchlist from the database rather than the list at startup.

---

## 3. Price Cache (`cache.py`)

The cache is the single source of truth for current prices. It is written by the background polling task and read concurrently by:
- `GET /api/stream/prices` (SSE handler, every 500 ms)
- `GET /api/portfolio` (read current prices for P&L)
- `GET /api/watchlist` (attach latest price to each ticker)

```python
# backend/market/cache.py
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

    asyncio is single-threaded, so a dict is safe without locking as
    long as all access is from the same event loop (which it is here).
    If threading is ever introduced, wrap mutations in asyncio.Lock.
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
```

**Reading from the cache in route handlers:**

```python
# In any FastAPI route that needs current prices:
from market import PriceCache

# cache is a module-level singleton (see Section 7)
price = cache.get("AAPL")
if price:
    current = price.price
    change  = price.change_pct
```

---

## 4. Simulator (`simulator.py`)

### 4.1 GBM Mathematics

Each price step uses the exact GBM solution (not Euler approximation):

```
S(t+Δt) = S(t) · exp((μ - σ²/2)·Δt + σ·√Δt·Z)
```

For a 500 ms tick on 252 trading days of 6.5 hours:

```
Δt = 0.5 / (252 × 6.5 × 3600) ≈ 8.48 × 10⁻⁸  (fraction of a year)
```

Per-tick standard deviation for AAPL (σ=0.22): `0.22 × √(8.48×10⁻⁸) ≈ 0.0064%` — smooth, realistic movement.

### 4.2 Sector Correlation

A single sector shock `Z_sector ~ N(0,1)` is drawn once per tick per sector. Each ticker combines it with its own idiosyncratic shock:

```
Z_total = ρ · Z_sector + √(1 - ρ²) · Z_idio
```

Correlation coefficients: Tech=0.40, Finance=0.30, EV/Media/Other=0.00.

### 4.3 Full Implementation

```python
# backend/market/simulator.py
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache


@dataclass
class TickerConfig:
    seed_price: float
    drift: float      # annualised (e.g. 0.15 = 15%/yr)
    volatility: float # annualised (e.g. 0.22 = 22%/yr)
    sector: str


# Default configurations for the 10 seeded watchlist tickers.
# These mirror the seed data in the database schema.
TICKER_CONFIGS: dict[str, TickerConfig] = {
    "AAPL":  TickerConfig(190.00, 0.15, 0.22, "tech"),
    "MSFT":  TickerConfig(415.00, 0.18, 0.20, "tech"),
    "GOOGL": TickerConfig(175.00, 0.16, 0.24, "tech"),
    "NVDA":  TickerConfig(870.00, 0.25, 0.40, "tech"),
    "META":  TickerConfig(500.00, 0.20, 0.28, "tech"),
    "AMZN":  TickerConfig(185.00, 0.18, 0.26, "tech"),
    "TSLA":  TickerConfig(250.00, 0.10, 0.55, "ev"),
    "JPM":   TickerConfig(205.00, 0.12, 0.18, "finance"),
    "V":     TickerConfig(275.00, 0.14, 0.16, "finance"),
    "NFLX":  TickerConfig(680.00, 0.15, 0.30, "media"),
}

# User-added tickers not in the table get this generic config
DEFAULT_CONFIG = TickerConfig(100.00, 0.10, 0.25, "other")

SECTOR_CORRELATION: dict[str, float] = {
    "tech":    0.40,
    "finance": 0.30,
    "ev":      0.00,
    "media":   0.00,
    "other":   0.00,
}

TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
TICK_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # 500ms as fraction of year

EVENT_PROBABILITY = 0.0005  # ~1 event per ticker per 17 minutes at 500ms ticks
EVENT_MAGNITUDE   = 0.025   # ±2.5% shock


@dataclass
class _TickerState:
    config: TickerConfig
    current_price: float
    prev_price: float
    open_price: float  # price at simulation start — used as "prev close" proxy


class SimulatorMarketProvider(MarketDataProvider):
    """
    GBM-based synthetic market data. No external dependencies.
    Runs as an in-process background task alongside FastAPI.
    """

    POLL_INTERVAL = 0.5  # seconds — matches SSE push cadence

    def __init__(self, cache: PriceCache, seed: Optional[int] = None) -> None:
        super().__init__(cache)
        self._rng = random.Random(seed)  # seed=None → non-deterministic
        self._states: dict[str, _TickerState] = {}

    def poll_interval_seconds(self) -> float:
        return self.POLL_INTERVAL

    # ── State ───────────────────────────────────────────────────────────────

    def _ensure_state(self, ticker: str) -> _TickerState:
        """Lazily create state on first encounter (handles user-added tickers)."""
        if ticker not in self._states:
            cfg = TICKER_CONFIGS.get(ticker, DEFAULT_CONFIG)
            self._states[ticker] = _TickerState(
                config=cfg,
                current_price=cfg.seed_price,
                prev_price=cfg.seed_price,
                open_price=cfg.seed_price,
            )
        return self._states[ticker]

    # ── GBM step ────────────────────────────────────────────────────────────

    def _gbm_step(self, state: _TickerState, sector_shock: float) -> float:
        """Advance price by one tick using exact GBM discretisation."""
        cfg = state.config
        rho = SECTOR_CORRELATION.get(cfg.sector, 0.0)

        z_idio = self._rng.gauss(0, 1)
        z_total = rho * sector_shock + math.sqrt(max(1 - rho ** 2, 0)) * z_idio

        drift_term     = (cfg.drift - 0.5 * cfg.volatility ** 2) * TICK_DT
        diffusion_term = cfg.volatility * math.sqrt(TICK_DT) * z_total

        return state.current_price * math.exp(drift_term + diffusion_term)

    def _maybe_apply_event(self, price: float) -> float:
        """Inject an occasional sudden price shock for visual drama."""
        if self._rng.random() < EVENT_PROBABILITY:
            shock = self._rng.uniform(-EVENT_MAGNITUDE, EVENT_MAGNITUDE)
            price *= (1 + shock)
        return max(price, 0.01)  # prices never go to zero

    # ── Main fetch ──────────────────────────────────────────────────────────

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        """Called every 500ms by the base class run() loop."""
        now = datetime.now(timezone.utc)

        # One sector shock per sector per tick — creates correlated moves
        sectors = {TICKER_CONFIGS.get(t, DEFAULT_CONFIG).sector for t in tickers}
        sector_shocks = {s: self._rng.gauss(0, 1) for s in sectors}

        updates: list[PriceUpdate] = []
        for ticker in tickers:
            state = self._ensure_state(ticker)
            shock = sector_shocks.get(state.config.sector, 0.0)

            new_price = self._gbm_step(state, shock)
            new_price = self._maybe_apply_event(new_price)

            prev = state.current_price
            change_pct = (new_price - state.open_price) / state.open_price * 100

            state.prev_price    = prev
            state.current_price = new_price

            updates.append(PriceUpdate(
                ticker=ticker,
                price=round(new_price, 4),
                prev_price=round(prev, 4),
                change_pct=round(change_pct, 4),
                timestamp=now,
            ))

        return updates
```

### 4.4 Deterministic Mode for Tests

Pass a fixed `seed` to produce reproducible price paths:

```python
# In test setup
from market.simulator import SimulatorMarketProvider
from market.cache import PriceCache

sim = SimulatorMarketProvider(cache=PriceCache(), seed=42)
```

Or set `SIMULATOR_SEED=42` in the environment (handled by the factory — see Section 6).

---

## 5. Massive API Provider (`massive.py`)

### 5.1 Endpoint Used

`GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers`

One HTTP call fetches all watched tickers simultaneously. This is the most efficient choice: 10–20 tickers in a single request, within the free tier's 5 req/min limit at a 15-second poll interval.

### 5.2 Response → PriceUpdate Mapping

| Snapshot field | → PriceUpdate field | Notes |
|---|---|---|
| `lastTrade.p` or `day.c` | `price` | Prefer lastTrade for recency |
| previous `cache.get(ticker).price` | `prev_price` | Previous cached value drives flash direction |
| `todaysChangePerc` or computed | `change_pct` | Uses API field if present, else `(price - prevDay.c) / prevDay.c * 100` |
| `datetime.now(utc)` | `timestamp` | Poll time, not trade time |

### 5.3 Full Implementation

```python
# backend/market/massive.py
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache

logger = logging.getLogger(__name__)

_SNAPSHOT_URL = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"


class MassiveMarketProvider(MarketDataProvider):
    """
    Polls the Massive/Polygon.io snapshot endpoint for live prices.
    Falls back gracefully on HTTP errors — the cache retains stale values.
    """

    FREE_TIER_INTERVAL  = 15.0   # 5 req/min limit on free tier
    PAID_TIER_INTERVAL  =  5.0   # safe for unlimited tiers

    def __init__(
        self,
        api_key: str,
        cache: PriceCache,
        poll_interval: float = FREE_TIER_INTERVAL,
    ) -> None:
        super().__init__(cache)
        self._api_key = api_key
        self._interval = poll_interval
        # Shared async client — reuses TCP connections across poll cycles
        self._client = httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": "FinAlly/1.0"},
        )

    def poll_interval_seconds(self) -> float:
        return self._interval

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        try:
            resp = await self._client.get(
                _SNAPSHOT_URL,
                params={
                    "tickers": ",".join(tickers),
                    "apiKey": self._api_key,
                },
            )
            resp.raise_for_status()
            body = resp.json()

            if body.get("status") != "OK":
                logger.warning("Massive API non-OK: %s", body.get("error"))
                return []

            return [self._parse(snap) for snap in body.get("tickers", [])]

        except httpx.HTTPStatusError as exc:
            logger.error("Massive API HTTP %s: %s", exc.response.status_code, exc)
            return []
        except (httpx.RequestError, ValueError) as exc:
            logger.error("Massive API error: %s", exc)
            return []

    def _parse(self, snap: dict) -> PriceUpdate:
        ticker = snap["ticker"]

        last_trade = snap.get("lastTrade") or {}
        day        = snap.get("day")       or {}
        prev_day   = snap.get("prevDay")   or {}

        # lastTrade.p is the most recent print; day.c is the running close
        current_price = last_trade.get("p") or day.get("c") or 0.0
        prev_close    = prev_day.get("c") or current_price

        # prev_price drives the green/red flash on the frontend
        cached = self._cache.get(ticker)
        prev_price = cached.price if cached else current_price

        # Prefer the API's pre-computed field; fall back to manual calculation
        raw_change = snap.get("todaysChangePerc")
        if raw_change is not None:
            change_pct = float(raw_change)
        elif prev_close:
            change_pct = (current_price - prev_close) / prev_close * 100
        else:
            change_pct = 0.0

        return PriceUpdate(
            ticker=ticker,
            price=round(current_price, 4),
            prev_price=round(prev_price, 4),
            change_pct=round(change_pct, 4),
            timestamp=datetime.now(timezone.utc),
        )

    async def close(self) -> None:
        """Call during app shutdown to release the HTTP connection pool."""
        await self._client.aclose()
```

### 5.4 Rate Limit Handling

The free tier allows 5 requests per minute. With a 15-second poll interval and a single snapshot call per cycle, usage is at most 4 req/min — safely under the limit.

For paid tiers, pass `poll_interval=5.0` to the constructor (or set via factory env var). Do not go below 2 seconds even on unlimited tiers — the snapshot endpoint has undocumented per-IP throttling at very high rates.

### 5.5 Error Recovery

On any error, `_fetch_prices` returns `[]`. The base class `run()` loop skips calling `cache.set()` for the failed cycle, so the cache retains the last known prices. The SSE stream continues pushing the stale prices to the frontend. The frontend's connection status dot remains green — users see prices that are slightly stale, not an error.

If the API returns `HTTP 429` (rate limited), the same fallback applies. For persistent failures, add an exponential backoff in `run()` or wrap the poll cycle with retry logic.

---

## 6. Factory (`factory.py`)

```python
# backend/market/factory.py
from __future__ import annotations

import os

from .base import MarketDataProvider
from .cache import PriceCache
from .massive import MassiveMarketProvider
from .simulator import SimulatorMarketProvider


def create_market_provider(cache: PriceCache) -> MarketDataProvider:
    """
    Selects the appropriate provider based on environment variables.

    MASSIVE_API_KEY set and non-empty  →  MassiveMarketProvider (free tier: 15s interval)
    MASSIVE_API_KEY absent/empty       →  SimulatorMarketProvider
    SIMULATOR_SEED set                 →  deterministic simulator (for E2E tests)
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        # Allow paid-tier users to reduce the poll interval
        interval_str = os.environ.get("MASSIVE_POLL_INTERVAL", "")
        try:
            interval = float(interval_str)
        except ValueError:
            interval = MassiveMarketProvider.FREE_TIER_INTERVAL
        return MassiveMarketProvider(api_key=api_key, cache=cache, poll_interval=interval)

    seed_str = os.environ.get("SIMULATOR_SEED", "")
    seed = int(seed_str) if seed_str.isdigit() else None
    return SimulatorMarketProvider(cache=cache, seed=seed)
```

**Environment variables summary:**

| Variable | Effect |
|---|---|
| `MASSIVE_API_KEY=<key>` | Activates Massive provider |
| `MASSIVE_POLL_INTERVAL=5` | Override poll interval (seconds) for paid tiers |
| `SIMULATOR_SEED=42` | Deterministic GBM for E2E tests |
| (none) | Default simulator, non-deterministic |

---

## 7. FastAPI Integration (`main.py`)

### 7.1 Startup and Shutdown

The background polling task is launched in FastAPI's `lifespan` context manager — this is the correct pattern for asyncio tasks in FastAPI (not `@app.on_event` which is deprecated).

```python
# backend/main.py
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from market import PriceCache, create_market_provider
from db import get_watchlist_tickers  # your DB query function

logger = logging.getLogger(__name__)

# Module-level singletons — created once, shared across all requests
price_cache      = PriceCache()
market_provider  = create_market_provider(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background polling loop
    task = asyncio.create_task(
        market_provider.run(get_tickers=_get_watchlist_tickers),
        name="market-data-poll",
    )
    logger.info("Market data polling started")
    yield
    # Graceful shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # Close HTTP client if using Massive provider
    if hasattr(market_provider, "close"):
        await market_provider.close()
    logger.info("Market data polling stopped")


app = FastAPI(lifespan=lifespan)


async def _get_watchlist_tickers() -> list[str]:
    """Reads current watchlist from DB each poll cycle."""
    return await get_watchlist_tickers(user_id="default")
```

### 7.2 SSE Stream Endpoint

The SSE handler reads from the cache every 500ms and pushes all current prices. This is decoupled from the poll rate — the cache may update at 15s (Massive) but the SSE stream always pushes at 500ms (returning the same stale price until the next poll).

```python
@app.get("/api/stream/prices")
async def stream_prices():
    """
    SSE stream of live price updates.
    Pushes all cached prices every 500ms.
    Client: const es = new EventSource('/api/stream/prices')
    """
    async def event_generator():
        while True:
            prices = price_cache.snapshot()
            for p in prices:
                payload = {
                    "ticker":     p.ticker,
                    "price":      p.price,
                    "prev_price": p.prev_price,
                    "change_pct": p.change_pct,
                    "timestamp":  p.updated_at.isoformat(),
                }
                yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
        },
    )
```

**SSE event format** (one `data:` line per ticker per push):
```
data: {"ticker": "AAPL", "price": 190.12, "prev_price": 190.09, "change_pct": 0.42, "timestamp": "2026-06-03T14:22:01.123456+00:00"}

data: {"ticker": "MSFT", "price": 415.88, "prev_price": 415.91, "change_pct": -0.11, "timestamp": "2026-06-03T14:22:01.123456+00:00"}
```

### 7.3 Using the Cache in Other Routes

```python
@app.get("/api/watchlist")
async def get_watchlist():
    tickers = await get_watchlist_tickers(user_id="default")
    return [
        {
            "ticker": t,
            "price":      (p := price_cache.get(t)) and p.price,
            "change_pct": p and p.change_pct,
            "updated_at": p and p.updated_at.isoformat(),
        }
        for t in tickers
    ]


@app.get("/api/portfolio")
async def get_portfolio():
    positions = await get_positions(user_id="default")
    enriched = []
    for pos in positions:
        cached = price_cache.get(pos.ticker)
        current_price = cached.price if cached else pos.avg_cost
        unrealized_pnl = (current_price - pos.avg_cost) * pos.quantity
        enriched.append({
            "ticker":        pos.ticker,
            "quantity":      pos.quantity,
            "avg_cost":      pos.avg_cost,
            "current_price": current_price,
            "unrealized_pnl": round(unrealized_pnl, 2),
            "change_pct":    (current_price - pos.avg_cost) / pos.avg_cost * 100,
        })
    # ... cash balance, total value, etc.
```

---

## 8. Data Flow

```
                          ┌──────────────────────────────────────┐
                          │  Background Task (asyncio)           │
                          │                                      │
  DB watchlist query ────►│  MarketDataProvider.run()            │
  (each poll cycle)       │    │                                 │
                          │    ▼                                 │
                          │  _fetch_prices(tickers)              │
                          │    │                                 │
                          │    ├─ Simulator: GBM math (in-proc)  │
                          │    └─ Massive: HTTP GET snapshot      │
                          │         (api.polygon.io)             │
                          │    │                                 │
                          │    ▼                                 │
                          │  PriceCache.set(PriceUpdate) ×N      │
                          └──────────────────────┬───────────────┘
                                                 │
                   ┌─────────────────────────────┼──────────────────────────┐
                   │                             │                          │
                   ▼ (every 500ms)               ▼ (on request)             ▼ (on request)
        GET /api/stream/prices       GET /api/portfolio          GET /api/watchlist
        SSE → EventSource            enriched with live prices   with latest prices
        → price flash animations
```

**Key timing note:** When `MassiveMarketProvider` is active, the cache updates every 15s but the SSE stream still fires every 500ms. The frontend will see 30 identical pushes between cache refreshes, then a sudden jump when the poll completes. This is the intended behaviour — the connection status dot stays green, sparklines still animate on each SSE event (using unchanged values), and users understand the delay is from the free tier. For the simulator, the cache updates every 500ms, matching the SSE push rate exactly.

---

## 9. Frontend Integration (SSE Client)

This is backend documentation, but here is the exact SSE event the frontend should expect:

```typescript
// frontend/src/hooks/usePriceStream.ts (reference)
const es = new EventSource('/api/stream/prices');

es.onmessage = (event) => {
  const data = JSON.parse(event.data) as {
    ticker: string;
    price: number;
    prev_price: number;
    change_pct: number;
    timestamp: string;
  };
  // Update price store, trigger flash animation based on price vs prev_price
};

es.onerror = () => {
  // EventSource retries automatically — update connection status dot to yellow
};
```

The `prev_price` field is what drives the green/red flash: if `price > prev_price`, flash green; if `price < prev_price`, flash red.

---

## 10. Testing

### 10.1 Unit Tests — Simulator

```python
# tests/market/test_simulator.py
import asyncio
import pytest
from market.simulator import SimulatorMarketProvider, TICKER_CONFIGS
from market.cache import PriceCache


@pytest.fixture
def sim():
    return SimulatorMarketProvider(cache=PriceCache(), seed=0)


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_prices_stay_positive(sim):
    tickers = ["AAPL", "TSLA", "JPM"]
    for _ in range(1000):
        updates = run(sim._fetch_prices(tickers))
        for u in updates:
            assert u.price > 0


def test_deterministic_with_seed():
    sim1 = SimulatorMarketProvider(cache=PriceCache(), seed=99)
    sim2 = SimulatorMarketProvider(cache=PriceCache(), seed=99)
    u1 = run(sim1._fetch_prices(["AAPL", "MSFT"]))
    u2 = run(sim2._fetch_prices(["AAPL", "MSFT"]))
    assert [r.price for r in u1] == [r.price for r in u2]


def test_unknown_ticker_uses_default_config(sim):
    updates = run(sim._fetch_prices(["ZZZZ"]))
    assert len(updates) == 1
    assert updates[0].ticker == "ZZZZ"
    assert updates[0].price > 0


def test_prev_price_tracks_previous_tick(sim):
    [u1] = run(sim._fetch_prices(["AAPL"]))
    [u2] = run(sim._fetch_prices(["AAPL"]))
    assert u2.prev_price == u1.price


def test_change_pct_relative_to_open(sim):
    # After many ticks, change_pct should reflect cumulative drift from open
    for _ in range(100):
        updates = run(sim._fetch_prices(["AAPL"]))
    u = updates[0]
    state = sim._states["AAPL"]
    expected_pct = (state.current_price - state.open_price) / state.open_price * 100
    assert abs(u.change_pct - round(expected_pct, 4)) < 0.001
```

### 10.2 Unit Tests — Massive Provider

```python
# tests/market/test_massive.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from market.massive import MassiveMarketProvider
from market.cache import PriceCache

FIXTURE_RESPONSE = {
    "status": "OK",
    "tickers": [
        {
            "ticker": "AAPL",
            "todaysChangePerc": 0.65,
            "lastTrade": {"p": 186.73},
            "day":     {"o": 185.50, "h": 187.20, "l": 184.90, "c": 186.73},
            "prevDay": {"c": 185.50},
        },
        {
            "ticker": "MSFT",
            "todaysChangePerc": -0.12,
            "lastTrade": {"p": 415.00},
            "day":     {"c": 415.00},
            "prevDay": {"c": 415.50},
        },
    ],
}


@pytest.fixture
def provider():
    return MassiveMarketProvider(api_key="test_key", cache=PriceCache())


@pytest.mark.asyncio
async def test_parse_snapshot_uses_last_trade_price(provider):
    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    assert update.ticker == "AAPL"
    assert update.price == 186.73
    assert update.change_pct == 0.65


@pytest.mark.asyncio
async def test_returns_empty_on_http_error(provider):
    with patch.object(provider._client, "get", side_effect=Exception("timeout")):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


@pytest.mark.asyncio
async def test_returns_empty_on_api_error_status(provider):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"status": "ERROR", "error": "NOT_AUTHORIZED"}
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


@pytest.mark.asyncio
async def test_prev_price_comes_from_cache(provider):
    # Seed the cache with an existing price
    from market.base import PriceUpdate
    from datetime import datetime, timezone
    provider._cache.set(PriceUpdate("AAPL", 185.00, 184.00, 0.1, datetime.now(timezone.utc)))

    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    # prev_price should be the previously cached value, not lastTrade.p
    assert update.prev_price == 185.00
```

### 10.3 Unit Tests — Factory

```python
# tests/market/test_factory.py
import pytest
from market.factory import create_market_provider
from market.cache import PriceCache
from market.massive import MassiveMarketProvider
from market.simulator import SimulatorMarketProvider


def test_selects_massive_when_key_set(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "real_key_123")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, MassiveMarketProvider)


def test_selects_simulator_when_key_absent(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)


def test_selects_simulator_when_key_empty(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)


def test_simulator_seed_from_env(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("SIMULATOR_SEED", "42")
    p = create_market_provider(PriceCache())
    assert isinstance(p, SimulatorMarketProvider)
    assert p._rng.random() == SimulatorMarketProvider.__new__(
        SimulatorMarketProvider
    ).__class__.__mro__[0]  # just check it doesn't crash


def test_massive_custom_interval(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "5")
    p = create_market_provider(PriceCache())
    assert p.poll_interval_seconds() == 5.0
```

### 10.4 Mock Provider for E2E Tests

```python
# tests/market/mock_provider.py
from datetime import datetime, timezone
from market.base import MarketDataProvider, PriceUpdate
from market.cache import PriceCache

MOCK_PRICES = {
    "AAPL": 190.00, "MSFT": 415.00, "GOOGL": 175.00,
    "NVDA": 870.00, "META": 500.00, "AMZN": 185.00,
    "TSLA": 250.00, "JPM":  205.00, "V":    275.00, "NFLX": 680.00,
}


class MockMarketProvider(MarketDataProvider):
    """
    Returns static prices + tiny deterministic drift.
    Used in E2E tests (LLM_MOCK=true) for fast, reproducible runs.
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
            # tiny sine wave so prices visibly move in tests
            import math
            price = round(base * (1 + 0.001 * math.sin(self._tick * 0.1)), 4)
            prev  = base * (1 + 0.001 * math.sin((self._tick - 1) * 0.1))
            updates.append(PriceUpdate(
                ticker=ticker,
                price=price,
                prev_price=round(prev, 4),
                change_pct=round((price - base) / base * 100, 4),
                timestamp=now,
            ))
        return updates
```

Activate it by extending the factory:

```python
# backend/market/factory.py — test mode addition
def create_market_provider(cache: PriceCache) -> MarketDataProvider:
    if os.environ.get("LLM_MOCK") == "true":
        from .mock_provider import MockMarketProvider
        return MockMarketProvider(cache=cache)
    # ... existing Massive / Simulator logic
```

---

## 11. Dependencies

Add to `backend/pyproject.toml`:

```toml
[project]
dependencies = [
    "fastapi>=0.111",
    "uvicorn[standard]>=0.29",
    "httpx>=0.27",       # async HTTP client for Massive provider
    # no extra deps for simulator — stdlib only (math, random)
]

[project.optional-dependencies]
test = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
    "httpx>=0.27",  # for TestClient
]
```

`httpx` is the only non-stdlib dependency added by the market data module. The simulator uses only `math`, `random`, and `dataclasses`.

---

## 12. Configuration Reference

| Env Var | Default | Description |
|---|---|---|
| `MASSIVE_API_KEY` | (empty) | If set, activates Massive provider |
| `MASSIVE_POLL_INTERVAL` | `15.0` | Poll interval in seconds (Massive only) |
| `SIMULATOR_SEED` | (none) | Integer seed for deterministic GBM |
| `LLM_MOCK` | `false` | If `true`, activates MockMarketProvider |
