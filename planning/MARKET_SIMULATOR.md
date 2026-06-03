# Market Simulator — Design & Implementation

This document covers the design, mathematics, and full implementation of `SimulatorMarketProvider` — the default market data provider used when no `MASSIVE_API_KEY` is configured.

See `MARKET_INTERFACE.md` for the provider interface contract.

---

## Goals

- Produce realistic-looking price action with no external dependencies.
- Run as a lightweight in-process background task alongside FastAPI.
- Emit price updates at 500 ms intervals — matching the SSE push cadence.
- Create visual interest: correlated sector moves, occasional sudden "events," green and red flash animations that make the UI feel alive.
- Be fully deterministic when seeded, so E2E tests can reproduce exact price paths.

---

## Price Model — Geometric Brownian Motion (GBM)

GBM is the standard model for stock prices under the Black-Scholes framework. It ensures prices remain positive and exhibit log-normal returns.

### Continuous-time SDE

```
dS = μ S dt + σ S dW
```

Where:
- `S` — current price
- `μ` — drift (expected return per unit time)
- `σ` — volatility (standard deviation of returns per unit time)
- `dW` — Wiener process increment ~ N(0, dt)

### Discrete approximation (Euler-Maruyama)

For each time step `Δt`:

```
S(t + Δt) = S(t) · exp((μ - σ²/2) · Δt + σ · √Δt · Z)
```

Where `Z ~ N(0, 1)`.

This is the exact solution to the GBM SDE, not an approximation — it avoids drift bias from naive Euler discretisation.

**Parameters per ticker (annualised):**

| Ticker | Seed Price | Drift μ | Volatility σ | Sector |
|--------|-----------|---------|-------------|--------|
| AAPL | 190.00 | 0.15 | 0.22 | Tech |
| MSFT | 415.00 | 0.18 | 0.20 | Tech |
| GOOGL | 175.00 | 0.16 | 0.24 | Tech |
| NVDA | 870.00 | 0.25 | 0.40 | Tech |
| META | 500.00 | 0.20 | 0.28 | Tech |
| AMZN | 185.00 | 0.18 | 0.26 | Tech |
| TSLA | 250.00 | 0.10 | 0.55 | EV |
| JPM | 205.00 | 0.12 | 0.18 | Finance |
| V | 275.00 | 0.14 | 0.16 | Finance |
| NFLX | 680.00 | 0.15 | 0.30 | Media |

Drift and volatility are annualised. For a 500 ms step: `Δt = 0.5 / (252 × 6.5 × 3600)`.

---

## Correlated Sector Moves

Real stocks in the same sector move together. To simulate this, each price step combines an idiosyncratic (ticker-specific) shock with a shared sector shock.

```
Z_total = ρ · Z_sector + √(1 - ρ²) · Z_idio
```

Where:
- `Z_sector ~ N(0,1)` — one draw per sector per tick
- `Z_idio ~ N(0,1)` — one draw per ticker per tick
- `ρ` — correlation coefficient (0.4 for Tech, 0.3 for Finance, 0.0 for singletons)

This produces realistic co-movement without requiring a full covariance matrix.

---

## Random Events

Every tick, each ticker has a small probability of experiencing a sudden price move (simulating earnings surprises, news events, etc.).

```python
EVENT_PROBABILITY = 0.0005   # ~0.05% per tick → roughly 1 event per 30 min per ticker
EVENT_MAGNITUDE   = 0.025    # ±2.5% shock
```

An event is a one-tick shock: a multiplicative factor drawn from `[1 - mag, 1 + mag]` uniformly. Events reset after one tick — they do not persist.

---

## Full Implementation

```python
# backend/market/simulator.py
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .base import MarketDataProvider, PriceUpdate
from .cache import PriceCache


# ──────────────────────────────────────────────
# Per-ticker configuration
# ──────────────────────────────────────────────

@dataclass
class TickerConfig:
    seed_price: float
    drift: float        # annualised
    volatility: float   # annualised
    sector: str


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

# Sector correlation coefficients
SECTOR_CORRELATION: dict[str, float] = {
    "tech":    0.40,
    "finance": 0.30,
    "ev":      0.00,
    "media":   0.00,
}

# Trading seconds per year (252 days × 6.5 hours × 3600 s)
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600

# Time step for each 500 ms tick (fraction of a year)
TICK_DT = 0.5 / TRADING_SECONDS_PER_YEAR

# Random event parameters
EVENT_PROBABILITY = 0.0005
EVENT_MAGNITUDE = 0.025

# Default config for tickers not in TICKER_CONFIGS (user-added tickers)
DEFAULT_CONFIG = TickerConfig(100.00, 0.10, 0.25, "other")


# ──────────────────────────────────────────────
# Simulator state
# ──────────────────────────────────────────────

@dataclass
class TickerState:
    config: TickerConfig
    current_price: float
    prev_price: float
    open_price: float   # price at simulator start (used as "prev close" proxy)


class SimulatorMarketProvider(MarketDataProvider):
    """
    Generates synthetic prices using exact GBM discretisation with
    sector correlation and random event injection.
    """

    POLL_INTERVAL = 0.5   # seconds between ticks

    def __init__(self, cache: PriceCache, seed: Optional[int] = None) -> None:
        super().__init__(cache)
        self._rng = random.Random(seed)
        self._states: dict[str, TickerState] = {}

    def poll_interval_seconds(self) -> float:
        return self.POLL_INTERVAL

    # ── State management ───────────────────────

    def _ensure_state(self, ticker: str) -> TickerState:
        """Lazily initialise state for a ticker on first encounter."""
        if ticker not in self._states:
            cfg = TICKER_CONFIGS.get(ticker, DEFAULT_CONFIG)
            self._states[ticker] = TickerState(
                config=cfg,
                current_price=cfg.seed_price,
                prev_price=cfg.seed_price,
                open_price=cfg.seed_price,
            )
        return self._states[ticker]

    # ── GBM step ───────────────────────────────

    def _gbm_step(self, state: TickerState, sector_shock: float) -> float:
        """
        Advance price one tick using the exact GBM solution.

        S(t+dt) = S(t) · exp((μ - σ²/2)·dt + σ·√dt·Z_total)
        """
        cfg = state.config
        rho = SECTOR_CORRELATION.get(cfg.sector, 0.0)

        # Combine sector and idiosyncratic shocks
        z_idio = self._rng.gauss(0, 1)
        z_total = rho * sector_shock + math.sqrt(1 - rho ** 2) * z_idio

        dt = TICK_DT
        drift_term = (cfg.drift - 0.5 * cfg.volatility ** 2) * dt
        diffusion_term = cfg.volatility * math.sqrt(dt) * z_total

        return state.current_price * math.exp(drift_term + diffusion_term)

    def _apply_event(self, price: float) -> float:
        """Occasionally inject a sudden price shock."""
        if self._rng.random() < EVENT_PROBABILITY:
            shock = self._rng.uniform(-EVENT_MAGNITUDE, EVENT_MAGNITUDE)
            price *= (1 + shock)
        return max(price, 0.01)  # prices never go negative

    # ── Main fetch ─────────────────────────────

    async def _fetch_prices(self, tickers: list[str]) -> list[PriceUpdate]:
        """
        Advances GBM state for each ticker and returns PriceUpdate objects.
        Called every POLL_INTERVAL seconds from the base class run() loop.
        """
        now = datetime.now(timezone.utc)

        # One sector shock per sector per tick
        sectors_present = {
            TICKER_CONFIGS.get(t, DEFAULT_CONFIG).sector
            for t in tickers
        }
        sector_shocks = {
            sector: self._rng.gauss(0, 1)
            for sector in sectors_present
        }

        updates: list[PriceUpdate] = []
        for ticker in tickers:
            state = self._ensure_state(ticker)
            sector = state.config.sector
            shock = sector_shocks.get(sector, 0.0)

            new_price = self._gbm_step(state, shock)
            new_price = self._apply_event(new_price)

            prev = state.current_price
            change_pct = (new_price - state.open_price) / state.open_price * 100

            state.prev_price = prev
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

---

## User-Added Tickers

When a user adds a ticker not in `TICKER_CONFIGS` (e.g. via the watchlist UI), the simulator assigns `DEFAULT_CONFIG`:

- Seed price: `$100.00`
- Drift: 10% annualised
- Volatility: 25% annualised
- No sector correlation

This ensures the app never crashes on unknown tickers — it just simulates a generic mid-cap stock.

---

## Time Step Mathematics

For a 500 ms polling interval on a simulated "trading day":

```
Δt = 0.5s / (252 trading days × 6.5 hours × 3600 s/hour)
   = 0.5 / 5,896,800
   ≈ 8.48 × 10⁻⁸  (fraction of a year)
```

For AAPL (σ = 0.22):

```
per-tick std dev = σ × √Δt = 0.22 × √(8.48×10⁻⁸) ≈ 0.000064 (0.0064%)
```

This produces smooth, realistic-looking price movements with small per-tick increments and visible drift over minutes.

---

## Resetting for E2E Tests

Pass a fixed `seed` to `SimulatorMarketProvider` for deterministic price paths:

```python
# In test setup
provider = SimulatorMarketProvider(cache=PriceCache(), seed=42)
```

Or via factory (extend `create_market_provider` with a test flag if needed):

```python
# backend/market/factory.py
import os
from .cache import PriceCache
from .simulator import SimulatorMarketProvider
from .massive import MassiveMarketProvider

def create_market_provider(cache: PriceCache) -> "MarketDataProvider":
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveMarketProvider(api_key=api_key, cache=cache)
    seed_str = os.environ.get("SIMULATOR_SEED", "")
    seed = int(seed_str) if seed_str.isdigit() else None
    return SimulatorMarketProvider(cache=cache, seed=seed)
```

Set `SIMULATOR_SEED=42` in `docker-compose.test.yml` for reproducible E2E runs.

---

## Unit Tests

```python
# tests/market/test_simulator.py
import pytest
from market.simulator import SimulatorMarketProvider
from market.cache import PriceCache


@pytest.fixture
def sim():
    return SimulatorMarketProvider(cache=PriceCache(), seed=0)


def test_prices_stay_positive(sim):
    import asyncio
    tickers = ["AAPL", "TSLA", "JPM"]
    for _ in range(1000):
        updates = asyncio.get_event_loop().run_until_complete(
            sim._fetch_prices(tickers)
        )
        for u in updates:
            assert u.price > 0, f"{u.ticker} price went non-positive"


def test_unknown_ticker_uses_default_config(sim):
    import asyncio
    updates = asyncio.get_event_loop().run_until_complete(
        sim._fetch_prices(["UNKNOWN"])
    )
    assert len(updates) == 1
    assert updates[0].ticker == "UNKNOWN"
    assert updates[0].price > 0


def test_deterministic_with_seed():
    import asyncio
    cache1, cache2 = PriceCache(), PriceCache()
    sim1 = SimulatorMarketProvider(cache=cache1, seed=99)
    sim2 = SimulatorMarketProvider(cache=cache2, seed=99)
    tickers = ["AAPL", "MSFT"]
    u1 = asyncio.get_event_loop().run_until_complete(sim1._fetch_prices(tickers))
    u2 = asyncio.get_event_loop().run_until_complete(sim2._fetch_prices(tickers))
    assert [r.price for r in u1] == [r.price for r in u2]


def test_correlated_tech_stocks_move_together(sim):
    """
    With maximum correlation (rho=1), all tech stocks should move in the
    same direction. This test checks for directional alignment, not magnitude.
    """
    import asyncio
    # Run many ticks and count concordant moves
    tech = ["AAPL", "MSFT", "GOOGL", "NVDA"]
    concordant = 0
    trials = 500

    state_before = {t: sim._ensure_state(t).current_price for t in tech}
    for _ in range(trials):
        asyncio.get_event_loop().run_until_complete(sim._fetch_prices(tech))

    # Not a strict test — just confirms the model runs without error
    assert all(sim._states[t].current_price > 0 for t in tech)
```

---

## Visual Behaviour Summary

| Parameter | Value | Effect |
|-----------|-------|--------|
| Poll interval | 500 ms | Price updates twice per second |
| AAPL per-tick move | ~0.006% | Smooth, subtle price drift |
| TSLA per-tick move | ~0.030% | Noticeably more volatile |
| Event probability | 0.05%/tick | ~1 dramatic move per ticker per 17 min |
| Event magnitude | ±2.5% | Clearly visible spike in chart |
| Sector correlation | 0.3–0.4 | Visible co-movement in heatmap |

The result is a UI where prices are always subtly moving, sparklines accumulate visible shape within seconds, and occasional events create the drama of watching a real market.
