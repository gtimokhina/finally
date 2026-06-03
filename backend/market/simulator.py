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
    drift: float       # annualised (e.g. 0.15 = 15%/yr)
    volatility: float  # annualised (e.g. 0.22 = 22%/yr)
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

# Trading seconds per year: 252 days × 6.5 hours × 3600 s
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600

# Each 500 ms tick expressed as a fraction of a trading year
TICK_DT = 0.5 / TRADING_SECONDS_PER_YEAR

EVENT_PROBABILITY = 0.0005  # ~1 event per ticker per 17 minutes at 500ms ticks
EVENT_MAGNITUDE   = 0.025   # ±2.5% shock


@dataclass
class TickerState:
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
        self._states: dict[str, TickerState] = {}

    def poll_interval_seconds(self) -> float:
        return self.POLL_INTERVAL

    def _ensure_state(self, ticker: str) -> TickerState:
        """Lazily create state on first encounter (handles user-added tickers)."""
        if ticker not in self._states:
            cfg = TICKER_CONFIGS.get(ticker, DEFAULT_CONFIG)
            self._states[ticker] = TickerState(
                config=cfg,
                current_price=cfg.seed_price,
                prev_price=cfg.seed_price,
                open_price=cfg.seed_price,
            )
        return self._states[ticker]

    def _gbm_step(self, state: TickerState, sector_shock: float) -> float:
        """
        Advance price by one tick using the exact GBM discretisation:
            S(t+dt) = S(t) · exp((μ - σ²/2)·dt + σ·√dt·Z_total)
        """
        cfg = state.config
        rho = SECTOR_CORRELATION.get(cfg.sector, 0.0)

        z_idio = self._rng.gauss(0, 1)
        # Orthogonal combination ensures total variance = 1
        z_total = rho * sector_shock + math.sqrt(max(1 - rho ** 2, 0)) * z_idio

        drift_term     = (cfg.drift - 0.5 * cfg.volatility ** 2) * TICK_DT
        diffusion_term = cfg.volatility * math.sqrt(TICK_DT) * z_total

        return state.current_price * math.exp(drift_term + diffusion_term)

    def _apply_event(self, price: float) -> float:
        """Inject an occasional sudden price shock for visual drama."""
        if self._rng.random() < EVENT_PROBABILITY:
            shock = self._rng.uniform(-EVENT_MAGNITUDE, EVENT_MAGNITUDE)
            price *= (1 + shock)
        return max(price, 0.01)  # prices never go to zero

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
            new_price = self._apply_event(new_price)

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
