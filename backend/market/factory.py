from __future__ import annotations

import os

from .base import MarketDataProvider
from .cache import PriceCache
from .massive import MassiveMarketProvider
from .simulator import SimulatorMarketProvider


def create_market_provider(cache: PriceCache) -> MarketDataProvider:
    """
    Selects the appropriate provider based on environment variables.

    LLM_MOCK=true              →  MockMarketProvider (deterministic, for E2E tests)
    MASSIVE_API_KEY set        →  MassiveMarketProvider (free tier: 15s interval)
    MASSIVE_API_KEY absent     →  SimulatorMarketProvider
    SIMULATOR_SEED set         →  deterministic simulator (for E2E tests)
    MASSIVE_POLL_INTERVAL set  →  override Massive poll interval (paid tiers)
    """
    if os.environ.get("LLM_MOCK", "").lower() == "true":
        from .mock_provider import MockMarketProvider
        return MockMarketProvider(cache=cache)

    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval_str = os.environ.get("MASSIVE_POLL_INTERVAL", "")
        try:
            interval = float(interval_str)
        except ValueError:
            interval = MassiveMarketProvider.FREE_TIER_INTERVAL
        return MassiveMarketProvider(api_key=api_key, cache=cache, poll_interval=interval)

    seed_str = os.environ.get("SIMULATOR_SEED", "")
    seed = int(seed_str) if seed_str.isdigit() else None
    return SimulatorMarketProvider(cache=cache, seed=seed)
