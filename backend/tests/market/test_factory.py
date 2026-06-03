import random

import pytest

from market.cache import PriceCache
from market.factory import create_market_provider
from market.massive import MassiveMarketProvider
from market.mock_provider import MockMarketProvider
from market.simulator import SimulatorMarketProvider


def test_selects_massive_when_key_set(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "real_key_123")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, MassiveMarketProvider)


def test_selects_simulator_when_key_absent(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MOCK", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)


def test_selects_simulator_when_key_empty(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)


def test_selects_simulator_when_key_whitespace_only(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "   ")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)


def test_selects_mock_when_llm_mock_true(monkeypatch):
    monkeypatch.setenv("LLM_MOCK", "true")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, MockMarketProvider)


def test_selects_mock_takes_priority_over_massive(monkeypatch):
    monkeypatch.setenv("LLM_MOCK", "true")
    monkeypatch.setenv("MASSIVE_API_KEY", "some_key")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, MockMarketProvider)


def test_simulator_seed_from_env(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MOCK", raising=False)
    monkeypatch.setenv("SIMULATOR_SEED", "42")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)
    # Verify the seed was applied: same seed → same initial RNG state
    expected_first_value = random.Random(42).random()
    assert provider._rng.random() == expected_first_value


def test_simulator_non_numeric_seed_ignored(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MOCK", raising=False)
    monkeypatch.setenv("SIMULATOR_SEED", "not_a_number")
    provider = create_market_provider(PriceCache())
    assert isinstance(provider, SimulatorMarketProvider)
    # seed=None → non-deterministic, just verify it doesn't crash
    assert provider._rng is not None


def test_massive_default_interval(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    monkeypatch.delenv("MASSIVE_POLL_INTERVAL", raising=False)
    provider = create_market_provider(PriceCache())
    assert provider.poll_interval_seconds() == MassiveMarketProvider.FREE_TIER_INTERVAL


def test_massive_custom_interval(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "5")
    provider = create_market_provider(PriceCache())
    assert provider.poll_interval_seconds() == 5.0


def test_massive_invalid_interval_falls_back_to_free_tier(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.delenv("LLM_MOCK", raising=False)
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "not_a_float")
    provider = create_market_provider(PriceCache())
    assert provider.poll_interval_seconds() == MassiveMarketProvider.FREE_TIER_INTERVAL


def test_provider_receives_shared_cache(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MOCK", raising=False)
    cache = PriceCache()
    provider = create_market_provider(cache)
    assert provider._cache is cache
