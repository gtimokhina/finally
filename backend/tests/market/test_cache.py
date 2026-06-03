from datetime import datetime, timezone

import pytest

from market.base import PriceUpdate
from market.cache import CachedPrice, PriceCache


def _make_update(ticker: str, price: float, prev: float = 0.0, change: float = 0.0) -> PriceUpdate:
    return PriceUpdate(
        ticker=ticker,
        price=price,
        prev_price=prev,
        change_pct=change,
        timestamp=datetime.now(timezone.utc),
    )


def test_set_and_get():
    cache = PriceCache()
    cache.set(_make_update("AAPL", 190.0, 189.5, 0.26))
    result = cache.get("AAPL")
    assert result is not None
    assert result.ticker == "AAPL"
    assert result.price == 190.0
    assert result.prev_price == 189.5
    assert result.change_pct == 0.26


def test_get_nonexistent_returns_none():
    cache = PriceCache()
    assert cache.get("UNKNOWN") is None


def test_has_returns_true_after_set():
    cache = PriceCache()
    assert not cache.has("AAPL")
    cache.set(_make_update("AAPL", 190.0))
    assert cache.has("AAPL")


def test_has_returns_false_for_missing():
    cache = PriceCache()
    assert not cache.has("ZZZZ")


def test_upsert_replaces_existing():
    cache = PriceCache()
    cache.set(_make_update("AAPL", 190.0))
    cache.set(_make_update("AAPL", 195.0))
    result = cache.get("AAPL")
    assert result.price == 195.0


def test_snapshot_returns_all_entries():
    cache = PriceCache()
    cache.set(_make_update("AAPL", 190.0))
    cache.set(_make_update("MSFT", 415.0))
    cache.set(_make_update("TSLA", 250.0))
    snap = cache.snapshot()
    assert len(snap) == 3
    tickers = {p.ticker for p in snap}
    assert tickers == {"AAPL", "MSFT", "TSLA"}


def test_snapshot_is_independent_copy():
    cache = PriceCache()
    cache.set(_make_update("AAPL", 190.0))
    snap1 = cache.snapshot()
    cache.set(_make_update("AAPL", 195.0))
    snap2 = cache.snapshot()
    # The first snapshot should not be affected by the later write
    assert snap1[0].price == 190.0
    assert snap2[0].price == 195.0


def test_get_all_returns_shallow_copy():
    cache = PriceCache()
    cache.set(_make_update("AAPL", 190.0))
    cache.set(_make_update("MSFT", 415.0))
    all_prices = cache.get_all()
    assert isinstance(all_prices, dict)
    assert "AAPL" in all_prices
    assert "MSFT" in all_prices
    # Mutating the returned dict does not affect the cache
    del all_prices["AAPL"]
    assert cache.has("AAPL")


def test_set_stores_correct_updated_at():
    ts = datetime(2026, 6, 3, 12, 0, 0, tzinfo=timezone.utc)
    update = PriceUpdate(ticker="AAPL", price=190.0, prev_price=189.0, change_pct=0.1, timestamp=ts)
    cache = PriceCache()
    cache.set(update)
    cached = cache.get("AAPL")
    assert cached.updated_at == ts


def test_snapshot_empty_when_no_data():
    cache = PriceCache()
    assert cache.snapshot() == []
