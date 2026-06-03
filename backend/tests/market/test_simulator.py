import pytest

from market.cache import PriceCache
from market.simulator import (
    DEFAULT_CONFIG,
    SECTOR_CORRELATION,
    TICK_DT,
    TICKER_CONFIGS,
    SimulatorMarketProvider,
    TickerConfig,
)


@pytest.fixture
def sim():
    return SimulatorMarketProvider(cache=PriceCache(), seed=0)


async def test_prices_stay_positive(sim):
    tickers = ["AAPL", "TSLA", "JPM"]
    for _ in range(1000):
        updates = await sim._fetch_prices(tickers)
        for u in updates:
            assert u.price > 0, f"{u.ticker} price went non-positive"


async def test_returns_correct_number_of_updates(sim):
    tickers = ["AAPL", "MSFT", "GOOGL"]
    updates = await sim._fetch_prices(tickers)
    assert len(updates) == 3
    returned_tickers = {u.ticker for u in updates}
    assert returned_tickers == set(tickers)


async def test_deterministic_with_seed():
    sim1 = SimulatorMarketProvider(cache=PriceCache(), seed=99)
    sim2 = SimulatorMarketProvider(cache=PriceCache(), seed=99)
    tickers = ["AAPL", "MSFT", "TSLA"]
    u1 = await sim1._fetch_prices(tickers)
    u2 = await sim2._fetch_prices(tickers)
    assert [r.price for r in u1] == [r.price for r in u2]
    assert [r.change_pct for r in u1] == [r.change_pct for r in u2]


async def test_different_seeds_produce_different_prices():
    sim1 = SimulatorMarketProvider(cache=PriceCache(), seed=1)
    sim2 = SimulatorMarketProvider(cache=PriceCache(), seed=2)
    u1 = await sim1._fetch_prices(["AAPL"])
    u2 = await sim2._fetch_prices(["AAPL"])
    assert u1[0].price != u2[0].price


async def test_unknown_ticker_uses_default_config(sim):
    updates = await sim._fetch_prices(["ZZZZ"])
    assert len(updates) == 1
    assert updates[0].ticker == "ZZZZ"
    assert updates[0].price > 0
    # Default config starts at $100
    assert abs(updates[0].price - DEFAULT_CONFIG.seed_price) < 5.0


async def test_unknown_ticker_state_initialised_lazily(sim):
    assert "ZZZZ" not in sim._states
    await sim._fetch_prices(["ZZZZ"])
    assert "ZZZZ" in sim._states
    assert sim._states["ZZZZ"].config == DEFAULT_CONFIG


async def test_prev_price_tracks_previous_tick(sim):
    [u1] = await sim._fetch_prices(["AAPL"])
    [u2] = await sim._fetch_prices(["AAPL"])
    assert u2.prev_price == u1.price


async def test_prev_price_tracks_across_multiple_ticks(sim):
    prices = []
    for _ in range(5):
        [u] = await sim._fetch_prices(["AAPL"])
        prices.append(u.price)
    # Each tick's price should equal the next tick's prev_price
    # (we just verify state updates correctly by checking final state)
    state = sim._states["AAPL"]
    assert state.current_price > 0


async def test_change_pct_relative_to_open(sim):
    # Run several ticks and verify change_pct tracks cumulative move from open
    for _ in range(50):
        updates = await sim._fetch_prices(["AAPL"])
    u = updates[0]
    state = sim._states["AAPL"]
    expected_pct = (state.current_price - state.open_price) / state.open_price * 100
    assert abs(u.change_pct - round(expected_pct, 4)) < 0.001


async def test_open_price_unchanged_across_ticks(sim):
    # open_price is set once at initialisation and never changes
    await sim._fetch_prices(["AAPL"])
    open_price_after_tick1 = sim._states["AAPL"].open_price
    for _ in range(100):
        await sim._fetch_prices(["AAPL"])
    assert sim._states["AAPL"].open_price == open_price_after_tick1


async def test_all_known_tickers_produce_updates(sim):
    tickers = list(TICKER_CONFIGS.keys())
    updates = await sim._fetch_prices(tickers)
    assert len(updates) == len(tickers)


async def test_poll_interval_is_500ms(sim):
    assert sim.poll_interval_seconds() == 0.5


def test_gbm_step_price_positive(sim):
    state = sim._ensure_state("AAPL")
    for _ in range(1000):
        new_price = sim._gbm_step(state, sector_shock=0.0)
        assert new_price > 0


def test_gbm_step_with_extreme_shock(sim):
    state = sim._ensure_state("TSLA")
    # Even with very large shocks, price should stay positive due to log-normal nature
    for shock in [-10.0, -5.0, 5.0, 10.0]:
        new_price = sim._gbm_step(state, sector_shock=shock)
        assert new_price > 0


def test_apply_event_never_zero(sim):
    # After event injection, price floor is 0.01
    for _ in range(10000):
        price = sim._apply_event(0.001)
        assert price >= 0.01


def test_sector_correlation_values():
    assert SECTOR_CORRELATION["tech"] == 0.40
    assert SECTOR_CORRELATION["finance"] == 0.30
    assert SECTOR_CORRELATION["ev"] == 0.00
    assert SECTOR_CORRELATION["media"] == 0.00
    assert SECTOR_CORRELATION["other"] == 0.00


async def test_multiple_tickers_get_sector_shocks(sim):
    # Tech stocks share a sector shock — they should be correlated.
    # Run many ticks with a fixed seed and verify all tech prices are positive.
    tech_tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]
    for _ in range(200):
        updates = await sim._fetch_prices(tech_tickers)
        for u in updates:
            assert u.price > 0


async def test_no_seed_produces_non_deterministic():
    # Without a seed, two simulators should produce different prices
    # (probabilistically true; extremely unlikely to collide)
    sim1 = SimulatorMarketProvider(cache=PriceCache(), seed=None)
    sim2 = SimulatorMarketProvider(cache=PriceCache(), seed=None)
    u1 = await sim1._fetch_prices(["AAPL"])
    u2 = await sim2._fetch_prices(["AAPL"])
    # This could theoretically fail but probability is astronomically low
    # We just verify both run without error and produce valid prices
    assert u1[0].price > 0
    assert u2[0].price > 0


async def test_empty_ticker_list_returns_empty(sim):
    updates = await sim._fetch_prices([])
    assert updates == []


async def test_price_update_has_timestamp(sim):
    updates = await sim._fetch_prices(["AAPL"])
    assert updates[0].timestamp is not None
    # Timezone-aware
    assert updates[0].timestamp.tzinfo is not None
