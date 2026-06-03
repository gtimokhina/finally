from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from market.base import PriceUpdate
from market.cache import PriceCache
from market.massive import MassiveMarketProvider

FIXTURE_RESPONSE = {
    "status": "OK",
    "count": 2,
    "tickers": [
        {
            "ticker": "AAPL",
            "todaysChangePerc": 0.65,
            "todaysChange": 1.21,
            "lastTrade": {"p": 186.73, "s": 100},
            "day":     {"o": 185.50, "h": 187.20, "l": 184.90, "c": 186.73, "v": 52000000},
            "prevDay": {"o": 183.80, "h": 185.40, "l": 183.10, "c": 185.50},
        },
        {
            "ticker": "MSFT",
            "todaysChangePerc": -0.12,
            "todaysChange": -0.50,
            "lastTrade": {"p": 415.00, "s": 50},
            "day":     {"o": 416.00, "h": 417.00, "l": 414.50, "c": 415.00},
            "prevDay": {"c": 415.50},
        },
    ],
}


@pytest.fixture
def provider():
    return MassiveMarketProvider(api_key="test_key", cache=PriceCache())


def _mock_response(body: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = body
    return resp


# ── _parse tests ────────────────────────────────────────────────────────────

def test_parse_uses_last_trade_price(provider):
    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    assert update.ticker == "AAPL"
    assert update.price == 186.73


def test_parse_falls_back_to_day_close_when_no_last_trade(provider):
    snap = {
        "ticker": "MSFT",
        "day": {"c": 415.00},
        "prevDay": {"c": 414.00},
    }
    update = provider._parse(snap)
    assert update.price == 415.00


def test_parse_uses_todays_change_perc_from_api(provider):
    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    assert update.change_pct == 0.65


def test_parse_computes_change_pct_when_missing(provider):
    snap = {
        "ticker": "AAPL",
        "lastTrade": {"p": 200.00},
        "prevDay":   {"c": 196.08},
    }
    update = provider._parse(snap)
    expected = round((200.00 - 196.08) / 196.08 * 100, 4)
    assert abs(update.change_pct - expected) < 0.001


def test_parse_zero_change_when_no_price_data(provider):
    snap = {"ticker": "AAPL"}
    update = provider._parse(snap)
    assert update.change_pct == 0.0


def test_parse_prev_price_comes_from_cache(provider):
    provider._cache.set(PriceUpdate("AAPL", 185.00, 184.00, 0.1, datetime.now(timezone.utc)))
    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    assert update.prev_price == 185.00


def test_parse_prev_price_equals_current_when_no_cache(provider):
    snap = {
        "ticker": "NEW",
        "lastTrade": {"p": 50.00},
        "prevDay": {"c": 49.00},
    }
    update = provider._parse(snap)
    assert update.prev_price == 50.00


def test_parse_timestamp_is_utc(provider):
    snap = FIXTURE_RESPONSE["tickers"][0]
    update = provider._parse(snap)
    assert update.timestamp.tzinfo is not None


def test_parse_rounds_to_4_decimal_places(provider):
    snap = {
        "ticker": "AAPL",
        "lastTrade": {"p": 186.123456789},
        "prevDay": {"c": 185.0},
    }
    update = provider._parse(snap)
    assert update.price == round(186.123456789, 4)


# ── _fetch_prices tests ─────────────────────────────────────────────────────

async def test_fetch_prices_parses_all_tickers(provider):
    mock_resp = _mock_response(FIXTURE_RESPONSE)
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        results = await provider._fetch_prices(["AAPL", "MSFT"])
    assert len(results) == 2
    tickers = {u.ticker for u in results}
    assert tickers == {"AAPL", "MSFT"}


async def test_fetch_prices_returns_empty_on_request_error(provider):
    with patch.object(provider._client, "get", side_effect=httpx.RequestError("timeout")):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


async def test_fetch_prices_returns_empty_on_http_status_error(provider):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "401 Unauthorized",
        request=MagicMock(),
        response=MagicMock(status_code=401),
    )
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


async def test_fetch_prices_returns_empty_on_api_error_status(provider):
    mock_resp = _mock_response({"status": "ERROR", "error": "NOT_AUTHORIZED"})
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


async def test_fetch_prices_returns_empty_on_json_parse_error(provider):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.side_effect = ValueError("not json")
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        result = await provider._fetch_prices(["AAPL"])
    assert result == []


async def test_fetch_prices_empty_tickers_list_returns_empty(provider):
    # Should not call the API with an empty ticker list — but if called it returns []
    mock_resp = _mock_response({"status": "OK", "tickers": []})
    with patch.object(provider._client, "get", new=AsyncMock(return_value=mock_resp)):
        result = await provider._fetch_prices([])
    assert result == []


# ── Provider configuration tests ────────────────────────────────────────────

def test_default_poll_interval_is_free_tier(provider):
    assert provider.poll_interval_seconds() == MassiveMarketProvider.FREE_TIER_INTERVAL


def test_custom_poll_interval():
    p = MassiveMarketProvider(api_key="k", cache=PriceCache(), poll_interval=5.0)
    assert p.poll_interval_seconds() == 5.0


def test_free_tier_interval_is_15_seconds():
    assert MassiveMarketProvider.FREE_TIER_INTERVAL == 15.0


def test_paid_tier_interval_is_5_seconds():
    assert MassiveMarketProvider.PAID_TIER_INTERVAL == 5.0


async def test_close_releases_client(provider):
    # Should not raise
    await provider.close()
