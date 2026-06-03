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
    One HTTP call fetches all watched tickers simultaneously.
    Falls back gracefully on HTTP errors — the cache retains stale values.
    """

    FREE_TIER_INTERVAL = 15.0   # 5 req/min limit on free tier
    PAID_TIER_INTERVAL =  5.0   # safe for unlimited tiers

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

        except (httpx.HTTPError, ValueError) as exc:
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
