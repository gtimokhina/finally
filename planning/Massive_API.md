# Massive API (formerly Polygon.io) — Market Data Reference

Massive.com rebranded from Polygon.io on October 30, 2025. Existing API keys, endpoints at `api.polygon.io`, and Python SDK code continue to work during the transition period. The new base URL is `api.massive.com`.

## Authentication

All endpoints require an API key passed as a query parameter or via the `Authorization: Bearer <key>` header.

```
https://api.polygon.io/v2/snapshot/...?apiKey=YOUR_KEY
```

Or with the official Python client:

```python
from massive import RESTClient

client = RESTClient(api_key="YOUR_KEY")  # pip install -U massive
```

The legacy package also works:

```python
# pip install polygon-api-client  (v1.16.3, still maintained)
from polygon import RESTClient
client = RESTClient("YOUR_KEY")
```

---

## Pricing Tiers

| Tier | Real-time data | REST rate limit |
|------|---------------|-----------------|
| Free | Delayed (15 min) | 5 requests/min |
| Starter / Developer / Advanced | Real-time | Unlimited |

For this project, the free tier is sufficient for development with the simulator. A paid tier is required for live real-time prices via the Massive API path.

---

## Key Endpoints

### 1. Full Market Snapshot — Multiple Tickers

Retrieves the current snapshot (day bar, previous day, minute bar, last trade, last quote) for one or many tickers in a single call. **This is the primary endpoint for polling live prices.**

```
GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers
```

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tickers` | string | No | Comma-separated list of tickers, e.g. `AAPL,MSFT,TSLA`. Omit for all tickers. |
| `include_otc` | boolean | No | Include OTC securities. Default: `false`. |
| `apiKey` | string | Yes | Your API key. |

**Response Schema**

```json
{
  "status": "OK",
  "count": 2,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": 1.23,
      "todaysChangePerc": 0.65,
      "updated": 1605192894630916600,
      "day": {
        "o": 185.50,
        "h": 187.20,
        "l": 184.90,
        "c": 186.73,
        "v": 52341000,
        "vw": 186.12
      },
      "prevDay": {
        "o": 183.80,
        "h": 185.40,
        "l": 183.10,
        "c": 185.50,
        "v": 48920000,
        "vw": 184.65
      },
      "min": {
        "o": 186.50,
        "h": 186.80,
        "l": 186.20,
        "c": 186.73,
        "v": 145000,
        "vw": 186.55
      },
      "lastTrade": {
        "p": 186.73,
        "s": 100,
        "t": 1605192894630916600
      },
      "lastQuote": {
        "P": 186.74,
        "p": 186.72,
        "t": 1605192894630916600
      }
    }
  ]
}
```

**Field Reference (day / prevDay / min objects)**

| Field | Description |
|-------|-------------|
| `o` | Open price |
| `h` | High price |
| `l` | Low price |
| `c` | Close / current price |
| `v` | Volume |
| `vw` | Volume-weighted average price (VWAP) |

**lastTrade fields**

| Field | Description |
|-------|-------------|
| `p` | Trade price |
| `s` | Trade size (shares) |
| `t` | Nanosecond Unix timestamp |

**Python example (raw requests)**

```python
import requests

API_KEY = "your_key"
TICKERS = ["AAPL", "MSFT", "TSLA", "NVDA"]

resp = requests.get(
    "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers",
    params={
        "tickers": ",".join(TICKERS),
        "apiKey": API_KEY,
    },
    timeout=10,
)
resp.raise_for_status()
data = resp.json()

for ticker_snap in data["tickers"]:
    print(ticker_snap["ticker"], ticker_snap["day"]["c"], ticker_snap["todaysChangePerc"])
```

**Python example (official client)**

```python
from massive import RESTClient

client = RESTClient(api_key="your_key")

# get_snapshot_all_tickers accepts a list of tickers
snapshots = client.get_snapshot_all_tickers(
    "stocks",
    tickers=["AAPL", "MSFT", "TSLA"],
)
for snap in snapshots.tickers:
    print(snap.ticker, snap.day.c, snap.todays_change_perc)
```

---

### 2. Previous Day Bar — Single Ticker

Returns the prior trading day's OHLCV data for a single ticker. Useful for computing day-change% when the market is closed or snapshot data is unavailable.

```
GET https://api.polygon.io/v2/aggs/ticker/{ticker}/prev
```

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ticker` | string | Yes | Case-sensitive ticker symbol, e.g. `AAPL` |

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `adjusted` | boolean | No | Split-adjusted prices. Default: `true`. |
| `apiKey` | string | Yes | Your API key. |

**Response Schema**

```json
{
  "ticker": "AAPL",
  "status": "OK",
  "adjusted": true,
  "resultsCount": 1,
  "results": [
    {
      "T": "AAPL",
      "o": 115.55,
      "h": 117.59,
      "l": 114.13,
      "c": 115.97,
      "v": 131704427,
      "vw": 116.3058,
      "t": 1605042000000
    }
  ]
}
```

**Field Reference**

| Field | Description |
|-------|-------------|
| `T` | Ticker symbol |
| `o` | Open price |
| `h` | High price |
| `l` | Low price |
| `c` | Close price |
| `v` | Volume |
| `vw` | VWAP |
| `t` | Unix millisecond timestamp of period start |

**Python example**

```python
import requests

def get_prev_close(ticker: str, api_key: str) -> float:
    resp = requests.get(
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev",
        params={"adjusted": "true", "apiKey": api_key},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json()["results"]
    return results[0]["c"] if results else None
```

---

### 3. Aggregate Bars (OHLCV) — Historical / Intraday

Returns OHLCV bars for a ticker over a custom date range and time interval. Use this to seed sparklines, charts, and backtesting data.

```
GET https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | string | Case-sensitive ticker symbol |
| `multiplier` | integer | Size of the timespan multiplier (e.g. `1`, `5`, `15`) |
| `timespan` | string | Time window: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year` |
| `from` | string | Start date `YYYY-MM-DD` or Unix ms timestamp |
| `to` | string | End date `YYYY-MM-DD` or Unix ms timestamp |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `adjusted` | boolean | `true` | Split-adjusted prices |
| `sort` | string | `asc` | Sort order: `asc` (oldest first) or `desc` |
| `limit` | integer | `5000` | Max results (hard cap: 50,000) |
| `apiKey` | string | — | Your API key |

**Response Schema**

```json
{
  "ticker": "AAPL",
  "status": "OK",
  "adjusted": true,
  "queryCount": 5,
  "resultsCount": 5,
  "results": [
    {
      "o": 74.06,
      "h": 75.15,
      "l": 73.80,
      "c": 75.09,
      "v": 135647456,
      "vw": 74.61,
      "t": 1577941200000,
      "n": 1234
    }
  ]
}
```

**Field Reference**

| Field | Description |
|-------|-------------|
| `o` | Open price |
| `h` | High price |
| `l` | Low price |
| `c` | Close price |
| `v` | Volume |
| `vw` | VWAP |
| `t` | Unix millisecond timestamp (start of bar) |
| `n` | Number of transactions in the bar |

**Python example — 1-minute bars for the last trading day**

```python
import requests
from datetime import date, timedelta

def get_intraday_bars(
    ticker: str,
    api_key: str,
    from_date: str,
    to_date: str,
    multiplier: int = 1,
    timespan: str = "minute",
) -> list[dict]:
    resp = requests.get(
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
        params={
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])
```

---

### 4. Last Trade — Single Ticker

Returns the most recent trade for a ticker. Lower latency than the snapshot when you only need the last price.

```
GET https://api.polygon.io/v2/last/trade/{ticker}
```

**Response Schema**

```json
{
  "status": "OK",
  "results": {
    "T": "AAPL",
    "p": 186.73,
    "s": 100,
    "t": 1617901342969834000
  }
}
```

| Field | Description |
|-------|-------------|
| `T` | Ticker symbol |
| `p` | Trade price |
| `s` | Trade size |
| `t` | Nanosecond SIP Unix timestamp |

---

### 5. Top Market Movers (Gainers / Losers)

Returns the top 20 gainers or losers of the day. Useful for seeding watchlist suggestions.

```
GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/{direction}
```

**Path Parameters**

| Parameter | Values | Description |
|-----------|--------|-------------|
| `direction` | `gainers`, `losers` | Filter direction |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_otc` | boolean | `false` | Include OTC securities |

---

## Polling Strategy for This Project

Because the free tier allows only 5 requests/minute, the recommended polling strategy is:

| Tier | Interval | Strategy |
|------|----------|----------|
| Free (dev/demo) | 15 s | Single snapshot call for all tickers |
| Paid (production) | 2–5 s | Single snapshot call for all tickers |

The snapshot endpoint is the most efficient choice: one HTTP call retrieves all tickers. With 10–20 watchlist tickers and 15-second polling on the free tier, the project stays within rate limits.

```python
# Efficient: one call for all watchlist tickers
params = {
    "tickers": ",".join(watchlist),   # "AAPL,MSFT,TSLA,..."
    "apiKey": API_KEY,
}
```

---

## Error Handling

The API returns HTTP 200 with `"status": "ERROR"` for logical errors, and HTTP 4xx/5xx for protocol errors.

```python
import requests
from requests.exceptions import HTTPError, Timeout

def safe_fetch_snapshots(tickers: list[str], api_key: str) -> dict:
    try:
        resp = requests.get(
            "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"tickers": ",".join(tickers), "apiKey": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "OK":
            raise ValueError(f"API error: {body.get('error', 'unknown')}")
        return {snap["ticker"]: snap for snap in body.get("tickers", [])}
    except (HTTPError, Timeout, ValueError) as exc:
        # Log and return empty — caller falls back to cache
        print(f"[massive] fetch failed: {exc}")
        return {}
```

---

## Notes

- **Base URL**: Both `https://api.polygon.io` and `https://api.massive.com` resolve to the same backend. Use `api.polygon.io` until Massive announces end-of-life for the legacy hostname.
- **Timestamps**: `lastTrade.t` and `lastQuote.t` are nanoseconds. `results[].t` in aggregates is milliseconds.
- **Market hours**: Snapshot `day` data accumulates from 4 AM ET (pre-market). `prevDay` is static until the current day closes.
- **OTC tickers**: Set `include_otc=false` (default) to exclude pink sheets and avoid spurious prices.
