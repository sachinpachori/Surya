import time
import json
import threading
import asyncio
import random
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from queue import Queue
from typing import List, Dict

import yfinance as yf
import pandas as pd
import websockets
import requests

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
BINANCE_WS_URL = "wss://stream.binance.com:9443"

# Prefer realistic spot/perp market names used in the frontend (USDC quote pairs).
# We will try to refresh this list at runtime from the stats-data endpoint, but
# fall back to this curated list when the endpoint is blocked.
HYPERLIQUID_MARKETS = [
    "BTC-USDC",
    "ETH-USDC",
    "SOL-USDC",
    "MATIC-USDC",
    "LINK-USDC",
    "DOGE-USDC",
    "ADA-USDC",
    "ARB-USDC",
    "S-USDC",
]
YFINANCE_SYMBOLS = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "SBIN.NS",
    "ICICIBANK.NS",
    "LT.NS",
    "BHARTIARTL.NS",
]
YFINANCE_POLL_SYMBOLS = list(YFINANCE_SYMBOLS)
YFINANCE_INDIA_INDEX_SYMBOLS = [
    "^NSEI",
    "^NSEBANK",
    "^CNXIT",
    "^CNXAUTO",
    "^CNXFMCG",
    "^CNXMETAL",
    "^CNXPHARMA",
    "^CNXREALTY",
    "^CNXENERGY",
    "^CNXMEDIA",
    "^CNXPSUBANK",
    "^CNXFIN",
    "^NSEMDCP50",
    "NIFTY_MID_SELECT.NS",
    "^BSESN",
    "NIFTYBEES.NS",
    "BANKBEES.NS",
    "JUNIORBEES.NS",
    "MID150BEES.NS",
]
YFINANCE_US_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
    "AMZN",
    "META",
    "GOOGL",
    "AMD",
    "NFLX",
    "SPY",
    "QQQ",
]
YFINANCE_US_POLL_SYMBOLS = list(YFINANCE_US_SYMBOLS)
YFINANCE_US_INDEX_SYMBOLS = [
    "^GSPC",
    "^DJI",
    "^IXIC",
    "^NDX",
    "^RUT",
    "^VIX",
    "^TNX",
    "^IRX",
    "^FVX",
    "^TYX",
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "VOO",
    "IVV",
    "SPLG",
    "VTI",
    "RSP",
    "SPYG",
    "SPYV",
    "TQQQ",
    "SQQQ",
    "UPRO",
    "SPXU",
]
SP500_FALLBACK_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "BRK-B",
    "AVGO",
    "TSLA",
    "JPM",
    "LLY",
    "V",
    "UNH",
    "XOM",
    "MA",
    "COST",
    "WMT",
    "PG",
    "NFLX",
]
BINANCE_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "MATICUSDT",
    "LINKUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "ARBUSDT",
    "BNBUSDT",
]
TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]

live_price_cache = {}
live_price_lock = threading.Lock()
history_cache = {}
history_cache_lock = threading.Lock()
symbol_cache = {}
symbol_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 20


def get_available_timeframes() -> List[str]:
    return TIMEFRAMES


def get_hyperliquid_symbols() -> List[str]:
    cache_key = "hyperliquid_symbols"
    with symbol_cache_lock:
        cached = symbol_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < 3600:
            return cached["symbols"]

    symbols = []
    try:
        r = requests.post(HYPERLIQUID_INFO_URL, json={"type": "meta"}, timeout=8)
        r.raise_for_status()
        data = r.json()
        for item in data.get("universe", []):
            name = item.get("name")
            if name:
                symbols.append(f"{name}-USDC")
    except Exception:
        pass

    if symbols:
        with symbol_cache_lock:
            symbol_cache[cache_key] = {"symbols": symbols, "timestamp": time.time()}
        return symbols

    try:
        url = "https://stats-data.hyperliquid.xyz/Mainnet/markets"
        r = requests.get(url, timeout=4)
        if r.status_code == 200:
            try:
                j = r.json()
                # expect an array of market objects or strings
                if isinstance(j, dict) and "markets" in j:
                    items = j["markets"]
                else:
                    items = j
                symbols = []
                for it in items:
                    if isinstance(it, str):
                        symbols.append(it)
                    elif isinstance(it, dict):
                        s = it.get("symbol") or it.get("name") or it.get("id")
                        if s:
                            symbols.append(s)
                if symbols:
                    with symbol_cache_lock:
                        symbol_cache[cache_key] = {"symbols": symbols, "timestamp": time.time()}
                    return symbols
            except Exception:
                pass
    except Exception:
        pass
    return HYPERLIQUID_MARKETS


def get_yfinance_symbols() -> List[str]:
    cache_key = "nifty500_symbols"
    with symbol_cache_lock:
        cached = symbol_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < 86400:
            return cached["symbols"]

    urls = [
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,application/csv,text/plain,*/*",
        "Referer": "https://www.nseindia.com/",
    }
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            from io import StringIO
            df = pd.read_csv(StringIO(resp.text))
            symbol_column = "Symbol" if "Symbol" in df.columns else df.columns[0]
            symbols = []
            for raw_symbol in df[symbol_column].dropna().astype(str):
                symbol = raw_symbol.strip().upper()
                if not symbol:
                    continue
                if not symbol.endswith(".NS"):
                    symbol = f"{symbol}.NS"
                symbols.append(symbol)
            if symbols:
                symbols = YFINANCE_INDIA_INDEX_SYMBOLS + sorted(
                    symbol for symbol in set(symbols) if symbol not in YFINANCE_INDIA_INDEX_SYMBOLS
                )
                with symbol_cache_lock:
                    symbol_cache[cache_key] = {"symbols": symbols, "timestamp": time.time()}
                return symbols
        except Exception as exc:
            print(f"nifty500 symbols error from {url} -> {exc}")

    return YFINANCE_INDIA_INDEX_SYMBOLS + YFINANCE_SYMBOLS


def get_yfinance_us_symbols() -> List[str]:
    cache_key = "sp500_symbols"
    with symbol_cache_lock:
        cached = symbol_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < 86400:
            return cached["symbols"]

    symbols = []
    try:
        from io import StringIO
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"},
            timeout=12,
        )
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        for table in tables:
            if "Symbol" not in table.columns:
                continue
            for raw_symbol in table["Symbol"].dropna().astype(str):
                symbol = raw_symbol.strip().upper().replace(".", "-")
                if symbol:
                    symbols.append(symbol)
            break
    except Exception as exc:
        print(f"sp500 symbols error -> {exc}")

    if not symbols:
        symbols = SP500_FALLBACK_SYMBOLS

    full_list = YFINANCE_US_INDEX_SYMBOLS + sorted(
        symbol for symbol in set(symbols) if symbol not in YFINANCE_US_INDEX_SYMBOLS
    )
    with symbol_cache_lock:
        symbol_cache[cache_key] = {"symbols": full_list, "timestamp": time.time()}
    return full_list


def get_binance_symbols() -> List[str]:
    cache_key = "binance_symbols"
    with symbol_cache_lock:
        cached = symbol_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < 3600:
            return cached["symbols"]
    try:
        resp = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        symbols = [
            item["symbol"]
            for item in data.get("symbols", [])
            if item.get("status") == "TRADING" and item.get("quoteAsset") in ("USDT", "USDC")
        ]
        if symbols:
            symbols = sorted(symbols)
            with symbol_cache_lock:
                symbol_cache[cache_key] = {"symbols": symbols, "timestamp": time.time()}
            return symbols
    except Exception as exc:
        print(f"binance symbols error -> {exc}")
    return BINANCE_SYMBOLS


def get_chart_data(source: str, symbol: str, timeframe: str) -> List[Dict]:
    cache_key = (source, symbol, timeframe)
    with history_cache_lock:
        cached = history_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < CACHE_TTL_SECONDS:
            return cached["bars"]

    if source in ("yfinance", "yfinance_india", "yfinance_us"):
        bars = get_yfinance_history(symbol, timeframe)
    elif source == "hyperliquid":
        bars = get_hyperliquid_history(symbol, timeframe)
    elif source == "binance":
        bars = get_binance_history(symbol, timeframe)
    else:
        raise ValueError(f"Unknown source: {source}")

    with history_cache_lock:
        history_cache[cache_key] = {"bars": bars, "timestamp": time.time()}
    return bars


def get_yfinance_history(symbol: str, timeframe: str) -> List[Dict]:
    period_map = {
        "1m": "7d",
        "5m": "30d",
        "15m": "60d",
        "1h": "180d",
        "4h": "365d",
        "1d": "2y",
    }
    if timeframe not in period_map:
        timeframe = "1d"
    period = period_map[timeframe]
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(interval=timeframe, period=period, auto_adjust=False)
        if df.empty:
            df = yf.download(symbol, interval=timeframe, period=period, progress=False)
        if df.empty:
            return []
        return [
            {
                "time": int(idx.timestamp()),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
            for idx, row in df.iterrows()
            if not pd.isna(row["Close"])
        ]
    except Exception as exc:
        print(f"yfinance history error: {symbol} {timeframe} -> {exc}")
        return []


def get_binance_history(symbol: str, timeframe: str) -> List[Dict]:
    interval_map = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    interval = interval_map.get(timeframe, "1h")
    limit = 1000
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        bars = []
        for item in data:
            if len(item) < 6:
                continue
            bars.append({
                "time": int(item[0] / 1000),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            })
        return bars
    except Exception as exc:
        print(f"binance history error: {symbol} {timeframe} -> {exc}")
        return []


def get_hyperliquid_history(symbol: str, timeframe: str) -> List[Dict]:
    interval_map = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    interval_seconds = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }.get(timeframe, 3600)
    bars_to_fetch = {
        "1m": 720,
        "5m": 360,
        "15m": 240,
        "1h": 240,
        "4h": 180,
        "1d": 120,
    }.get(timeframe, 240)
    coin = symbol.split("-")[0] if "-" in symbol else symbol
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - bars_to_fetch * interval_seconds * 1000
    try:
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval_map.get(timeframe, "1h"),
                "startTime": start_ms,
                "endTime": now_ms,
            },
        }
        resp = requests.post(HYPERLIQUID_INFO_URL, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        bars = []
        for item in data:
            bars.append({
                "time": int(item["t"] / 1000),
                "open": float(item["o"]),
                "high": float(item["h"]),
                "low": float(item["l"]),
                "close": float(item["c"]),
                "volume": float(item.get("v", 0)),
            })
        return bars
    except Exception as exc:
        print(f"hyperliquid history error: {symbol} {timeframe} -> {exc}")
        return []


async def _hyperliquid_ws_loop(queue):
    while True:
        try:
            async with websockets.connect(HYPERLIQUID_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                # subscribe using the same frames the frontend sends
                # subscribe to trades and L2 book for each coin (coin = part before '-')
                coins = []
                for market in get_hyperliquid_symbols():
                    coin = market.split("-")[0] if "-" in market else market
                    if coin not in coins:
                        coins.append(coin)
                for coin in coins:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin, "nSigFigs": None}}))

                async for message in ws:
                    try:
                        data = json.loads(message)
                    except Exception:
                        continue

                    # best-effort extraction of latest price and symbol
                    price = None
                    volume = None
                    symbol = None
                    if isinstance(data, dict):
                        # common patterns: {"method":"update","params":{...}} or Hyperliquid channel frames
                        if data.get("channel") == "subscriptionResponse":
                            continue
                        payload = data.get("data")
                        if isinstance(payload, dict):
                            symbol = payload.get("market") or payload.get("coin") or payload.get("symbol")
                            price = payload.get("price") or payload.get("last") or payload.get("close") or payload.get("p")
                            if price is None and isinstance(payload.get("trades"), list) and payload["trades"]:
                                t = payload["trades"][-1]
                                price = t.get("price") or t.get("px") or t.get("p")
                                volume = t.get("size") or t.get("sz") or t.get("qty") or t.get("q")
                                symbol = symbol or t.get("market") or t.get("coin") or t.get("symbol")
                        elif isinstance(payload, list):
                            for item in reversed(payload):
                                if isinstance(item, dict):
                                    price = price or item.get("price") or item.get("px") or item.get("p")
                                    volume = volume or item.get("size") or item.get("sz") or item.get("qty") or item.get("q")
                                    symbol = symbol or item.get("market") or item.get("coin") or item.get("symbol")
                                    if price is not None and symbol is not None:
                                        break
                        else:
                            symbol = data.get("coin") or data.get("symbol") or data.get("market")
                            price = data.get("price") or data.get("last") or data.get("close") or data.get("p")
                            if isinstance(data.get("trades"), list) and data["trades"]:
                                t = data["trades"][-1]
                                price = price or t.get("price") or t.get("px") or t.get("p")
                                volume = volume or t.get("size") or t.get("sz") or t.get("qty") or t.get("q")
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                if not symbol:
                                    symbol = item.get("coin") or item.get("symbol") or item.get("market")
                                price = price or item.get("price") or item.get("px") or item.get("p")
                                volume = volume or item.get("size") or item.get("sz") or item.get("qty") or item.get("q")

                    if price is None:
                        continue
                    try:
                        price = float(price)
                    except Exception:
                        continue

                    # map coin to market (prefer the curated market name)
                    market_symbol = None
                    if symbol:
                        for m in get_hyperliquid_symbols():
                            if m.startswith(symbol + "-") or m == symbol:
                                market_symbol = m
                                break
                    if not market_symbol:
                        market_symbol = symbol or "UNKNOWN"

                    event = {
                        "source": "hyperliquid",
                        "symbol": market_symbol,
                        "price": price,
                        "volume": float(volume) if volume is not None else 0,
                        "timestamp": int(time.time()),
                    }
                    with live_price_lock:
                        live_price_cache[market_symbol] = {"price": price, "timestamp": time.time()}
                    queue.put(event)
        except Exception:
            await asyncio.sleep(10)


def start_hyperliquid_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    queue = Queue()

    async def runner():
        await _hyperliquid_ws_loop(queue)

    threading.Thread(target=lambda: loop.run_until_complete(runner()), daemon=True).start()
    while True:
        try:
            event = queue.get(timeout=1)
            yield event
        except Exception:
            continue


def get_hyperliquid_live_prices():
    yield from start_hyperliquid_feed()


def get_market_status(source: str, symbol: str) -> Dict:
    """Return market open status and current time for a given source/symbol.

    - For `yfinance` Indian symbols (ending with .NS) assume NSE hours 09:15-15:30 local time Mon-Fri.
    - For crypto sources (binance, hyperliquid) markets are treated as always open.
    """
    try:
        # Choose timezone per-exchange: NSE uses Asia/Kolkata, crypto markets are always open
        if source in ("yfinance", "yfinance_india") and symbol.endswith(".NS"):
            if ZoneInfo:
                tz = ZoneInfo("Asia/Kolkata")
                now_tz = datetime.now(tz)
                tzname = tz.key
            else:
                now_tz = datetime.now()
                tzname = now_tz.tzname() or "local"
            current_iso = now_tz.isoformat()
            weekday = now_tz.weekday()
            open_dt = now_tz.replace(hour=9, minute=15, second=0, microsecond=0)
            close_dt = now_tz.replace(hour=15, minute=30, second=0, microsecond=0)
            is_open = (weekday < 5) and (open_dt <= now_tz <= close_dt)
            return {
                "open": bool(is_open),
                "current_time": current_iso,
                "timezone": tzname,
                "open_time": "09:15",
                "close_time": "15:30",
            }

        if source == "yfinance_us":
            if ZoneInfo:
                tz = ZoneInfo("America/New_York")
                now_tz = datetime.now(tz)
                tzname = tz.key
            else:
                now_tz = datetime.now()
                tzname = now_tz.tzname() or "local"
            weekday = now_tz.weekday()
            open_dt = now_tz.replace(hour=9, minute=30, second=0, microsecond=0)
            close_dt = now_tz.replace(hour=16, minute=0, second=0, microsecond=0)
            is_open = (weekday < 5) and (open_dt <= now_tz <= close_dt)
            return {
                "open": bool(is_open),
                "current_time": now_tz.isoformat(),
                "timezone": tzname,
                "open_time": "09:30",
                "close_time": "16:00",
            }

        # crypto markets: always open
        now = datetime.now().astimezone()
        tzname = now.tzname() or "local"
        current_iso = now.isoformat()
        if source in ("binance", "hyperliquid"):
            return {"open": True, "current_time": current_iso, "timezone": tzname}

        # fallback conservative local hours
        weekday = now.weekday()
        open_dt = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_dt = now.replace(hour=17, minute=0, second=0, microsecond=0)
        is_open = (weekday < 5) and (open_dt <= now <= close_dt)
        return {"open": bool(is_open), "current_time": current_iso, "timezone": tzname}
    except Exception:
        return {"open": False, "current_time": datetime.utcnow().isoformat(), "timezone": "UTC"}


def get_live_price(source: str, symbol: str) -> Dict:
    """Return the latest live price for a source/symbol from the in-memory cache."""
    with live_price_lock:
        # try direct key
        p = live_price_cache.get(symbol)
        if p:
            return {"price": p.get("price"), "timestamp": p.get("timestamp"), "source": source, "symbol": symbol}
        # try mapping for hyperliquid where cache keys use curated market names
        for k, v in live_price_cache.items():
            if isinstance(k, str) and (k == symbol or k.replace('-', '') == symbol.replace('-', '')):
                return {"price": v.get("price"), "timestamp": v.get("timestamp"), "source": source, "symbol": k}

    try:
        if source == "binance":
            resp = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5)
            resp.raise_for_status()
            price = float(resp.json()["price"])
            with live_price_lock:
                live_price_cache[symbol] = {"price": price, "timestamp": time.time()}
            return {"price": price, "timestamp": int(time.time()), "source": source, "symbol": symbol}

        if source == "hyperliquid":
            coin = symbol.split("-")[0] if "-" in symbol else symbol
            resp = requests.post(HYPERLIQUID_INFO_URL, json={"type": "allMids"}, timeout=5)
            resp.raise_for_status()
            mids = resp.json()
            if coin in mids:
                price = float(mids[coin])
                with live_price_lock:
                    live_price_cache[symbol] = {"price": price, "timestamp": time.time()}
                return {"price": price, "timestamp": int(time.time()), "source": source, "symbol": symbol}

        if source in ("yfinance", "yfinance_india", "yfinance_us"):
            df = yf.Ticker(symbol).history(period="1d", interval="1m")
            if df is not None and not df.empty:
                last = df.iloc[-1]
                price = float(last["Close"])
                ts = int(last.name.timestamp())
                with live_price_lock:
                    live_price_cache[symbol] = {"price": price, "timestamp": time.time()}
                return {
                    "price": price,
                    "timestamp": ts,
                    "volume": float(last.get("Volume", 0)),
                    "source": source,
                    "symbol": symbol,
                }
    except Exception as exc:
        return {"price": None, "timestamp": None, "source": source, "symbol": symbol, "error": str(exc)}
    return {"price": None, "timestamp": None, "source": source, "symbol": symbol}


def _binance_ws_loop(queue: Queue):
    async def run():
        # combined stream for trades
        streams = "/".join([f"{s.lower()}@trade" for s in BINANCE_SYMBOLS])
        url = f"{BINANCE_WS_URL}/stream?streams={streams}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    async for message in ws:
                        try:
                            j = json.loads(message)
                        except Exception:
                            continue
                        # Binance combined stream wraps data in {stream, data}
                        data = j.get("data") if isinstance(j, dict) else None
                        if not data:
                            continue
                        symbol = data.get("s")
                        price = data.get("p")
                        ts = data.get("T") or int(time.time() * 1000)
                        try:
                            price = float(price)
                        except Exception:
                            continue
                        qty = data.get("q") or 0
                        event = {
                            "source": "binance",
                            "symbol": symbol,
                            "price": price,
                            "volume": float(qty),
                            "timestamp": int(ts / 1000),
                        }
                        with live_price_lock:
                            live_price_cache[symbol] = {"price": price, "timestamp": time.time()}
                        queue.put(event)
            except Exception:
                await asyncio.sleep(5)

    return run()


def _yfinance_poller(queue: Queue, poll_interval: int = 10):
    # Poll latest price for configured yfinance symbols periodically
    while True:
        try:
            for sym in YFINANCE_POLL_SYMBOLS + YFINANCE_US_POLL_SYMBOLS:
                try:
                    ticker = yf.Ticker(sym)
                    # try to get the most recent minute bar
                    df = ticker.history(period="1d", interval="1m")
                    if df is None or df.empty:
                        continue
                    last = df.iloc[-1]
                    price = float(last["Close"])
                    event_source = "yfinance_india" if sym.endswith(".NS") else "yfinance_us"
                    event = {
                        "source": event_source,
                        "symbol": sym,
                        "price": price,
                        "volume": float(last.get("Volume", 0)),
                        "timestamp": int(last.name.timestamp()),
                    }
                    with live_price_lock:
                        live_price_cache[sym] = {"price": price, "timestamp": time.time()}
                    queue.put(event)
                except Exception:
                    continue
            time.sleep(poll_interval)
        except Exception:
            time.sleep(poll_interval)


def start_all_feeds():
    """Start hyperliquid + binance websockets and yfinance poller, yield unified events."""
    # queue used to surface events to caller
    queue = Queue()

    # start yfinance poller in a background thread
    threading.Thread(target=_yfinance_poller, args=(queue,), daemon=True).start()

    # start websocket loops (hyperliquid + binance) on an asyncio loop in a thread
    def ws_runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def runner():
            # schedule both ws coroutines concurrently
            tasks = [
                _hyperliquid_ws_loop(queue),
                _binance_ws_loop(queue),
            ]
            await asyncio.gather(*tasks)

        try:
            loop.run_until_complete(runner())
        except Exception:
            pass

    threading.Thread(target=ws_runner, daemon=True).start()

    # yield events to caller
    while True:
        try:
            ev = queue.get(timeout=1)
            yield ev
        except Exception:
            continue
