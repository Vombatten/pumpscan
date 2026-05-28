"""
binance_feed.py — Real-time Binance kline feed via WebSocket

Henter live OHLCV data direkte fra Binance WebSocket streams.
Ingen API-nøgle, nul forsinkelse.

Brug:
    feed = BinanceFeed()
    feed.subscribe(["PEPEUSDT","WIFUSDT","BONKUSDT"], interval="1h")
    feed.start()
    df = feed.get_ohlcv("PEPEUSDT", "1h")
"""

import json, time, threading, logging
import websocket
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict, deque

logging.basicConfig(level=logging.WARNING)

BINANCE_WS   = "wss://stream.binance.com:9443/stream"
BYBIT_WS     = "wss://stream.bybit.com/v5/public/spot"
BINANCE_REST = "https://api.binance.com/api/v3"
BYBIT_REST   = "https://api.bybit.com/v5/market"

# Max candles i RAM per symbol
MAX_CANDLES = 500


class BinanceFeed:
    """
    Real-time Binance kline feed.
    Kombinerer initial historik (REST) med live updates (WebSocket).
    """

    def __init__(self):
        self._candles   = defaultdict(lambda: defaultdict(dict))
        # {symbol: {interval: {open_time: {o,h,l,c,v,closed}}}}
        self._lock      = threading.RLock()
        self._ws        = None
        self._ws_thread = None
        self._running   = False
        self._symbols   = []
        self._interval  = "1h"
        self._ready     = threading.Event()
        self._error     = None

    # ─────────────────────────────────────────────
    #  REST: Historisk data (seed)
    # ─────────────────────────────────────────────

    def _fetch_history(self, symbol: str, interval: str, limit: int = 200):
        """Henter historisk data — Bybit først, Binance fallback."""
        import urllib.request

        # ── Bybit interval mapping ──
        iv_map = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30",
                  "1h":"60","2h":"120","4h":"240","1d":"D"}
        bybit_iv = iv_map.get(interval, "60")

        # ── Prøv Bybit ──
        try:
            url = (f"{BYBIT_REST}/kline?category=spot&symbol={symbol.upper()}"
                   f"&interval={bybit_iv}&limit={limit}")
            with urllib.request.urlopen(url, timeout=10) as r:
                data = json.loads(r.read())
            klines = data.get("result", {}).get("list", [])
            if klines:
                with self._lock:
                    for k in klines:
                        # Bybit format: [startTime, open, high, low, close, volume, turnover]
                        ot = int(k[0])
                        self._candles[symbol][interval][ot] = {
                            "open_time": ot,
                            "open":   float(k[1]),
                            "high":   float(k[2]),
                            "low":    float(k[3]),
                            "close":  float(k[4]),
                            "volume": float(k[5]),
                            "closed": True,
                        }
                return
        except Exception as e:
            logging.warning(f"Bybit REST fejl {symbol}: {e}")

        # ── Binance fallback ──
        try:
            url = (f"{BINANCE_REST}/klines?symbol={symbol.upper()}"
                   f"&interval={interval}&limit={limit}")
            with urllib.request.urlopen(url, timeout=10) as r:
                data = json.loads(r.read())
            with self._lock:
                for k in data:
                    ot = int(k[0])
                    self._candles[symbol][interval][ot] = {
                        "open_time": ot,
                        "open":   float(k[1]),
                        "high":   float(k[2]),
                        "low":    float(k[3]),
                        "close":  float(k[4]),
                        "volume": float(k[5]),
                        "closed": True,
                    }
        except Exception as e:
            logging.warning(f"REST seed fejl {symbol}: {e}")

    def _seed_all(self, symbols, interval):
        """Henter historik for alle symboler parallelt."""
        threads = []
        for sym in symbols:
            t = threading.Thread(
                target=self._fetch_history,
                args=(sym, interval),
                daemon=True,
            )
            threads.append(t)
            t.start()
            time.sleep(0.05)   # Rate limit

        for t in threads:
            t.join(timeout=15)

    # ─────────────────────────────────────────────
    #  WebSocket
    # ─────────────────────────────────────────────

    def _build_stream_url(self, symbols, interval):
        # Bybit WebSocket — ingen geo-blokering
        return "wss://stream.bybit.com/v5/public/spot"

    def _on_open(self, ws):
        """Subscribe til Bybit kline streams i batches."""
        iv_map = {"1m":"1","5m":"5","15m":"15","30m":"30",
                  "1h":"60","2h":"120","4h":"240","1d":"D"}
        bybit_iv = iv_map.get(self._interval, "60")
        for i in range(0, len(self._symbols), 10):
            batch  = self._symbols[i:i+10]
            topics = [f"kline.{bybit_iv}.{s.upper()}" for s in batch]
            ws.send(json.dumps({"op": "subscribe", "args": topics}))
            time.sleep(0.1)
        self._ready.set()

    def _on_message(self, ws, message):
        try:
            msg = json.loads(message)
            # Bybit ping
            if msg.get("op") == "ping":
                ws.send(json.dumps({"op": "pong"}))
                return
            # Bybit kline: topic = "kline.60.BTCUSDT"
            topic = msg.get("topic", "")
            if not topic.startswith("kline."):
                return
            data  = msg.get("data", [{}])
            if not data:
                return
            k     = data[0]
            parts = topic.split(".")
            sym   = parts[2] if len(parts) > 2 else ""
            iv_map = {"1":"1m","5":"5m","15":"15m","30":"30m",
                      "60":"1h","120":"2h","240":"4h","D":"1d"}
            intv  = iv_map.get(parts[1], self._interval)
            ot    = int(k.get("start", 0))
            candle = {
                "open_time": ot,
                "open":   float(k.get("open",   0)),
                "high":   float(k.get("high",   0)),
                "low":    float(k.get("low",    0)),
                "close":  float(k.get("close",  0)),
                "volume": float(k.get("volume", 0)),
                "closed": bool(k.get("confirm", False)),
            }
            with self._lock:
                self._candles[sym][intv][ot] = candle
        except Exception as e:
            logging.warning(f"WS message fejl: {e}")

    def _on_error(self, ws, error):
        self._error = str(error)
        logging.warning(f"WS fejl: {error}")

    def _on_close(self, ws, code, msg):
        if self._running:
            logging.warning("WS lukket — genforbinder om 5s")
            time.sleep(5)
            self._connect()

    def _connect(self):
        url = self._build_stream_url(self._symbols, self._interval)
        self._ws = websocket.WebSocketApp(
            url,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = self._on_error,
            on_close   = self._on_close,
        )
        self._ws_thread = threading.Thread(
            target=self._ws.run_forever,
            kwargs={"ping_interval": 20, "ping_timeout": 10},
            daemon=True,
        )
        self._ws_thread.start()

    # ─────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────

    def subscribe(self, symbols: list, interval: str = "1h"):
        """Abonnér på symbols × interval."""
        self._symbols  = [s.upper() for s in symbols]
        self._interval = interval

    def start(self, seed_history: bool = True, timeout: int = 15):
        """
        Start feed.
        seed_history=True: Hent historisk data først via REST,
                           derefter live via WebSocket.
        """
        self._running = True

        if seed_history:
            print(f"  [BinanceFeed] Henter historik for {len(self._symbols)} symboler...")
            self._seed_all(self._symbols, self._interval)
            print(f"  [BinanceFeed] Historik klar — starter WebSocket...")

        self._connect()

        if not self._ready.wait(timeout=timeout):
            raise RuntimeError("Binance WebSocket timeout — tjek internet")

        print(f"  [BinanceFeed] Live feed aktivt ✓  ({len(self._symbols)} symboler)")

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def get_ohlcv(self, symbol: str, interval: str = None) -> pd.DataFrame:
        """
        Returnerer OHLCV DataFrame for symbol.
        Kun lukkede candles (closed=True) medtages — sikker til signal-beregning.
        """
        intv = interval or self._interval
        sym  = symbol.upper()

        with self._lock:
            data = dict(self._candles.get(sym, {}).get(intv, {}))

        if not data:
            return pd.DataFrame()

        rows = sorted(data.values(), key=lambda x: x["open_time"])
        # Kun lukkede candles (undgå signaler på halvfærdig candle)
        rows = [r for r in rows if r["closed"]]
        rows = rows[-MAX_CANDLES:]

        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.set_index("datetime")
        df = df[["open","high","low","close","volume"]].rename(columns={
            "open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"
        })
        return df.astype(float)

    def last_price(self, symbol: str) -> float | None:
        """Seneste close-pris (inkl. åben candle)."""
        intv = self._interval
        sym  = symbol.upper()
        with self._lock:
            data = self._candles.get(sym, {}).get(intv, {})
        if not data:
            return None
        latest = max(data.values(), key=lambda x: x["open_time"])
        return latest["close"]

    def is_ready(self, symbol: str) -> bool:
        """True hvis symbolet har data klar."""
        with self._lock:
            return bool(self._candles.get(symbol.upper(), {}).get(self._interval))

    def status(self) -> dict:
        with self._lock:
            return {
                "symbols":   len(self._symbols),
                "connected": self._ws is not None and self._running,
                "candles":   {
                    s: len(self._candles.get(s,{}).get(self._interval,{}))
                    for s in self._symbols[:5]
                }
            }
