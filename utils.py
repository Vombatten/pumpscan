"""
utils.py — Fælles hjælpefunktioner til krypto backtesting
Henter OHLCV-data fra Binance (offentlig API, ingen nøgle nødvendig)
og beregner tekniske indikatorer.
"""

import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ─────────────────────────────────────────────
#  Binance data
# ─────────────────────────────────────────────

BINANCE_BASE = "https://api.binance.com/api/v3"
BYBIT_BASE   = "https://api.bybit.com/v5/market"

def fetch_ohlcv(symbol: str, interval: str, days: int = 90) -> pd.DataFrame:
    """
    Henter OHLCV candledata fra Binance.

    Args:
        symbol:   Trading par, fx 'BTCUSDT', 'PEPEUSDT'
        interval: '1m','3m','5m','15m','1h','4h','1d'
        days:     Antal dage tilbage i tid

    Returns:
        DataFrame med kolonner: open, high, low, close, volume, (datetime index)
    """
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000

    url    = f"{BINANCE_BASE}/klines"
    limit  = 1000
    frames = []

    current = start_ms
    while current < end_ms:
        params = {
            "symbol":    symbol.upper(),
            "interval":  interval,
            "startTime": current,
            "endTime":   end_ms,
            "limit":     limit,
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_vol","trades","taker_base","taker_quote","ignore"
        ])
        frames.append(df)
        current = int(data[-1][6]) + 1   # close_time + 1ms
        if len(data) < limit:
            break

    if not frames:
        raise ValueError(f"Ingen data fundet for {symbol} ({interval})")

    df = pd.concat(frames, ignore_index=True)
    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("datetime")
    df = df[["open","high","low","close","volume"]].astype(float)
    df = df[~df.index.duplicated()].sort_index()
    return df


def fetch_top_altcoins(quote="USDT", min_volume_usd=500_000) -> list:
    """
    Returnerer top altcoins sorteret efter 24h volumen.
    Prøver Bybit først (ingen geo-blokering), fallback til Binance.
    """
    # ── Bybit (ingen geo-blokering) ──
    try:
        url  = f"{BYBIT_BASE}/tickers?category=spot"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        tickers = data.get("result", {}).get("list", [])
        result = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith(quote):
                continue
            base = sym[:-len(quote)]
            if base in ("BTC","ETH","BNB","USDC","DAI","TUSD","FDUSD"):
                continue
            try:
                vol = float(t.get("turnover24h", 0) or 0)
                price = float(t.get("lastPrice", 0) or 0)
            except (ValueError, TypeError):
                continue
            if vol < min_volume_usd or price <= 0:
                continue
            result.append({"symbol": sym, "volume": vol,
                           "price": price, "source": "bybit"})
        if result:
            result.sort(key=lambda x: x["volume"], reverse=True)
            return result
    except Exception:
        pass

    # ── Binance fallback ──
    try:
        url  = f"{BINANCE_BASE}/ticker/24hr"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        tickers = resp.json()
        result = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith(quote):
                continue
            base = sym[:-len(quote)]
            if base in ("BTC","ETH","BNB","USDC","DAI","TUSD","FDUSD"):
                continue
            try:
                vol   = float(t.get("quoteVolume", 0) or 0)
                price = float(t.get("lastPrice", 0) or 0)
            except (ValueError, TypeError):
                continue
            if vol < min_volume_usd or price <= 0:
                continue
            result.append({"symbol": sym, "volume": vol,
                           "price": price, "source": "binance"})
        result.sort(key=lambda x: x["volume"], reverse=True)
        return result
    except Exception:
        return []


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def bollinger(series: pd.Series, period: int = 20, std: float = 2.0):
    mid   = series.rolling(period).mean()
    sigma = series.rolling(period).std()
    return mid + std * sigma, mid, mid - std * sigma


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl  = df["high"] - df["low"]
    hc  = (df["high"] - df["close"].shift()).abs()
    lc  = (df["low"]  - df["close"].shift()).abs()
    tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, adjust=False).mean()


def vwap(df: pd.DataFrame) -> pd.Series:
    """Intradag VWAP — nulstilles pr. dag."""
    typical = (df["high"] + df["low"] + df["close"]) / 3
    df2     = df.copy()
    df2["_tp"]  = typical
    df2["_tpv"] = typical * df["volume"]
    df2["_date"] = df2.index.date
    cum_tpv = df2.groupby("_date")["_tpv"].cumsum()
    cum_vol = df2.groupby("_date")["volume"].cumsum()
    return cum_tpv / cum_vol


def rolling_max_return(close: pd.Series, window: int) -> pd.Series:
    """Maks. afkast i % inden for et rullende vindue (bruges til pump-detektion)."""
    roll_max  = close.rolling(window).max()
    roll_open = close.shift(window)
    return (roll_max / roll_open - 1) * 100


# ─────────────────────────────────────────────
#  Performance-metrics
# ─────────────────────────────────────────────

def compute_metrics(trades: list[dict], initial_capital: float = 10_000) -> dict:
    """
    Beregner standard backtest-metrics fra en liste af handler.

    Hver handel skal have nøglerne:
        pnl (float), entry_time (datetime), exit_time (datetime)
    """
    if not trades:
        return {}

    pnls   = [t["pnl"] for t in trades]
    equity = initial_capital + pd.Series(pnls).cumsum()
    equity = pd.concat([pd.Series([initial_capital]), equity]).reset_index(drop=True)

    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    gross_win  = sum(wins)  if wins   else 0
    gross_loss = sum(losses) if losses else 0

    # Max drawdown
    roll_max = equity.cummax()
    dd       = (equity - roll_max) / roll_max * 100
    max_dd   = dd.min()

    # Sharpe (daglig returns)
    daily = pd.Series(pnls) / initial_capital
    sharpe = (daily.mean() / daily.std() * np.sqrt(365)) if daily.std() > 0 else 0

    # Gennemsnitlig holdtid
    durations = []
    for t in trades:
        if "entry_time" in t and "exit_time" in t and t["entry_time"] and t["exit_time"]:
            durations.append((t["exit_time"] - t["entry_time"]).total_seconds() / 3600)

    return {
        "total_trades":    len(trades),
        "win_rate":        len(wins) / len(trades) * 100,
        "profit_factor":   gross_win / abs(gross_loss) if gross_loss != 0 else float("inf"),
        "net_pnl":         sum(pnls),
        "return_pct":      sum(pnls) / initial_capital * 100,
        "max_drawdown_pct": max_dd,
        "sharpe_annual":   sharpe,
        "avg_win":         np.mean(wins)   if wins   else 0,
        "avg_loss":        np.mean(losses) if losses else 0,
        "avg_hold_h":      np.mean(durations) if durations else 0,
        "final_equity":    equity.iloc[-1],
        "equity_curve":    equity.tolist(),
    }


# ─────────────────────────────────────────────
#  Rapport-print
# ─────────────────────────────────────────────

def print_report(title: str, metrics: dict, trades: list[dict], show_trades: int = 10):
    w = 56
    sep = "─" * w

    print(f"\n{'═'*w}")
    print(f"  {title}")
    print(f"{'═'*w}")
    print(f"  Handler i alt   : {metrics.get('total_trades', 0)}")
    print(f"  Win rate        : {metrics.get('win_rate', 0):.1f}%")
    print(f"  Profit faktor   : {metrics.get('profit_factor', 0):.2f}")
    print(f"  Netto P&L       : ${metrics.get('net_pnl', 0):,.2f}")
    print(f"  Afkast          : {metrics.get('return_pct', 0):.2f}%")
    print(f"  Max drawdown    : {metrics.get('max_drawdown_pct', 0):.2f}%")
    print(f"  Sharpe (år)     : {metrics.get('sharpe_annual', 0):.2f}")
    print(f"  Gns. gevinst    : ${metrics.get('avg_win', 0):.2f}")
    print(f"  Gns. tab        : ${metrics.get('avg_loss', 0):.2f}")
    print(f"  Gns. holdtid    : {metrics.get('avg_hold_h', 0):.1f}t")
    print(f"  Slutkapital     : ${metrics.get('final_equity', 0):,.2f}")
    print(sep)

    if trades and show_trades > 0:
        print(f"\n  {'Seneste handler':}")
        print(f"  {'Symbol':<12} {'Retning':<8} {'Entry':<12} {'PnL':>8}  {'Status'}")
        print(f"  {sep}")
        for t in trades[-show_trades:]:
            status = "✓ WIN" if t["pnl"] > 0 else "✗ TAB"
            entry  = t.get("entry_time","")
            if isinstance(entry, datetime):
                entry = entry.strftime("%m-%d %H:%M")
            print(f"  {t.get('symbol',''):<12} {t.get('direction',''):<8} {str(entry):<12} {t['pnl']:>8.2f}  {status}")

    print()
