"""
replay_scan.py — Historisk signal replay

Scanner de seneste N timer og finder signaler der BURDE have været.
Viser hvad der faktisk skete (TP/SL/timeout) baseret på efterfølgende priser.

Bruges via PWA API: GET /api/replay?hours=48
"""

import sys, os
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from utils import fetch_ohlcv, rsi, atr
    from grade_signal import grade_signal, GRADE_RISK
    SCANNER_OK = True
except ImportError:
    SCANNER_OK = False


def interval_to_hours(interval: str) -> float:
    return {"1m":1/60,"5m":5/60,"15m":0.25,"30m":0.5,
            "1h":1,"2h":2,"4h":4,"1d":24}.get(interval, 1.0)


def replay(symbols: list, params: dict, hours: int = 48) -> list:
    """
    Finder alle signaler der ville have opstået i de seneste `hours` timer.
    Simulerer exit (TP/SL/timeout) på faktiske efterfølgende priser.
    Returnerer liste af signal-dicts med outcome.
    """
    if not SCANNER_OK:
        return []

    interval   = params.get("interval", "1h")
    ih         = interval_to_hours(interval)
    delay_c    = max(1, int(params.get("entry_delay_h", 2) / ih))
    hold_c     = max(1, int(params.get("max_hold_h", 48) / ih))
    sl_pct     = params.get("stop_loss_pct", 5.5) / 100
    tp_atr     = params.get("tp_atr", 1.5)
    pump_min   = params.get("pump_pct", 20)
    rsi_max    = params.get("rsi_max", 80)
    pump_win_h = params.get("pump_window_h", 24)
    costs      = (params.get("fee_pct", 0.06) +
                  params.get("slippage_pct", 0.15)) / 100
    capital    = params.get("capital", 100)

    # Cutoff: kun signaler fra de seneste `hours` timer
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    all_signals = []

    for symbol in symbols:
        try:
            # Hent lidt mere data end nødvendigt for at have context
            days   = max(7, int(hours / 24) + 3)
            df_raw = fetch_ohlcv(symbol, interval, days=days)
            if df_raw.empty or len(df_raw) < 30:
                continue

            df = df_raw.copy()
            df["rsi_v"] = rsi(df["close"], 14)
            df["atr_v"] = atr(df, 14)

            can = max(1, int(pump_win_h / ih))

            # Pump detection
            roll_h = df["high"].rolling(can).max().shift(1)
            roll_l = df["low"].rolling(can).min().shift(1)
            pump_pct_series = ((roll_h - roll_l) / roll_l * 100).fillna(0)
            avg_vol = df["volume"].rolling(can*2).mean().shift(1)
            vol_ok  = df["volume"] > avg_vol * 1.5

            arr = df.values
            idx = df.index

            for i in range(max(delay_c, can+1), len(df)):
                ts = idx[i]

                # Kun signaler inden for replay-vinduet
                ts_utc = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")
                if ts_utc < cutoff:
                    continue

                pi = i - delay_c
                if pi < 0:
                    continue

                row  = df.iloc[i]
                prev = df.iloc[pi]

                if pd.isna(row["rsi_v"]) or pd.isna(row["atr_v"]):
                    continue
                if row["atr_v"] <= 0:
                    continue

                # Entry signal
                if not (pump_pct_series.iloc[pi] >= pump_min and
                        vol_ok.iloc[pi] and
                        row["rsi_v"] < rsi_max and
                        row["close"] < prev["high"]):
                    continue

                # Grade
                ep        = row["close"] * (1 + costs / 2)
                roll_hi_v = roll_h.iloc[pi]
                roll_lo_v = roll_l.iloc[pi]
                avg_vol_v = avg_vol.iloc[pi]
                pump_v    = df["volume"].iloc[pi]
                pmp_pct_v = pump_pct_series.iloc[pi]

                grade_info = grade_signal(
                    pump_pct    = pmp_pct_v,
                    rsi         = row["rsi_v"],
                    entry_price = ep,
                    pump_high   = roll_hi_v,
                    atr         = row["atr_v"],
                    avg_volume  = avg_vol_v if not pd.isna(avg_vol_v) else 1,
                    pump_volume = pump_v,
                )

                # Kun A-grade
                if grade_info["grade"] != "A":
                    continue

                sl_price = ep * (1 + sl_pct)
                tp_price = ep - row["atr_v"] * tp_atr
                risk_amt = capital * 0.07
                size     = risk_amt / (ep * sl_pct)

                # ── Simulér exit på efterfølgende bars ──
                outcome     = "OPEN"
                exit_price  = None
                exit_time   = None
                hold_candles= 0

                for j in range(i+1, min(i+1+hold_c, len(df))):
                    fut = df.iloc[j]
                    hold_candles += 1

                    if fut["low"] <= tp_price:
                        outcome    = "TP"
                        exit_price = tp_price
                        exit_time  = idx[j]
                        break
                    elif fut["high"] >= sl_price:
                        outcome    = "SL"
                        exit_price = sl_price
                        exit_time  = idx[j]
                        break

                if outcome == "OPEN" and hold_candles >= hold_c:
                    outcome    = "TIMEOUT"
                    exit_price = df.iloc[min(i+hold_c, len(df)-1)]["close"]
                    exit_time  = idx[min(i+hold_c, len(df)-1)]

                # Beregn P&L
                if exit_price:
                    pnl = (ep - exit_price) * size - ep * size * costs
                else:
                    pnl = 0

                dur_h = hold_candles * ih
                dur_str = (f"{int(dur_h)}H {int((dur_h%1)*60)}M"
                           if dur_h < 48 else f"{dur_h/24:.1f}d")

                all_signals.append({
                    "symbol":      symbol.replace("USDT",""),
                    "symbol_full": symbol,
                    "ts":          ts_utc.isoformat(),
                    "ts_str":      ts_utc.strftime("%d/%m %H:%M"),
                    "entry":       round(ep, 6),
                    "sl":          round(sl_price, 6),
                    "tp":          round(tp_price, 6),
                    "sl_pct":      round(sl_pct*100, 1),
                    "tp_pct":      round((ep-tp_price)/ep*100, 1),
                    "pump_size":   round(pmp_pct_v, 1),
                    "rsi":         round(row["rsi_v"], 1),
                    "grade":       grade_info["grade"],
                    "grade_score": grade_info["score"],
                    "outcome":     outcome,
                    "exit_price":  round(exit_price, 6) if exit_price else None,
                    "exit_time":   exit_time.strftime("%d/%m %H:%M") if exit_time else None,
                    "pnl":         round(pnl, 2),
                    "duration":    dur_str,
                    "risk_usd":    round(risk_amt, 2),
                    "pos_usd":     round(risk_amt / sl_pct, 0),
                    "missed":      True,   # Markér som "missed" signal
                })

        except Exception:
            continue

    # Sortér nyeste først
    all_signals.sort(key=lambda s: s["ts"], reverse=True)
    return all_signals
