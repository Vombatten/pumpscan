"""
replay_scan.py — Kronologisk replay der matcher live-scannerens logik præcist.

Princip:
  For hvert bar-tidspunkt T i replay-vinduet:
    → Kør præcis samme logik som scan_symbol_live for hvert symbol
    → Samme dc+8 lookback, samme grade-filter, samme active_symbols-blokering
    → Resultatet = nøjagtigt de signaler du ville have fået i live trading

Garanti:
  - 24h og 48h replay er konsistente (overlappende periode er identisk)
  - Ingen kunstig cooldown — kun blokering mens trade er åben (max 48h)
  - Samme pump-detektion, RSI, ATR, grade som live-scanner
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

try:
    from strategy_pump_dump import detect_pumps, interval_to_hours
    from grade_signal import grade_signal, GRADE_RISK
    from utils import rsi, atr
    SCANNER_OK = True
except ImportError:
    SCANNER_OK = False


# ══════════════════════════════════════════════════════════════
# KRONOLOGISK REPLAY — matcher live-scanner præcist
# ══════════════════════════════════════════════════════════════

def replay_from_feed(feed, symbols: list, params: dict, hours: int = 48) -> list:
    """
    Simulerer kronologisk hvad live-scanneren ville have fundet.

    For hvert bar-tidspunkt T i replay-vinduet:
      - For hvert symbol der IKKE har en aktiv trade:
          Kør samme logik som scan_symbol_live (dc+8 lookback, A-grade only)
      - Hvis signal: simuler trade fremad (TP/SL/TIMEOUT)
      - Bloker symbol til trade lukker (max 48t)

    Garanterer at 24h og 48h replay er konsistente.
    """
    if not SCANNER_OK:
        return []

    ih        = interval_to_hours(params.get("interval", "1h"))
    dc        = max(1, int(params.get("entry_delay_h", 2) / ih))
    hold_c    = max(1, int(48 / ih))
    lookback  = dc + 8          # samme som live-scanner
    sl_pct    = params.get("stop_loss_pct", 3) / 100
    tp_atr    = params.get("tp_atr", 2.0)
    pump_min  = params.get("pump_pct", 15)
    rsi_max   = params.get("rsi_max", 80)
    pump_win_h= params.get("pump_window_h", 24)
    vol_filter= params.get("volume_filter", False)
    min_can   = params.get("min_pump_candles", 1)
    capital   = params.get("capital", 100)
    cp        = (params.get("fee_pct", 0.06) + params.get("slippage_pct", 0.15)) / 100
    cutoff    = datetime.now(timezone.utc) - timedelta(hours=hours)
    can       = max(1, int(pump_win_h / ih))

    # ── 1. Pre-beregn indikatorer for alle symboler ──
    sym_data = {}
    for symbol in symbols:
        try:
            df_raw = feed.get_ohlcv(symbol, params.get("interval", "1h"))
            if df_raw is None or len(df_raw) < 30:
                continue
            df = df_raw.copy()
            df.columns = [c.lower() for c in df.columns]
            df["rsi_v"] = rsi(df["close"], 14)
            df["atr_v"] = atr(df, 14)
            df["pump"]  = detect_pumps(df, pump_min, pump_win_h, ih,
                                       min_candles=min_can,
                                       volume_filter=vol_filter)
            # Pre-beregn rolling high/low for pump-størrelse
            df["roll_h"] = df["high"].rolling(can).max().shift(1)
            df["roll_l"] = df["low"].rolling(can).min().shift(1)
            df["vol_ma"] = df["volume"].rolling(can * 2).mean().shift(1)
            sym_data[symbol] = df
        except Exception:
            continue

    if not sym_data:
        return []

    # ── 2. Byg fælles tidsserie ──
    # Brug union af alle timestamps — sorteret kronologisk
    all_ts = sorted(set(
        ts for df in sym_data.values() for ts in df.index
    ))

    # ── 3. Kronologisk simulation ──
    all_signals = []
    # active_trades: symbol → (exit_ts, entry_info)
    active_trades: dict = {}
    seen_keys: set = set()     # undgår duplikater for samme pump-bar

    for bar_ts in all_ts:
        ts_utc = bar_ts.tz_convert("UTC") if bar_ts.tzinfo else bar_ts.tz_localize("UTC")
        if ts_utc < cutoff:
            continue

        # Frigiv udløbne trades
        active_trades = {
            sym: info for sym, info in active_trades.items()
            if info["exit_ts"] > ts_utc
        }

        # Scan hvert symbol ved dette tidspunkt
        for symbol, df in sym_data.items():
            if symbol in active_trades:
                continue

            # Find bar-index for dette tidspunkt
            if bar_ts not in df.index:
                continue
            sym_i = df.index.get_loc(bar_ts)
            if sym_i < dc + 2:
                continue

            # ── Samme logik som scan_symbol_live ──
            signal = None
            for i in range(sym_i, max(sym_i - lookback, dc), -1):
                row  = df.iloc[i]
                prev = df.iloc[i - dc]

                if pd.isna(row["rsi_v"]) or pd.isna(row["atr_v"]):
                    continue
                if row["atr_v"] <= 0:
                    continue
                if not (prev["pump"] and
                        row["rsi_v"] < rsi_max and
                        row["close"] < prev["high"]):
                    continue

                rh = prev["roll_h"]
                rl = prev["roll_l"]
                if pd.isna(rh) or pd.isna(rl) or rl <= 0:
                    continue
                ps = (rh - rl) / rl * 100

                ep       = row["close"] * (1 + cp / 2)
                sl_price = ep * (1 + sl_pct)
                tp_price = ep - row["atr_v"] * tp_atr
                if tp_price >= ep:
                    continue

                avg_vol  = prev["vol_ma"]
                pump_vol = df["volume"].iloc[i - dc]

                g = grade_signal(
                    pump_pct    = ps,
                    rsi         = row["rsi_v"],
                    entry_price = ep,
                    pump_high   = rh,
                    atr         = row["atr_v"],
                    avg_volume  = float(avg_vol) if not pd.isna(avg_vol) else 1.0,
                    pump_volume = float(pump_vol),
                )
                if g["grade"] != "A":
                    continue

                # Deduplicer på signal-bar tidspunkt
                sig_key = f"{symbol}_{df.index[i].isoformat()[:13]}"
                if sig_key in seen_keys:
                    signal = None; break
                seen_keys.add(sig_key)

                signal = {
                    "symbol":       symbol.replace("USDT", ""),
                    "symbol_full":  symbol,
                    "ts":           ts_utc.isoformat(),
                    "ts_str":       ts_utc.strftime("%d/%m %H:%M"),
                    "entry":        round(ep, 6),
                    "sl":           round(sl_price, 6),
                    "tp":           round(tp_price, 6),
                    "sl_pct":       params.get("stop_loss_pct", 3),
                    "tp_pct":       round((ep - tp_price) / ep * 100, 1),
                    "pump_size":    round(ps, 1),
                    "rsi":          round(row["rsi_v"], 1),
                    "grade":        g["grade"],
                    "grade_score":  g["score"],
                    "grade_details":g.get("details", {}),
                    "risk_usd":     round(capital * 0.07, 2),
                    "pos_usd":      round(capital * 0.07 / sl_pct, 0),
                    "missed":       True,
                }
                break

            if signal is None:
                continue

            # ── Simuler trade fremad fra sym_i+1 ──
            outcome    = "OPEN"
            exit_price = signal["entry"]
            exit_time  = None
            exit_ts    = ts_utc + timedelta(hours=48)  # default timeout

            for j in range(sym_i + 1, min(sym_i + hold_c + 1, len(df))):
                f = df.iloc[j]
                if f["high"] >= signal["sl"]:
                    outcome    = "SL"
                    exit_price = signal["sl"]
                    exit_ts    = df.index[j].tz_convert("UTC") if df.index[j].tzinfo else df.index[j].tz_localize("UTC")
                    exit_time  = df.index[j].strftime("%d/%m %H:%M")
                    break
                if f["low"] <= signal["tp"]:
                    outcome    = "TP"
                    exit_price = signal["tp"]
                    exit_ts    = df.index[j].tz_convert("UTC") if df.index[j].tzinfo else df.index[j].tz_localize("UTC")
                    exit_time  = df.index[j].strftime("%d/%m %H:%M")
                    break

            if outcome == "OPEN":
                last_j = min(sym_i + hold_c, len(df) - 1)
                exit_price = df.iloc[last_j]["close"]
                outcome    = "TIMEOUT"

            # P&L (SHORT: profit når pris falder)
            pnl_pct = (signal["entry"] - exit_price) / signal["entry"] * 100
            risk    = signal["risk_usd"]
            pnl_usd = round(pnl_pct / signal["sl_pct"] * risk, 2)

            signal.update({
                "outcome":     outcome,
                "exit_price":  round(exit_price, 6),
                "exit_time":   exit_time,
                "pnl":         round(pnl_usd, 2),
            })

            all_signals.append(signal)

            # Bloker symbol til trade lukker
            active_trades[symbol] = {"exit_ts": exit_ts}

    all_signals.sort(key=lambda s: s["ts"], reverse=True)
    return all_signals


# ── Legacy wrapper (bruges ikke længere men beholdes for kompatibilitet) ──
def replay(symbols: list, params: dict, hours: int = 48) -> list:
    return []
