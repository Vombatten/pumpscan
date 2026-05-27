"""
strategy_pump_dump.py — Pump & Dump Short  (v2 + ATR TP, 100% exit)

ÆNDRINGER FRA v2:
  1. TP = 1× ATR under entry  (ramtes 85% af gangene i test)
  2. 100% fuld exit ved TP — ingen partial, ingen rest

ALT ANDET er v2 uændret:
  - pump_pct=20, pump_window=24t, delay=2t
  - SL=8% fast — ingen trailing stop
  - max_hold=48t, rsi_max=80, 1h interval

Kør:
  python strategy_pump_dump.py
  python strategy_pump_dump.py --tp_atr 1.5   # prøv større TP
  python strategy_pump_dump.py --symbols PEPEUSDT WIFUSDT BONKUSDT
"""

import time
import argparse
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from utils import (
    fetch_ohlcv, fetch_top_altcoins,
    rsi, atr,
    compute_metrics, print_report,
)


# ─────────────────────────────────────────────
#  Parametre — v2 + TP-ændring
# ─────────────────────────────────────────────
DEFAULTS = {
    "pump_pct":         20,
    "pump_window_h":    24,
    "entry_delay_h":    2,
    "stop_loss_pct":    8,     # Fast SL — ingen trailing
    "tp_atr":           1.0,   # TP = 1× ATR under entry  ← eneste ændring
    "max_hold_h":       48,
    "risk_per_trade":   0.02,
    "initial_capital":  10_000,
    "backtest_days":    180,
    "interval":         "1h",
    "fee_pct":          0.06,
    "slippage_pct":     0.15,
    "cooldown_h":       72,
    "rsi_max":          80,
    "top_n":            40,
    "volume_filter":    True,
    "min_pump_candles": 2,
}


# ─────────────────────────────────────────────
#  Hjælpefunktioner
# ─────────────────────────────────────────────

def interval_to_hours(interval: str) -> float:
    return {
        "1m":1/60,"3m":3/60,"5m":5/60,"15m":0.25,
        "30m":0.5,"1h":1,"2h":2,"4h":4,"6h":6,"8h":8,"12h":12,"1d":24,
    }.get(interval, 1)


def detect_pumps(df: pd.DataFrame, pump_pct: float, window_h: int,
                 interval_h: float, min_candles: int = 2,
                 volume_filter: bool = True) -> pd.Series:
    candles   = max(1, int(window_h / interval_h))
    roll_low  = df["low"].rolling(candles).min()
    roll_high = df["high"].rolling(candles).max()
    pct       = (roll_high - roll_low) / roll_low * 100
    pump_ok   = pct >= pump_pct

    if volume_filter:
        vol_avg = df["volume"].rolling(candles).mean()
        pump_ok = pump_ok & (df["volume"] >= vol_avg * 1.5)

    if min_candles > 1:
        pump_ok = pump_ok.rolling(min_candles).min().fillna(0).astype(bool)

    return pump_ok


# ─────────────────────────────────────────────
#  Backtest kerne
# ─────────────────────────────────────────────

def backtest_pump_dump(symbol: str, params: dict) -> tuple[list, pd.DataFrame]:
    ih         = interval_to_hours(params["interval"])
    delay_c    = max(1, int(params["entry_delay_h"] / ih))
    hold_c     = max(1, int(params["max_hold_h"] / ih))
    cooldown_c = max(1, int(params["cooldown_h"] / ih))
    costs_pct  = (params["fee_pct"] + params["slippage_pct"]) / 100

    df = fetch_ohlcv(symbol, params["interval"], days=params["backtest_days"])
    if len(df) < 50:
        return [], df

    df["rsi_v"]       = rsi(df["close"], 14)
    df["atr_v"]       = atr(df, 14)
    df["pump_signal"] = detect_pumps(
        df, params["pump_pct"], params["pump_window_h"], ih,
        min_candles=params["min_pump_candles"],
        volume_filter=params["volume_filter"],
    )

    trades       = []
    capital      = params["initial_capital"]
    in_trade     = False
    last_trade_i = -9999

    entry_price = sl_price = tp_price = None
    size        = 0.0
    entry_meta  = {}
    entry_idx   = 0

    for i in range(max(delay_c, 20), len(df)):
        row = df.iloc[i]
        ts  = df.index[i]

        if pd.isna(row["rsi_v"]) or pd.isna(row["atr_v"]):
            continue

        # ── Åbn short ──
        if not in_trade:
            if (i - last_trade_i) < cooldown_c:
                continue

            prev = df.iloc[i - delay_c]

            if (prev["pump_signal"]
                    and row["rsi_v"] < params["rsi_max"]
                    and row["close"] < prev["high"]):

                sl_pct      = params.get("stop_loss_pct", params.get("stop_loss_atr", 8)) / 100

                # ── Kelly position sizing ──
                k_frac   = params.get("kelly_fraction", params["risk_per_trade"])
                risk_amt = capital * k_frac
                entry_price = row["close"] * (1 + costs_pct / 2)
                sl_price    = entry_price * (1 + sl_pct)
                tp_price    = entry_price - row["atr_v"] * params["tp_atr"]
                size        = risk_amt / (entry_price * sl_pct)

                in_trade   = True
                entry_idx  = i
                entry_meta = {
                    "symbol":      symbol,
                    "direction":   "short",
                    "entry_price": entry_price,
                    "entry_time":  ts,
                    "size":        size,
                    "tp_pct":      (entry_price - tp_price) / entry_price * 100,
                }

        # ── Håndter åben position ──
        else:
            exit_price = None
            reason     = None

            if row["low"] <= tp_price:
                exit_price, reason = tp_price, "TP"
            elif row["high"] >= sl_price:
                exit_price, reason = sl_price, "SL"
            elif (i - entry_idx) >= hold_c:
                exit_price, reason = row["close"], "TIMEOUT"

            if exit_price is not None:
                pnl_raw  = (entry_price - exit_price) * size
                cost     = entry_price * size * costs_pct
                pnl      = pnl_raw - cost
                capital += pnl
                last_trade_i = i

                trades.append({
                    **entry_meta,
                    "exit_price": exit_price,
                    "exit_time":  ts,
                    "reason":     reason,
                    "pnl":        pnl,
                    "capital":    capital,
                })
                in_trade = False

    return trades, df


# ─────────────────────────────────────────────
#  Analyse
# ─────────────────────────────────────────────

def reason_breakdown(trades: list) -> str:
    if not trades:
        return ""
    df = pd.DataFrame(trades)
    g  = df.groupby("reason")["pnl"].agg(["count", "sum", "mean"])
    lines = [f"\n  {'Exit-årsag':<12} {'Antal':>6}  {'Total P&L':>10}  {'Gns P&L':>10}"]
    lines.append("  " + "─" * 46)
    for r, row in g.iterrows():
        flag = " ✓" if row["sum"] > 0 else " ✗"
        lines.append(f"  {r:<12} {int(row['count']):>6}  "
                     f"${row['sum']:>9.2f}  ${row['mean']:>9.2f}{flag}")
    return "\n".join(lines)


def symbol_breakdown(trades: list, top_n: int = 10) -> str:
    if not trades:
        return ""
    df = pd.DataFrame(trades)
    g  = df.groupby("symbol").agg(
        handler=("pnl", "count"),
        pnl=("pnl", "sum"),
        wr=("pnl", lambda x: (x > 0).mean() * 100),
    ).sort_values("pnl", ascending=False)
    lines = [f"\n  {'Symbol':<14} {'Handler':>7}  {'P&L':>10}  {'WR':>7}"]
    lines.append("  " + "─" * 44)
    for sym, row in g.head(top_n).iterrows():
        lines.append(f"  {sym:<14} {int(row['handler']):>7}  "
                     f"${row['pnl']:>9.2f}  {row['wr']:>6.1f}%")
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  Plot
# ─────────────────────────────────────────────

def plot_results(all_trades: list, equity_curve: list, params: dict):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        f"Pump & Dump v2+  |  pump≥{params['pump_pct']}%  {params['interval']}"
        f"  TP={params['tp_atr']}×ATR (100% exit)  SL={params['stop_loss_pct']}%  "
        f"hold≤{params['max_hold_h']}t",
        fontsize=11, fontweight="bold", color="#ddd",
    )
    fig.patch.set_facecolor("#0f0f14")
    for ax in axes.flat:
        ax.set_facecolor("#16161f")
        ax.tick_params(colors="#aaa", labelsize=9)
        ax.spines[:].set_color("#2a2a3a")

    reason_colors = {"TP": "#00e5a0", "TIMEOUT": "#5bc8f5", "SL": "#ff4f4f"}
    df_t = pd.DataFrame(all_trades)

    # Equity
    eq = pd.Series(equity_curve)
    axes[0,0].plot(eq.index, eq.values, color="#00e5a0", linewidth=1.5)
    axes[0,0].fill_between(eq.index, eq.values, equity_curve[0],
                           alpha=0.12, color="#00e5a0")
    axes[0,0].axhline(equity_curve[0], color="#444", linewidth=0.8, linestyle="--")
    axes[0,0].set_ylabel("Kapital ($)", color="#aaa", fontsize=9)
    axes[0,0].set_title("Equity kurve", color="#ccc", fontsize=10)

    # P&L per trade
    pnls  = [t["pnl"] for t in all_trades]
    bar_c = [reason_colors.get(t["reason"], "#aaa") for t in all_trades]
    axes[0,1].bar(range(len(pnls)), pnls, color=bar_c, width=0.8, alpha=0.85)
    axes[0,1].axhline(0, color="#555", linewidth=0.8)
    axes[0,1].set_title("P&L per trade  (grøn=TP  blå=TIMEOUT  rød=SL)",
                        color="#ccc", fontsize=10)
    axes[0,1].set_ylabel("P&L ($)", color="#aaa", fontsize=9)

    # Exit fordeling
    exit_g = df_t.groupby("reason")["pnl"].agg(["count", "sum"])
    ec     = [reason_colors.get(r, "#aaa") for r in exit_g.index]
    axes[1,0].bar(exit_g.index, exit_g["sum"], color=ec, width=0.5, alpha=0.85)
    for i, (idx, row) in enumerate(exit_g.iterrows()):
        offset = 30 if row["sum"] >= 0 else -80
        axes[1,0].text(i, row["sum"] + offset, f"n={int(row['count'])}",
                       ha="center", fontsize=9, color="#aaa")
    axes[1,0].axhline(0, color="#555", linewidth=0.8)
    axes[1,0].set_title("Total P&L pr. exit-årsag", color="#ccc", fontsize=10)
    axes[1,0].set_ylabel("Total P&L ($)", color="#aaa", fontsize=9)

    # Symbol breakdown
    sym_pnl = df_t.groupby("symbol")["pnl"].sum().sort_values()
    top     = pd.concat([sym_pnl.head(6), sym_pnl.tail(6)]).drop_duplicates()
    sc      = ["#00e5a0" if v > 0 else "#ff4f4f" for v in top.values]
    axes[1,1].barh(top.index, top.values, color=sc, height=0.6)
    axes[1,1].axvline(0, color="#555", linewidth=0.8)
    axes[1,1].set_xlabel("Total P&L ($)", color="#aaa", fontsize=9)
    axes[1,1].set_title("Bedste og dårligste symbols", color="#ccc", fontsize=10)

    plt.tight_layout(rect=[0,0,1,0.94])
    fname = "pump_dump_results.png"
    plt.savefig(fname, dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"\n  Graf gemt som: {fname}")
    plt.show()



# ─────────────────────────────────────────────
#  Smart filter — baseret på faktor-analysen
# ─────────────────────────────────────────────
#
# Signifikante fund fra analyze_factors.py (p < 0.05):
#   MC $100M–$1B    → 93% WR  (bedste kategori)
#   30d change < 0% → færre SL-exits, højere WR
#   Vol/MC > 0.05   → bedre total P&L
#   Circ. supply %  → lav = bedre (mere locked = dumper hårdere)

SMART_FILTER_DEFAULTS = {
    "mc_min":         10e6,    # Min $10M  (ikke micro-caps med ingen likviditet)
    "mc_max":         5e9,     # Max $5B   (inkl. large caps — data viste stadig 77% WR)
    "change_30d_max": 30,      # Max +30% 30d — fjerner kun ekstreme uptrender
    "vol_mc_min":     0.01,    # Min Vol/MC 0.01 (lavere end før — nok likviditet)
    "circ_pct_max":   98,      # Max 98% cirkulerende — fjerner kun næsten fuldt udstedte
}

COINGECKO_BASE = "https://api.coingecko.com/api/v3"


def fetch_coin_metadata(symbols: list) -> pd.DataFrame:
    """Henter coin-metadata fra CoinGecko for filtrering."""
    print("  Henter metadata fra CoinGecko...")
    try:
        resp = requests.get(f"{COINGECKO_BASE}/coins/list", timeout=15)
        resp.raise_for_status()
        all_coins = resp.json()
    except Exception as e:
        print(f"  CoinGecko fejl: {e} — springer filter over")
        return pd.DataFrame()

    bases = {s.replace("USDT","").lower() for s in symbols}
    cg_map = {}
    for coin in all_coins:
        sym = coin["symbol"].lower()
        if sym in bases and sym not in cg_map:
            cg_map[sym] = coin["id"]

    if not cg_map:
        return pd.DataFrame()

    ids_needed = list(cg_map.values())
    rows = []
    for i in range(0, len(ids_needed), 50):
        batch = ids_needed[i:i+50]
        try:
            resp = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency":            "usd",
                    "ids":                    ",".join(batch),
                    "per_page":               250,
                    "sparkline":              "false",
                    "price_change_percentage":"30d",
                },
                timeout=15,
            )
            if resp.status_code == 429:
                print("  Rate limit — venter 65s...")
                time.sleep(65)
                resp = requests.get(resp.url, timeout=15)
            resp.raise_for_status()
            rows.extend(resp.json())
            time.sleep(1.5)
        except Exception as e:
            print(f"  Metadata batch-fejl: {e}")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["symbol_upper"] = df["symbol"].str.upper() + "USDT"
    df["vol_mc_ratio"] = (df["total_volume"] /
                          df["market_cap"].replace(0, np.nan))
    df["circ_pct"]     = (df["circulating_supply"] /
                          df["total_supply"].replace(0, np.nan) * 100)
    df["change_30d"]   = df.get(
        "price_change_percentage_30d_in_currency",
        pd.Series(np.nan, index=df.index),
    )
    keep = ["symbol_upper", "market_cap", "vol_mc_ratio",
            "circ_pct", "change_30d"]
    return df[[c for c in keep if c in df.columns]].copy()


def apply_smart_filter(symbols: list, f: dict,
                       verbose: bool = True) -> tuple:
    """
    Filtrerer symboler ud fra faktor-analyse kriterierne.
    Returnerer (godkendte_symboler, metadata_df).
    """
    meta = fetch_coin_metadata(symbols)
    if meta.empty:
        print("  Springer smart filter over — bruger alle symboler")
        return symbols, meta

    kept = []
    removed = []

    for sym in symbols:
        row = meta[meta["symbol_upper"] == sym]
        if row.empty:
            kept.append(sym)
            continue

        r          = row.iloc[0]
        mc         = r.get("market_cap",   np.nan)
        vol_mc     = r.get("vol_mc_ratio", np.nan)
        circ_pct   = r.get("circ_pct",     np.nan)
        change_30d = r.get("change_30d",   np.nan)

        reasons = []
        if not pd.isna(mc):
            if mc < f["mc_min"]:
                reasons.append(f"MC for lav (${mc/1e6:.0f}M < ${f['mc_min']/1e6:.0f}M)")
            if mc > f["mc_max"]:
                reasons.append(f"MC for høj (${mc/1e9:.1f}B > ${f['mc_max']/1e9:.1f}B)")
        if not pd.isna(change_30d) and change_30d > f["change_30d_max"]:
            reasons.append(f"30d optrend (+{change_30d:.1f}%)")
        if not pd.isna(vol_mc) and vol_mc < f["vol_mc_min"]:
            reasons.append(f"Vol/MC lav ({vol_mc:.3f} < {f['vol_mc_min']})")
        if not pd.isna(circ_pct) and circ_pct > f["circ_pct_max"]:
            reasons.append(f"Supply høj ({circ_pct:.0f}% > {f['circ_pct_max']}%)")

        if reasons:
            removed.append((sym, ", ".join(reasons)))
        else:
            kept.append(sym)

    if verbose:
        print(f"\n  Smart filter resultat: "
              f"{len(kept)} godkendt / {len(removed)} fjernet")
        if removed:
            print(f"\n  {'Fjernet':<16} Årsag")
            print("  " + "─" * 58)
            for sym, rsn in removed:
                print(f"  {sym:<16} {rsn}")
        print()

    return kept, meta


# ─────────────────────────────────────────────
#  Kelly Criterion position sizing
# ─────────────────────────────────────────────

def kelly_fraction(win_rate: float, avg_win: float, avg_loss: float,
                   fraction: float = 0.5) -> float:
    """
    Beregner optimal Kelly-brøk for position sizing.

    Formel: f* = (b×p - q) / b
      b = avg_win / avg_loss  (odds)
      p = win rate
      q = 1 - win rate

    fraction=0.5 → Half Kelly (reducerer varians markant, anbefalet).
    Returnerer andel af kapital der risikeres (klampet 0.5%–25%).
    """
    if avg_loss <= 0 or win_rate <= 0 or win_rate >= 1:
        return 0.02
    b = abs(avg_win) / abs(avg_loss)
    p = win_rate
    q = 1 - p
    f = (b * p - q) / b
    f = max(0.005, min(f * fraction, 0.25))
    return round(f, 4)


def rolling_kelly(trades: list, window: int = 20,
                  fraction: float = 0.5) -> list:
    """
    Beregner Kelly-brøken rullende over de seneste N handler.
    Bruges til adaptiv position sizing i backtesten.
    """
    fractions = []
    for i in range(len(trades)):
        subset = trades[max(0, i - window):i]
        if len(subset) < 5:
            fractions.append(0.02)   # Default de første handler
            continue
        pnls    = [t["pnl"] for t in subset]
        wins    = [p for p in pnls if p > 0]
        losses  = [p for p in pnls if p < 0]
        wr      = len(wins) / len(pnls)
        avg_w   = np.mean(wins)   if wins   else 0.01
        avg_l   = abs(np.mean(losses)) if losses else 0.01
        fractions.append(kelly_fraction(wr, avg_w, avg_l, fraction))
    return fractions


def save_backtest_stats(trades: list, filepath: str = "backtest_stats.json"):
    """Gemmer win rate og gns. win/loss til brug i live_scanner.py."""
    import json
    if not trades:
        return
    pnls   = [t["pnl"] for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    stats  = {
        "win_rate": len(wins) / len(pnls),
        "avg_win":  float(np.mean(wins))          if wins   else 0,
        "avg_loss": float(abs(np.mean(losses)))   if losses else 0,
        "n_trades": len(trades),
        "total_pnl":float(sum(pnls)),
    }
    with open(filepath, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"  Backtest-stats gemt til {filepath} (bruges af live_scanner.py)")


# ─────────────────────────────────────────────
#  Hoved
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pump & Dump v2 — TP=1×ATR, 100% exit, ingen trailing SL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols",     nargs="+", default=None)
    parser.add_argument("--top",         type=int,   default=DEFAULTS["top_n"])
    parser.add_argument("--pump_pct",    type=float, default=DEFAULTS["pump_pct"])
    parser.add_argument("--pump_window", type=int,   default=DEFAULTS["pump_window_h"])
    parser.add_argument("--delay",       type=int,   default=DEFAULTS["entry_delay_h"])
    parser.add_argument("--sl",          type=float, default=DEFAULTS["stop_loss_pct"])
    parser.add_argument("--tp_atr",      type=float, default=DEFAULTS["tp_atr"],
                        help="TP = N × ATR under entry (default 1.0)")
    parser.add_argument("--max_hold",    type=int,   default=DEFAULTS["max_hold_h"])
    parser.add_argument("--cooldown",    type=int,   default=DEFAULTS["cooldown_h"])
    parser.add_argument("--days",        type=int,   default=DEFAULTS["backtest_days"])
    parser.add_argument("--interval",    type=str,   default=DEFAULTS["interval"],
                        choices=["15m","30m","1h","2h","4h"])
    parser.add_argument("--capital",     type=float, default=DEFAULTS["initial_capital"])
    parser.add_argument("--risk",        type=float, default=DEFAULTS["risk_per_trade"])
    parser.add_argument("--no_vol",       action="store_true")
    parser.add_argument("--kelly",         type=float, default=0.5,
                        help="Kelly fraction: 0.5=half Kelly (anbefalet), 1.0=full Kelly")
    parser.add_argument("--no_kelly",      action="store_true",
                        help="Brug fast --risk i stedet for Kelly sizing")
    parser.add_argument("--kelly_window",  type=int, default=20,
                        help="Rullende vindue til adaptiv Kelly beregning")
    parser.add_argument("--smart_filter",  action="store_true",
                        help="Filtrer coins ud fra faktor-analyse (MC, 30d trend, vol/MC, supply)")
    parser.add_argument("--mc_min",        type=float, default=SMART_FILTER_DEFAULTS["mc_min"])
    parser.add_argument("--mc_max",        type=float, default=SMART_FILTER_DEFAULTS["mc_max"])
    parser.add_argument("--change_30d",    type=float, default=SMART_FILTER_DEFAULTS["change_30d_max"])
    parser.add_argument("--vol_mc_min",    type=float, default=SMART_FILTER_DEFAULTS["vol_mc_min"])
    parser.add_argument("--circ_max",      type=float, default=SMART_FILTER_DEFAULTS["circ_pct_max"])
    parser.add_argument("--save_stats",    action="store_true",
                        help="Gem backtest-stats til backtest_stats.json (bruges af live_scanner.py)")
    args = parser.parse_args()

    params = {
        "pump_pct":         args.pump_pct,
        "pump_window_h":    args.pump_window,
        "entry_delay_h":    args.delay,
        "stop_loss_pct":    args.sl,
        "tp_atr":           args.tp_atr,
        "max_hold_h":       args.max_hold,
        "cooldown_h":       args.cooldown,
        "risk_per_trade":   args.risk,
        "initial_capital":  args.capital,
        "backtest_days":    args.days,
        "interval":         args.interval,
        "fee_pct":          DEFAULTS["fee_pct"],
        "slippage_pct":     DEFAULTS["slippage_pct"],
        "rsi_max":          DEFAULTS["rsi_max"],
        "volume_filter":    not args.no_vol,
        "min_pump_candles": DEFAULTS["min_pump_candles"],
        "top_n":            args.top,
        # Kelly — sættes til fast risk hvis --no_kelly
        "kelly_fraction":   args.risk if args.no_kelly else None,
        "kelly_frac_mult":  args.kelly,
        "kelly_window":     args.kelly_window,
        "use_kelly":        not args.no_kelly,
    }

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
        print(f"\n  {len(symbols)} specificerede symboler")
    else:
        print(f"\n  Scanner top-{args.top} altcoins på Binance...")
        top = fetch_top_altcoins()
        symbols = [t["symbol"] for t in top[:args.top]]
        print(f"  Fundet {len(symbols)} symboler")

    # ── Smart filter ──
    if args.smart_filter:
        sf = {
            "mc_min":         args.mc_min,
            "mc_max":         args.mc_max,
            "change_30d_max": args.change_30d,
            "vol_mc_min":     args.vol_mc_min,
            "circ_pct_max":   args.circ_max,
        }
        print(f"\n  Smart filter aktivt:"
              f"  MC ${sf['mc_min']/1e6:.0f}M–${sf['mc_max']/1e9:.1f}B"
              f"  30d<{sf['change_30d_max']}%"
              f"  Vol/MC>{sf['vol_mc_min']}"
              f"  Supply<{sf['circ_pct_max']}%")
        symbols, _ = apply_smart_filter(symbols, sf, verbose=True)
        if not symbols:
            print("  Ingen symboler overlevede filteret — prøv bredere kriterier")
            return

    print(f"\n  pump≥{params['pump_pct']}%  TP={params['tp_atr']}×ATR"
          f"  SL={params['stop_loss_pct']}%  hold≤{params['max_hold_h']}t"
          f"  {params['interval']}")
    if params["use_kelly"]:
        print(f"  Position sizing: {'Half' if args.kelly==0.5 else ''}Kelly "
              f"(fraction={args.kelly}, rullende vindue={args.kelly_window})\n")
    else:
        print(f"  Position sizing: fast {args.risk*100:.1f}% pr. trade\n")

    # ── Første pass: kør med fast risk for at beregne initial Kelly ──
    # Kelly kræver historik — vi bruger et warm-up pass på 30 dage
    if params["use_kelly"]:
        print("  Beregner initial Kelly fra warm-up pass (30 dage)...")
        warmup_params = {**params, "backtest_days": 30,
                         "kelly_fraction": args.risk, "use_kelly": False}
        warmup_trades = []
        for sym in symbols[:min(10, len(symbols))]:
            try:
                t, _ = backtest_pump_dump(sym, warmup_params)
                warmup_trades.extend(t)
                time.sleep(0.1)
            except Exception:
                pass

        if warmup_trades:
            wt_pnls  = [t["pnl"] for t in warmup_trades]
            wt_wins  = [p for p in wt_pnls if p > 0]
            wt_loss  = [p for p in wt_pnls if p < 0]
            init_wr  = len(wt_wins) / len(wt_pnls) if wt_pnls else 0.5
            init_aw  = np.mean(wt_wins)  if wt_wins  else 50
            init_al  = abs(np.mean(wt_loss)) if wt_loss else 50
            init_k   = kelly_fraction(init_wr, init_aw, init_al, args.kelly)
            print(f"  Initial Kelly: {init_k*100:.2f}%  "
                  f"(WR={init_wr*100:.1f}% W={init_aw:.0f} L={init_al:.0f})\n")
            params["kelly_fraction"] = init_k
        else:
            params["kelly_fraction"] = args.risk
            print(f"  Ikke nok warm-up data — bruger {args.risk*100:.1f}%\n")

    all_trades = []
    for sym in symbols:
        try:
            print(f"  {sym:<14}", end=" ", flush=True)
            trades, _ = backtest_pump_dump(sym, params)
            all_trades.extend(trades)
            if trades:
                wins = sum(1 for t in trades if t["pnl"] > 0)
                pnl  = sum(t["pnl"] for t in trades)
                tp_n = sum(1 for t in trades if t["reason"] == "TP")
                print(f"{len(trades):>3} handler  WR={wins/len(trades)*100:.0f}%"
                      f"  TP={tp_n}  P&L=${pnl:+.0f}")
            else:
                print("0 handler")
            time.sleep(0.15)
        except Exception as e:
            print(f"FEJL: {e}")

    print()
    if not all_trades:
        print("  Ingen handler — prøv: --pump_pct 15 --no_vol")
        return

    metrics = compute_metrics(all_trades, params["initial_capital"])
    print_report(
        f"PUMP & DUMP v2+  |  {len(symbols)} symboler  |  {args.days} dage",
        metrics, all_trades, show_trades=15,
    )
    print(reason_breakdown(all_trades))
    print(symbol_breakdown(all_trades))

    # ── Kelly opsummering ──
    if params["use_kelly"] and all_trades:
        pnls   = [t["pnl"] for t in all_trades]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        wr     = len(wins) / len(pnls)
        aw     = np.mean(wins)        if wins   else 0
        al     = abs(np.mean(losses)) if losses else 0
        fk     = kelly_fraction(wr, aw, al, fraction=1.0)
        hk     = kelly_fraction(wr, aw, al, fraction=0.5)
        qk     = kelly_fraction(wr, aw, al, fraction=0.25)
        w = 56
        print(f"\n{'═'*w}")
        print(f"  KELLY POSITION SIZING — baseret på backtest")
        print(f"{'═'*w}")
        print(f"  Win rate    : {wr*100:.1f}%")
        print(f"  Gns. gevinst: ${aw:.2f}")
        print(f"  Gns. tab    : ${al:.2f}")
        print(f"  Odds (b)    : {aw/al:.2f}:1")
        print(f"{'─'*w}")
        print(f"  Full Kelly  : {fk*100:.2f}%  per trade  (max varians)")
        print(f"  Half Kelly  : {hk*100:.2f}%  per trade  ← anbefalet")
        print(f"  Quarter K.  : {qk*100:.2f}%  per trade  (konservativ)")
        print(f"{'─'*w}")
        print(f"  Eksempel ($10.000 kapital):")
        print(f"    Half Kelly risikerer ${10000*hk:,.0f} pr. trade")
        print(f"    → Position størrelse: ${10000*hk/(params['stop_loss_pct']/100):,.0f}")
        print(f"{'═'*w}\n")

    # Gem stats til live_scanner
    if args.save_stats:
        save_backtest_stats(all_trades)

    plot_results(all_trades, metrics["equity_curve"], params)


if __name__ == "__main__":
    main()
