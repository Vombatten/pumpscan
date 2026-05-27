"""
grade_signal.py — Setup graderingssystem A/B/C (v2 — inverterede tærskler)

Baseret på backtest-resultater viste moderate pumps med lavere RSI
bedre performance end ekstreme pumps. Kriterierne er derfor inverteret:

  A (score 8-10): Moderat pump, lav RSI, moderat volumen → 7% risiko
  B (score 5-7):  Middel pump, middel RSI               → 5% risiko
  C (score 0-4):  Ekstrem pump, høj RSI, høj volumen    → 3% risiko

Position sizing (af AKTUEL kapital — dynamisk):
  A = 7%  |  B = 5%  |  C = 3%
"""

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
#  Scoring — INVERTERET: moderate setups = bedre
# ─────────────────────────────────────────────

GRADE_RISK = {
    "A": 0.07,   # 7% af aktuel kapital
    "B": 0.05,   # 5%
    "C": 0.03,   # 3%
}


def grade_signal(
    pump_pct:    float,
    rsi:         float,
    entry_price: float,
    pump_high:   float,
    atr:         float,
    avg_volume:  float,
    pump_volume: float,
) -> dict:
    """
    Scorer et signal og returnerer grade A/B/C.

    Inverteret logik: moderate pumps med lav RSI = bedst (A).
    Ekstreme pumps med meget høj RSI = lavest grade (C).
    """

    score = 0

    # ── 1. Pump størrelse (2 pt) ──
    # Moderat pump = bedst, ekstrem pump = dårligst
    if 20 <= pump_pct < 35:
        s_pump = 2   # Moderat — bedste entry
    elif 35 <= pump_pct < 60:
        s_pump = 1   # Middel
    else:
        s_pump = 0   # Ekstrem (>60%) — pumpen er sandsynligvis for fremskreden
    score += s_pump

    # ── 2. RSI niveau (2 pt) ──
    # Lavere RSI = mere plads til at prisen fortsætter ned
    if rsi < 65:
        s_rsi = 2    # Lav RSI — god short
    elif rsi < 75:
        s_rsi = 1    # Middel
    else:
        s_rsi = 0    # Meget høj RSI (>75) — muligvis for sent
    score += s_rsi

    # ── 3. Volumen spike (2 pt) ──
    # Moderat volumen = signal uden frenzy-toppen er nået
    vol_spike = pump_volume / avg_volume if avg_volume > 0 else 1.0
    if 1.5 <= vol_spike < 2.5:
        s_vol = 2    # Moderat spike
    elif 2.5 <= vol_spike < 4.0:
        s_vol = 1    # Højt
    else:
        s_vol = 0    # Meget høj (>4×) — kan være frenzy-top, eller meget lav (<1.5)
    score += s_vol

    # ── 4. Entry position fra pump high (2 pt) ──
    # Lidt under pump top = bedre (ikke allerede faldet meget)
    entry_from_top = (pump_high - entry_price) / pump_high * 100 if pump_high > 0 else 99
    if 3 <= entry_from_top <= 12:
        s_entry = 2  # Tæt på top men ikke på toppen
    elif entry_from_top < 3 or (12 < entry_from_top <= 20):
        s_entry = 1  # Enten meget tæt på top, eller lidt længere fra
    else:
        s_entry = 0  # Meget langt fra top (>20%) — pumpen er for gammel
    score += s_entry

    # ── 5. ATR/pris ratio (2 pt) ──
    # Moderat volatilitet = bedst for SL/TP management
    atr_ratio = atr / entry_price * 100 if entry_price > 0 else 0
    if 1.5 <= atr_ratio <= 4.0:
        s_atr = 2    # God volatilitet — TP nås realistisk
    elif 1.0 <= atr_ratio < 1.5 or 4.0 < atr_ratio <= 6.0:
        s_atr = 1    # Acceptabelt
    else:
        s_atr = 0    # For lav (<1%) eller ekstrem høj (>6%)
    score += s_atr

    # ── Grade ──
    if score >= 8:
        grade = "A"
    elif score >= 5:
        grade = "B"
    else:
        grade = "C"

    return {
        "grade":          grade,
        "score":          score,
        "score_max":      10,
        "risk_pct":       GRADE_RISK[grade],
        "details": {
            "pump_pct":       {"value": round(pump_pct, 1),       "score": s_pump,  "max": 2},
            "rsi":            {"value": round(rsi, 1),            "score": s_rsi,   "max": 2},
            "vol_spike":      {"value": round(vol_spike, 2),      "score": s_vol,   "max": 2},
            "entry_from_top": {"value": round(entry_from_top, 1), "score": s_entry, "max": 2},
            "atr_ratio":      {"value": round(atr_ratio, 2),      "score": s_atr,   "max": 2},
        },
    }


def grade_label(grade: str) -> str:
    return {"A": "A ★★★", "B": "B ★★ ", "C": "C ★  "}.get(grade, "?")


def format_grade_details(g: dict) -> str:
    d = g["details"]
    lines = [
        f"  Grade: {grade_label(g['grade'])}  "
        f"(score {g['score']}/{g['score_max']})  "
        f"→ risiko {g['risk_pct']*100:.0f}%",
        f"  {'Faktor':<18} {'Værdi':>8}  {'Point':>5}",
        f"  {'─'*36}",
    ]
    labels = {
        "pump_pct":       "Pump størrelse",
        "rsi":            "RSI",
        "vol_spike":      "Volumen spike",
        "entry_from_top": "Entry fra top",
        "atr_ratio":      "ATR ratio",
    }
    for k, info in d.items():
        dots = "●" * info["score"] + "○" * (info["max"] - info["score"])
        unit = {"pump_pct":"%","rsi":"","vol_spike":"×",
                "entry_from_top":"%","atr_ratio":"%"}.get(k,"")
        lines.append(
            f"  {labels[k]:<18} {info['value']:>6.1f}{unit}  {dots:>5}"
        )
    return "\n".join(lines)
