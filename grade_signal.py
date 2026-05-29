"""
grade_signal.py — Setup graderingssystem A/B/C (inverterede tærskler)

Moderate pumps med lav RSI = bedst (A).
Ekstreme pumps med høj RSI = lavest grade (C).

  A (score ≥6): 7% risiko
  B (score ≥3): 5% risiko  
  C (score <3): Skip
"""

GRADE_RISK = {
    "A": 0.07,
    "B": 0.05,
    "C": 0.03,
}

SKIP_GRADES = {"C"}


def grade_signal(pump_pct, rsi, entry_price, pump_high,
                 atr, avg_volume, pump_volume) -> dict:
    score = 0

    # 1. Pump størrelse (moderat = bedst)
    if 15 <= pump_pct < 40:   s_pump = 2
    elif 40 <= pump_pct < 60: s_pump = 1
    else:                      s_pump = 0
    score += s_pump

    # 2. RSI (lav = bedst)
    if rsi < 65:   s_rsi = 2
    elif rsi < 75: s_rsi = 1
    else:          s_rsi = 0
    score += s_rsi

    # 3. Volumen spike (moderat = bedst)
    vol = pump_volume / avg_volume if avg_volume > 0 else 1.0
    if 1.5 <= vol < 3.0:   s_vol = 2
    elif 3.0 <= vol < 5.0: s_vol = 1
    else:                   s_vol = 0
    score += s_vol

    # 4. Entry fra pump top (3-15% = bedst)
    from_top = (pump_high - entry_price) / pump_high * 100 if pump_high > 0 else 99
    if 3 <= from_top <= 15:                       s_entry = 2
    elif from_top < 3 or 15 < from_top <= 25:     s_entry = 1
    else:                                          s_entry = 0
    score += s_entry

    # 5. ATR ratio (1.5-5% = bedst)
    atr_ratio = atr / entry_price * 100 if entry_price > 0 else 0
    if 1.5 <= atr_ratio <= 5.0:  s_atr = 2
    elif 1.0 <= atr_ratio < 1.5: s_atr = 1
    else:                         s_atr = 0
    score += s_atr

    # Grade
    if score >= 6:   grade = "A"
    elif score >= 3: grade = "B"
    else:            grade = "C"

    return {
        "grade":    grade,
        "score":    score,
        "score_max":10,
        "risk_pct": GRADE_RISK[grade],
        "details": {
            "pump_pct":       {"value": round(pump_pct, 1), "score": s_pump, "max": 2},
            "rsi":            {"value": round(rsi, 1),      "score": s_rsi,  "max": 2},
            "vol_spike":      {"value": round(vol, 2),       "score": s_vol,  "max": 2},
            "entry_from_top": {"value": round(from_top, 1), "score": s_entry,"max": 2},
            "atr_ratio":      {"value": round(atr_ratio, 2),"score": s_atr,  "max": 2},
        },
    }
