"""
signal_tracker.py — SQLite-baseret signal tracker (cloud-kompatibel)
Data gemmes permanent i SQLite — overlever server-genstarter.
"""

import json, os, threading, time, sqlite3
from datetime import datetime, timezone
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH",
          os.path.join(os.path.dirname(__file__), "signals.db"))


class SignalTracker:
    def __init__(self):
        self._lock   = threading.RLock()
        self._feed   = None
        self._running= False
        self._init_db()

    # ─────────────────────────────────────────────
    #  Database
    # ─────────────────────────────────────────────

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._db() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    key         TEXT PRIMARY KEY,
                    symbol      TEXT,
                    symbol_full TEXT,
                    entry       REAL,
                    sl          REAL,
                    tp          REAL,
                    sl_pct      REAL,
                    tp_pct      REAL,
                    pump_size   REAL,
                    rsi         REAL,
                    kelly_pct   REAL,
                    risk_usd    REAL,
                    pos_usd     REAL,
                    ts          TEXT,
                    first_seen  TEXT,
                    tracked_at  TEXT,
                    cur_price   REAL,
                    cur_pct     REAL,
                    dist_tp_pct REAL,
                    dist_sl_pct REAL,
                    outcome     TEXT,
                    exit_price  REAL,
                    realized_pnl REAL,
                    duration    TEXT,
                    closed_at   TEXT,
                    raw_json    TEXT
                )
            """)

    # ─────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────

    def set_feed(self, feed):
        self._feed = feed

    def add_signal(self, sig: dict):
        key = sig.get("symbol_full","") + "_" + sig.get("ts","")[:13]
        now = datetime.now(timezone.utc).isoformat()
        with self._db() as db:
            existing = db.execute(
                "SELECT key FROM signals WHERE key=?", (key,)).fetchone()
            if existing:
                return
            db.execute("""
                INSERT INTO signals
                (key,symbol,symbol_full,entry,sl,tp,sl_pct,tp_pct,
                 pump_size,rsi,kelly_pct,risk_usd,pos_usd,ts,
                 first_seen,tracked_at,cur_price,raw_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                key,
                sig.get("symbol",""), sig.get("symbol_full",""),
                sig.get("entry",0), sig.get("sl",0), sig.get("tp",0),
                sig.get("sl_pct",8), sig.get("tp_pct",0),
                sig.get("pump_size",0), sig.get("rsi",0),
                sig.get("kelly_pct",0), sig.get("risk_usd",0),
                sig.get("pos_usd",0), sig.get("ts",""),
                sig.get("first_seen",""), now,
                sig.get("entry",0),
                json.dumps(sig),
            ))

    def _update_price(self, key, cur, dist_tp, dist_sl, cur_pct):
        with self._db() as db:
            db.execute("""
                UPDATE signals SET cur_price=?,dist_tp_pct=?,dist_sl_pct=?,cur_pct=?
                WHERE key=? AND outcome IS NULL
            """, (cur, dist_tp, dist_sl, cur_pct, key))

    def _close_signal(self, key, outcome, exit_price, capital=10_000):
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM signals WHERE key=? AND outcome IS NULL",
                (key,)).fetchone()
            if not row:
                return

            entry  = row["entry"]
            sl_pct = row["sl_pct"] / 100
            kelly  = row["kelly_pct"] / 100
            risk   = capital * kelly

            if outcome == "WIN":
                move   = (entry - exit_price) / entry
                pnl    = risk * (move / sl_pct)
            else:
                pnl = -risk

            tracked_at = row["tracked_at"] or ""
            try:
                t_dt    = datetime.fromisoformat(tracked_at.replace("Z",""))
                dur_h   = (datetime.now() - t_dt).total_seconds() / 3600
                dur_str = f"{int(dur_h)}H {int((dur_h%1)*60)}M"
            except Exception:
                dur_str = "—"

            now = datetime.now(timezone.utc).isoformat()
            db.execute("""
                UPDATE signals
                SET outcome=?,exit_price=?,realized_pnl=?,duration=?,closed_at=?
                WHERE key=?
            """, (outcome, round(exit_price,6), round(pnl,2),
                  dur_str, now, key))

    def get_active(self) -> list:
        with self._db() as db:
            rows = db.execute(
                "SELECT * FROM signals WHERE outcome IS NULL "
                "ORDER BY tracked_at DESC").fetchall()
        now = datetime.now(timezone.utc)
        result = []
        for r in rows:
            d = dict(r)
            # Beregn age_h fra tracked_at
            try:
                ta    = datetime.fromisoformat(d["tracked_at"].replace("Z",""))
                age_h = (now - ta.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            except Exception:
                age_h = 0
            d["age_h"]      = round(age_h, 2)
            d["is_new"]     = age_h < 0.25
            d["first_seen"] = d.get("first_seen") or (
                datetime.fromisoformat(d["tracked_at"].replace("Z","")).strftime("%H:%M")
                if d.get("tracked_at") else "—"
            )
            # bar_pos: hvor er entry mellem TP og SL (0-100%)
            entry = d.get("entry", 0)
            sl    = d.get("sl", 0)
            tp    = d.get("tp", 0)
            cur   = d.get("cur_price") or entry
            rng   = sl - tp
            d["bar_pos"]  = round(max(2, min(96, (sl-entry)/rng*100)), 1) if rng > 0 else 50
            d["cur_pos"]  = round(max(2, min(97, (sl-cur)/rng*100)), 1)  if rng > 0 else 50
            result.append(d)
        return result

    def get_closed(self, n=50) -> list:
        with self._db() as db:
            rows = db.execute(
                "SELECT * FROM signals WHERE outcome IS NOT NULL "
                "ORDER BY closed_at DESC LIMIT ?", (n,)).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict:
        with self._db() as db:
            closed = db.execute(
                "SELECT * FROM signals WHERE outcome IS NOT NULL").fetchall()
            active_n = db.execute(
                "SELECT COUNT(*) as n FROM signals WHERE outcome IS NULL"
            ).fetchone()["n"]

        if not closed:
            return {"n":0,"wins":0,"losses":0,"wr":0,
                    "net_pnl":0,"pf":0,"avg_win":0,"avg_loss":0,
                    "active":active_n}

        wins  = [r for r in closed if r["outcome"]=="WIN"]
        losses= [r for r in closed if r["outcome"]=="LOSS"]
        gw    = sum(r["realized_pnl"] or 0 for r in wins)
        gl    = sum(abs(r["realized_pnl"] or 0) for r in losses)

        return {
            "n":       len(closed),
            "wins":    len(wins),
            "losses":  len(losses),
            "active":  active_n,
            "wr":      round(len(wins)/len(closed)*100,1) if closed else 0,
            "net_pnl": round(gw-gl,2),
            "pf":      round(gw/gl,2) if gl>0 else 0,
            "avg_win": round(gw/len(wins),2) if wins else 0,
            "avg_loss":round(gl/len(losses),2) if losses else 0,
        }

    # ─────────────────────────────────────────────
    #  Background monitor
    # ─────────────────────────────────────────────

    def _monitor_loop(self, capital=10_000):
        while self._running:
            if self._feed:
                active = self.get_active()
                for sig in active:
                    sym = sig.get("symbol_full","")
                    cur = self._feed.last_price(sym)
                    if not cur:
                        continue
                    entry = sig.get("entry",0)
                    tp    = sig.get("tp",0)
                    sl    = sig.get("sl",0)
                    key   = sig.get("key","")

                    cur_pct     = round((entry-cur)/entry*100,2)
                    dist_tp_pct = round((cur-tp)/entry*100,2)
                    dist_sl_pct = round((sl-cur)/entry*100,2)

                    self._update_price(key, round(cur,6),
                                       dist_tp_pct, dist_sl_pct, cur_pct)

                    if cur <= tp:
                        self._close_signal(key, "WIN",  tp,  capital)
                    elif cur >= sl:
                        self._close_signal(key, "LOSS", sl, capital)
            time.sleep(2)

    def start(self, capital=10_000):
        self._running = True
        threading.Thread(target=self._monitor_loop,
                         args=(capital,), daemon=True).start()

    def stop(self):
        self._running = False
