"""
signal_tracker.py — SQLite signal tracker med "taget" og manuel outcome
"""

import json, os, threading, time, sqlite3
from datetime import datetime, timezone
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH",
          os.path.join(os.path.dirname(__file__), "signals.db"))


class SignalTracker:
    def __init__(self):
        self._lock    = threading.RLock()
        self._feed    = None
        self._running = False
        self._init_db()

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
                    key             TEXT PRIMARY KEY,
                    symbol          TEXT,
                    symbol_full     TEXT,
                    entry           REAL,
                    sl              REAL,
                    tp              REAL,
                    sl_pct          REAL,
                    tp_pct          REAL,
                    pump_size       REAL,
                    rsi             REAL,
                    kelly_pct       REAL,
                    risk_usd        REAL,
                    pos_usd         REAL,
                    ts              TEXT,
                    first_seen      TEXT,
                    tracked_at      TEXT,
                    cur_price       REAL,
                    cur_pct         REAL,
                    dist_tp_pct     REAL,
                    dist_sl_pct     REAL,
                    outcome         TEXT,
                    manual_outcome  TEXT,
                    taken           INTEGER DEFAULT 0,
                    exit_price      REAL,
                    realized_pnl    REAL,
                    duration        TEXT,
                    closed_at       TEXT,
                    raw_json        TEXT
                )
            """)
            # Migrer eksisterende DB hvis nødvendigt
            try:
                db.execute("ALTER TABLE signals ADD COLUMN taken INTEGER DEFAULT 0")
            except: pass
            try:
                db.execute("ALTER TABLE signals ADD COLUMN manual_outcome TEXT")
            except: pass

    # ─────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────

    def _enrich(self, d: dict) -> dict:
        """Tilføj beregnede felter til et signal-dict."""
        now = datetime.now(timezone.utc)
        try:
            ta    = datetime.fromisoformat(d["tracked_at"].replace("Z",""))
            age_h = (now - ta.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        except Exception:
            age_h = 0
        d["age_h"]  = round(age_h, 2)
        d["is_new"] = age_h < 0.25
        d["first_seen"] = d.get("first_seen") or (
            datetime.fromisoformat(d["tracked_at"].replace("Z","")).strftime("%H:%M")
            if d.get("tracked_at") else "—"
        )
        entry = d.get("entry", 0) or 0
        sl    = d.get("sl", 0)    or 0
        tp    = d.get("tp", 0)    or 0
        cur   = d.get("cur_price") or entry
        rng   = sl - tp
        d["bar_pos"] = round(max(2,min(96,(sl-entry)/rng*100)),1) if rng>0 else 50
        d["cur_pos"] = round(max(2,min(97,(sl-cur  )/rng*100)),1) if rng>0 else 50
        d["taken"]   = bool(d.get("taken", 0))

        # Effektivt outcome: auto > manuelt
        d["effective_outcome"] = d.get("outcome") or d.get("manual_outcome")

        # Er signalet lukket inden for de seneste 30 min?
        if d.get("closed_at"):
            try:
                closed = datetime.fromisoformat(d["closed_at"].replace("Z",""))
                min_since_close = (now - closed.replace(tzinfo=timezone.utc)).total_seconds() / 60
                d["recently_closed"] = min_since_close < 30
                d["min_since_close"] = round(min_since_close, 0)
            except Exception:
                d["recently_closed"] = False
        else:
            d["recently_closed"] = False

        return d

    # ─────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────

    def set_feed(self, feed):
        self._feed = feed

    def add_signal(self, sig: dict):
        key = sig.get("symbol_full","") + "_" + sig.get("ts","")[:13]
        now = datetime.now(timezone.utc).isoformat()
        with self._db() as db:
            if db.execute("SELECT key FROM signals WHERE key=?", (key,)).fetchone():
                return
            db.execute("""
                INSERT INTO signals
                (key,symbol,symbol_full,entry,sl,tp,sl_pct,tp_pct,
                 pump_size,rsi,kelly_pct,risk_usd,pos_usd,ts,
                 first_seen,tracked_at,cur_price,raw_json,taken)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
            """, (
                key,
                sig.get("symbol",""), sig.get("symbol_full",""),
                sig.get("entry",0), sig.get("sl",0), sig.get("tp",0),
                sig.get("sl_pct",8), sig.get("tp_pct",0),
                sig.get("pump_size",0), sig.get("rsi",0),
                sig.get("kelly_pct",0), sig.get("risk_usd",0),
                sig.get("pos_usd",0), sig.get("ts",""),
                sig.get("first_seen",""), now,
                sig.get("entry",0), json.dumps(sig),
            ))

    def set_taken(self, key: str, taken: bool):
        """Marker om trade er taget."""
        with self._db() as db:
            db.execute("UPDATE signals SET taken=? WHERE key=?",
                       (1 if taken else 0, key))

    def set_manual_outcome(self, key: str, outcome: str | None):
        """
        Sæt manuelt udfald: 'WIN', 'LOSS' eller None.
        Bruges når trade allerede er lukket på exchange.
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._db() as db:
            row = db.execute("SELECT * FROM signals WHERE key=?", (key,)).fetchone()
            if not row:
                return

            if outcome and not row["realized_pnl"]:
                # Beregn estimeret P&L
                entry  = row["entry"] or 0
                sl_pct = (row["sl_pct"] or 8) / 100
                kelly  = (row["kelly_pct"] or 4) / 100
                risk   = 100 * kelly   # Fallback kapital ($100 test)
                if outcome == "WIN":
                    tp   = row["tp"] or entry * 0.95
                    move = (entry - tp) / entry if entry > 0 else 0
                    pnl  = risk * (move / sl_pct) if sl_pct > 0 else 0
                else:
                    pnl = -risk
                db.execute("""
                    UPDATE signals SET manual_outcome=?, realized_pnl=?, closed_at=?
                    WHERE key=?
                """, (outcome, round(pnl,2), now, key))
            else:
                db.execute("UPDATE signals SET manual_outcome=? WHERE key=?",
                           (outcome, key))

    def _update_price(self, key, cur, dist_tp, dist_sl, cur_pct):
        with self._db() as db:
            db.execute("""
                UPDATE signals SET cur_price=?,dist_tp_pct=?,dist_sl_pct=?,cur_pct=?
                WHERE key=? AND outcome IS NULL
            """, (cur, dist_tp, dist_sl, cur_pct, key))

    def _close_auto(self, key: str, outcome: str, exit_price: float,
                    capital: float = 10_000):
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM signals WHERE key=? AND outcome IS NULL",
                (key,)).fetchone()
            if not row:
                return
            entry  = row["entry"] or 0
            sl_pct = (row["sl_pct"] or 8) / 100
            kelly  = (row["kelly_pct"] or 4) / 100
            risk   = capital * kelly
            if outcome == "WIN":
                move = (entry - exit_price) / entry if entry > 0 else 0
                pnl  = risk * (move / sl_pct) if sl_pct > 0 else 0
            else:
                pnl = -risk
            try:
                ta      = datetime.fromisoformat(row["tracked_at"].replace("Z",""))
                dur_h   = (datetime.now() - ta).total_seconds() / 3600
                dur_str = f"{int(dur_h)}H {int((dur_h%1)*60)}M"
            except Exception:
                dur_str = "—"
            now = datetime.now(timezone.utc).isoformat()
            db.execute("""
                UPDATE signals
                SET outcome=?,exit_price=?,realized_pnl=?,duration=?,closed_at=?
                WHERE key=?
            """, (outcome, round(exit_price,6), round(pnl,2), dur_str, now, key))

    def get_active(self) -> list:
        with self._db() as db:
            rows = db.execute(
                "SELECT * FROM signals WHERE outcome IS NULL "
                "ORDER BY tracked_at DESC").fetchall()
        return [self._enrich(dict(r)) for r in rows]

    def get_all(self, n=100) -> list:
        """Alle signaler — både aktive og lukkede."""
        with self._db() as db:
            rows = db.execute(
                "SELECT * FROM signals ORDER BY tracked_at DESC LIMIT ?",
                (n,)).fetchall()
        return [self._enrich(dict(r)) for r in rows]

    def get_closed(self, n=50) -> list:
        with self._db() as db:
            rows = db.execute("""
                SELECT * FROM signals
                WHERE outcome IS NOT NULL OR manual_outcome IS NOT NULL
                ORDER BY COALESCE(closed_at, tracked_at) DESC LIMIT ?
            """, (n,)).fetchall()
        return [self._enrich(dict(r)) for r in rows]

    def stats(self) -> dict:
        """Stats KUN for tagne trades."""
        with self._db() as db:
            # Kun tagne + lukkede
            closed = db.execute("""
                SELECT * FROM signals
                WHERE taken=1
                AND (outcome IS NOT NULL OR manual_outcome IS NOT NULL)
            """).fetchall()
            active_n  = db.execute(
                "SELECT COUNT(*) n FROM signals WHERE outcome IS NULL").fetchone()["n"]
            taken_open = db.execute(
                "SELECT COUNT(*) n FROM signals WHERE taken=1 AND outcome IS NULL "
                "AND manual_outcome IS NULL").fetchone()["n"]
            total_sigs = db.execute(
                "SELECT COUNT(*) n FROM signals").fetchone()["n"]
            taken_total= db.execute(
                "SELECT COUNT(*) n FROM signals WHERE taken=1").fetchone()["n"]

        if not closed:
            return {"n":0,"wins":0,"losses":0,"wr":0,"net_pnl":0,"pf":0,
                    "avg_win":0,"avg_loss":0,"active":active_n,
                    "taken_open":taken_open,"total_sigs":total_sigs,
                    "taken_total":taken_total}

        def outcome_of(r):
            return r["outcome"] or r["manual_outcome"]

        wins   = [r for r in closed if outcome_of(r)=="WIN"]
        losses = [r for r in closed if outcome_of(r)=="LOSS"]
        gw     = sum(r["realized_pnl"] or 0 for r in wins)
        gl     = sum(abs(r["realized_pnl"] or 0) for r in losses)

        return {
            "n":           len(closed),
            "wins":        len(wins),
            "losses":      len(losses),
            "active":      active_n,
            "taken_open":  taken_open,
            "total_sigs":  total_sigs,
            "taken_total": taken_total,
            "wr":          round(len(wins)/len(closed)*100,1) if closed else 0,
            "net_pnl":     round(gw-gl,2),
            "pf":          round(gw/gl,2) if gl>0 else 0,
            "avg_win":     round(gw/len(wins),2)   if wins   else 0,
            "avg_loss":    round(gl/len(losses),2) if losses else 0,
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
                    if not cur: continue
                    entry = sig.get("entry",0)
                    tp    = sig.get("tp",0)
                    sl    = sig.get("sl",0)
                    key   = sig.get("key","")
                    rng   = sl - tp if sl and tp else 1
                    self._update_price(
                        key, round(cur,6),
                        round((cur-tp)/entry*100,2) if entry else 0,
                        round((sl-cur)/entry*100,2) if entry else 0,
                        round((entry-cur)/entry*100,2) if entry else 0,
                    )
                    # Auto-close når TP eller SL rammes — tagne OG utagne
                    # (stats tæller kun tagne, men outcome registreres for alle)
                    if not sig.get("manual_outcome") and not sig.get("outcome"):
                        if cur <= tp:
                            self._close_auto(key,"WIN", tp, capital)
                        elif cur >= sl:
                            self._close_auto(key,"LOSS",sl, capital)
            time.sleep(2)

    def start(self, capital=10_000):
        self._running = True
        threading.Thread(target=self._monitor_loop,
                         args=(capital,), daemon=True).start()

    def stop(self):
        self._running = False
