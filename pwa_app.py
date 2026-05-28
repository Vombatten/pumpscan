"""
pwa_app.py — Pump & Dump Scanner PWA
Binance WebSocket feed + live signal outcome tracking
"""

import sys, os, time, json, threading, struct, zlib
from datetime import datetime, timezone
from flask import Flask, jsonify, request, Response

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from utils import fetch_top_altcoins, rsi, atr
    from strategy_pump_dump import detect_pumps, interval_to_hours
    import pandas as pd, numpy as np
    SCANNER_OK = True
except ImportError as e:
    print(f"  Advarsel: {e}"); SCANNER_OK = False

try:
    from binance_feed import BinanceFeed
    FEED_OK = True
except ImportError:
    FEED_OK = False

try:
    from replay_scan import replay as run_replay
    REPLAY_OK = True
except ImportError:
    REPLAY_OK = False

try:
    from signal_tracker import SignalTracker
    tracker = SignalTracker()
except ImportError:
    tracker = None

app         = Flask(__name__)
feed        = None
seen_sigs   = {}

PARAMS = {
    "pump_pct":20,"pump_window_h":24,"entry_delay_h":2,
    "stop_loss_pct":8,"tp_atr":1.5,"rsi_max":80,
    "fee_pct":0.06,"slippage_pct":0.15,
    "volume_filter":True,"min_pump_candles":2,
    "interval":"1h","capital":100,"top_n":40,
}
STATS = {"win_rate":0.701,"avg_win":121.0,"avg_loss":190.0}
state = {
    "signals":[],"last_scan":None,"scanning":False,
    "error":None,"n_symbols":0,"scan_count":0,
    "feed_status":"Ikke startet","feed_connected":False,
}

def calc_kelly(s,f=0.5):
    wr,aw,al=s["win_rate"],s["avg_win"],s["avg_loss"]
    if al<=0 or wr<=0 or wr>=1: return 0.02
    return round(min(max(0,(aw/al*wr-(1-wr))/(aw/al))*f,0.25),4)

def init_feed(symbols):
    global feed
    if not FEED_OK or not SCANNER_OK: return
    try:
        state["feed_status"]="Henter historik fra Binance..."
        feed = BinanceFeed()
        feed.subscribe(symbols, interval=PARAMS["interval"])
        feed.start(seed_history=True, timeout=20)
        state["feed_status"]=f"Live ✓  ({len(symbols)} symboler)"
        state["feed_connected"]=True
        if tracker:
            tracker.set_feed(feed)
            tracker.start(capital=PARAMS["capital"])
    except Exception as e:
        state["feed_status"]=f"Fejl: {e}"
        state["feed_connected"]=False
        feed = None

def scan_symbol_live(symbol):
    if not SCANNER_OK or feed is None: return None
    try:
        df = feed.get_ohlcv(symbol, PARAMS["interval"])
        if df is None or len(df)<30: return None
        ih=interval_to_hours(PARAMS["interval"])
        dc=max(1,int(PARAMS["entry_delay_h"]/ih))
        cp=(PARAMS["fee_pct"]+PARAMS["slippage_pct"])/100
        df2=df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
        df2["rsi_v"]=rsi(df2["close"],14)
        df2["atr_v"]=atr(df2,14)
        df2["pump"]=detect_pumps(df2,PARAMS["pump_pct"],PARAMS["pump_window_h"],ih,
                                  min_candles=PARAMS["min_pump_candles"],
                                  volume_filter=PARAMS["volume_filter"])
        for i in range(len(df2)-1,max(len(df2)-8,dc),-1):
            row,prev=df2.iloc[i],df2.iloc[i-dc]
            if not(pd.notna(row["rsi_v"]) and pd.notna(row["atr_v"])): continue
            if not(prev["pump"] and row["rsi_v"]<PARAMS["rsi_max"] and
                   row["close"]<prev["high"]): continue
            can=max(1,int(PARAMS["pump_window_h"]/ih))
            rl=df2["low"].rolling(can).min().iloc[i-dc]
            rh=df2["high"].rolling(can).max().iloc[i-dc]
            ps=(rh-rl)/rl*100 if rl>0 else 0
            ep=row["close"]*(1+cp/2)
            sl=ep*(1+PARAMS["stop_loss_pct"]/100)
            tp=ep-row["atr_v"]*PARAMS["tp_atr"]
            kf=calc_kelly(STATS); ru=PARAMS["capital"]*kf
            now=datetime.now(timezone.utc)
            ts=df2.index[i].isoformat()
            key=f"{symbol}_{ts[:13]}"
            if key not in seen_sigs: seen_sigs[key]=now
            first=seen_sigs[key]
            age_h=round((now-first).total_seconds()/3600,2)
            sig={
                "symbol":symbol.replace("USDT",""),"symbol_full":symbol,
                "entry":round(ep,6),"sl":round(sl,6),"tp":round(tp,6),
                "sl_pct":PARAMS["stop_loss_pct"],"tp_pct":round((ep-tp)/ep*100,1),
                "pump_size":round(ps,1),"rsi":round(row["rsi_v"],1),
                "age_h":age_h,"is_new":age_h<0.25,
                "first_seen":first.strftime("%H:%M"),
                "kelly_pct":round(kf*100,2),"risk_usd":round(ru,0),
                "pos_usd":round(ru/(PARAMS["stop_loss_pct"]/100),0),
                "ts":ts,
                "cur_price":round(feed.last_price(symbol) or ep,6),
            }
            # Tilføj live distance info
            cur=sig["cur_price"]
            sig["cur_pct"]=round((ep-cur)/ep*100,2)
            sig["dist_tp_pct"]=round((cur-tp)/ep*100,2)
            sig["dist_sl_pct"]=round((sl-cur)/ep*100,2)
            sig["bar_pos"]=round(max(2,min(96,(ep-cur)/(ep-sl)*100)),1)
            # Registrer i tracker
            if tracker: tracker.add_signal(sig)
            return sig
    except Exception: return None

def run_scan():
    global state
    if state["scanning"]: return
    state["scanning"]=True; state["error"]=None; sigs=[]
    try:
        syms=feed._symbols if feed else []
        state["n_symbols"]=len(syms)
        for sym in syms:
            sig=scan_symbol_live(sym)
            if sig: sigs.append(sig)
        sigs.sort(key=lambda x:x["pump_size"],reverse=True)
        state["signals"]=sigs
        state["last_scan"]=datetime.now().strftime("%H:%M:%S")
        state["scan_count"]+=1
    except Exception as e:
        state["error"]=str(e)
    state["scanning"]=False

def continuous_scan():
    while True:
        if feed and state["feed_connected"]: run_scan()
        time.sleep(60)

@app.route("/")
def index(): return HTML

@app.route("/manifest.json")
def manifest(): return Response(MANIFEST,mimetype="application/json")

@app.route("/sw.js")
def sw(): return Response(SW_JS,mimetype="application/javascript")

@app.route("/icon/<int:size>")
def icon(size): return Response(_icon(size),mimetype="image/png")

@app.route("/api/signals")
def api_signals():
    tr_stats=tracker.stats() if tracker else {}
    active  =tracker.get_active() if tracker else []
    return jsonify({
        **state,
        "params":PARAMS,"kelly_pct":round(calc_kelly(STATS)*100,2),
        "capital":PARAMS["capital"],
        "tracker_stats":tr_stats,"active_signals_taken":[s for s in (tracker.get_active() if tracker else []) if s.get("taken")],"active_signals_all":tracker.get_active() if tracker else [],
        "active_signals":active,
    })

@app.route("/api/replay")
def api_replay():
    hours = int(request.args.get("hours", 48))
    if not REPLAY_OK or not SCANNER_OK:
        return jsonify({"signals": [], "error": "Scanner ikke klar"})
    syms = feed._symbols if feed else []
    if not syms:
        return jsonify({"signals": [], "error": "Feed ikke forbundet"})
    try:
        sigs = run_replay(syms, PARAMS, hours=hours)
        wins   = sum(1 for s in sigs if s["outcome"]=="TP")
        losses = sum(1 for s in sigs if s["outcome"]=="SL")
        pnl    = sum(s["pnl"] for s in sigs if s["outcome"] in ("TP","SL"))
        return jsonify({
            "signals": sigs,
            "hours":   hours,
            "n":       len(sigs),
            "wins":    wins,
            "losses":  losses,
            "open":    sum(1 for s in sigs if s["outcome"]=="OPEN"),
            "pnl":     round(pnl, 2),
            "wr":      round(wins/(wins+losses)*100, 1) if (wins+losses)>0 else 0,
        })
    except Exception as e:
        return jsonify({"signals": [], "error": str(e)})


@app.route("/api/history")
def api_history():
    n=int(request.args.get("n",20))
    return jsonify({"history":tracker.get_closed(n) if tracker else []})

@app.route("/api/scan",methods=["POST"])
def api_scan():
    threading.Thread(target=run_scan,daemon=True).start()
    return jsonify({"status":"ok"})


@app.route("/api/take/<path:key>", methods=["POST"])
def api_take(key):
    d = request.json or {}
    taken = d.get("taken", True)
    if tracker: tracker.set_taken(key, taken)
    return jsonify({"ok": True, "taken": taken})

@app.route("/api/outcome/<path:key>", methods=["POST"])
def api_outcome(key):
    d = request.json or {}
    outcome = d.get("outcome")  # "WIN", "LOSS" eller None
    if tracker: tracker.set_manual_outcome(key, outcome)
    return jsonify({"ok": True, "outcome": outcome})

@app.route("/api/all")
def api_all():
    n = int(request.args.get("n", 100))
    sigs = tracker.get_all(n) if tracker else []
    return jsonify({"signals": sigs})

@app.route("/api/settings",methods=["POST"])
def api_settings():
    d=request.json or {}
    for k in ["pump_pct","stop_loss_pct","tp_atr","capital","top_n"]:
        if k in d: PARAMS[k]=float(d[k])
    return jsonify({"ok":True})

def _icon(sz):
    raw=b''
    for y in range(sz):
        raw+=b'\x00'
        for x in range(sz):
            d=((x-sz//2)**2+(y-sz//2)**2)**.5
            bg=int(max(0,255-d*2))
            raw+=bytes([0,int(229*.7+bg*.3),int(160*.7)])
    comp=zlib.compress(raw)
    def c(n,d):
        cc=zlib.crc32(n+d)&0xffffffff
        return struct.pack('>I',len(d))+n+d+struct.pack('>I',cc)
    p=b'\x89PNG\r\n\x1a\n'
    p+=c(b'IHDR',struct.pack('>IIBBBBB',sz,sz,8,2,0,0,0))
    p+=c(b'IDAT',comp); p+=c(b'IEND',b'')
    return p

MANIFEST=json.dumps({
    "name":"Pump & Dump Scanner","short_name":"PumpScan",
    "start_url":"/","display":"standalone",
    "background_color":"#070b12","theme_color":"#070b12",
    "icons":[{"src":"/icon/192","sizes":"192x192","type":"image/png"},
             {"src":"/icon/512","sizes":"512x512","type":"image/png"}]
})
SW_JS="const C='ps-v3';self.addEventListener('fetch',e=>{if(e.request.url.includes('/api/'))return;e.respondWith(fetch(e.request).catch(()=>caches.match(e.request)));});"

HTML=r"""<!DOCTYPE html>
<html lang="da"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070b12">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<link rel="manifest" href="/manifest.json">
<title>PumpScan</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#070b12;--bg2:#0c1220;--bg3:#111827;--bdr:#1e2d45;--bdr2:#243144;
--g:#00d68f;--r:#ff3b5c;--y:#f7b731;--b:#3b82f6;
--g-dim:rgba(0,214,143,.1);--r-dim:rgba(255,59,92,.1);--b-dim:rgba(59,130,246,.1);
--txt:#e2e8f4;--txt2:#94a3b8;--txt3:#4a5568;
--mono:'JetBrains Mono',monospace;--sans:'Inter',sans-serif}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100dvh;overflow-x:hidden}
/* Header */
.hdr{position:sticky;top:0;z-index:100;background:rgba(7,11,18,.94);backdrop-filter:blur(24px);
border-bottom:1px solid var(--bdr);padding:13px 16px;display:flex;align-items:center;gap:12px}
.logo{width:34px;height:34px;border-radius:10px;background:linear-gradient(145deg,#00d68f,#0090ff);
display:grid;place-items:center;font-size:18px;flex-shrink:0;box-shadow:0 0 16px rgba(0,214,143,.25)}
.htitle{font-size:18px;font-weight:800;letter-spacing:-.5px}
.htitle b{color:var(--g)}
.hdr-r{margin-left:auto;display:flex;align-items:center;gap:8px}
.live-pill{display:flex;align-items:center;gap:4px;padding:4px 10px;border-radius:20px;
font-size:10px;font-weight:700;letter-spacing:.06em;transition:all .3s;
background:var(--g-dim);color:var(--g);border:1px solid rgba(0,214,143,.2)}
.live-pill.off{background:rgba(74,85,104,.1);color:var(--txt3);border-color:var(--bdr)}
.ldot{width:6px;height:6px;border-radius:50%;background:currentColor;animation:pulse 1.6s ease-in-out infinite}
.live-pill.off .ldot{animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.sbtn{background:linear-gradient(135deg,var(--g),#00b8ff);color:#000;border:none;
font-weight:800;font-size:12px;padding:8px 14px;border-radius:10px;cursor:pointer;
transition:opacity .15s,transform .1s;box-shadow:0 4px 14px rgba(0,214,143,.2)}
.sbtn:active{transform:scale(.94)}.sbtn.busy{opacity:.5;pointer-events:none}
/* Feed bar */
.fbar{padding:6px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);
font-size:11px;color:var(--g);font-weight:600;display:flex;align-items:center;gap:6px}
.fbar span{color:var(--txt3);font-weight:400}
/* Stats */
.stats{display:flex;overflow-x:auto;scrollbar-width:none;background:var(--bg2);
border-bottom:1px solid var(--bdr);padding:0 16px}
.stats::-webkit-scrollbar{display:none}
.st{flex-shrink:0;padding:11px 16px 11px 0;margin-right:16px;border-right:1px solid var(--bdr)}
.st:last-child{border-right:none}
.stl{font-size:9px;color:var(--txt3);text-transform:uppercase;letter-spacing:.08em;margin-bottom:3px}
.stv{font-family:var(--mono);font-size:14px;font-weight:700}
.stv small{font-size:9px;color:var(--txt3);font-weight:400;margin-left:2px}
/* Tabs */
.tabs{display:flex;background:var(--bg2);border-bottom:1px solid var(--bdr)}
.tab{flex:1;padding:11px 0;font-size:11px;font-weight:700;text-align:center;
color:var(--txt3);border:none;background:none;cursor:pointer;
border-bottom:2px solid transparent;text-transform:uppercase;letter-spacing:.07em;transition:color .15s}
.tab.on{color:var(--g);border-bottom-color:var(--g)}
/* Panels */
.panel{display:none;padding:14px;max-width:620px;margin:0 auto}
.panel.on{display:block}
.sec{font-size:9px;font-weight:700;color:var(--txt3);text-transform:uppercase;
letter-spacing:.12em;display:flex;align-items:center;gap:8px;margin-bottom:12px;margin-top:4px}
.sec::after{content:'';flex:1;height:1px;background:var(--bdr)}
/* Win rate ring card */
.wr-card{background:var(--bg2);border:1px solid var(--bdr2);border-radius:16px;
padding:16px;display:flex;align-items:center;gap:18px;margin-bottom:14px}
.wr-ring{flex-shrink:0}
.wr-info{flex:1}
.wr-num{font-family:var(--mono);font-size:30px;font-weight:900;color:var(--g);line-height:1}
.wr-lbl{font-size:10px;color:var(--txt3);font-weight:700;text-transform:uppercase;letter-spacing:.1em;margin-top:2px}
.wr-sub{font-size:11px;color:var(--txt2);margin-top:3px}
.wr-row{display:flex;gap:16px;margin-top:10px;padding-top:10px;border-top:1px solid var(--bdr)}
.wr-item .wv{font-family:var(--mono);font-size:13px;font-weight:700}
.wr-item .wl{font-size:9px;color:var(--txt3);margin-top:1px}
/* Active signal card */
.sig-card{background:var(--bg2);border:1px solid var(--bdr2);border-radius:16px;
margin-bottom:12px;overflow:hidden;transition:border-color .2s}
.sig-card.new{border-color:rgba(0,214,143,.4);box-shadow:0 0 0 1px rgba(0,214,143,.12)}
.sig-card.watching{border-color:rgba(59,130,246,.3)}
.sig-top{display:flex;align-items:flex-start;padding:14px 16px 12px;gap:12px;border-bottom:1px solid var(--bdr)}
.coin-n{font-size:22px;font-weight:900;letter-spacing:-.7px;line-height:1}
.tags{display:flex;align-items:center;gap:5px;margin-top:5px}
.tag{font-size:9px;font-weight:700;padding:2px 7px;border-radius:4px;letter-spacing:.06em}
.t-short{background:var(--r-dim);color:var(--r)}
.t-new{background:var(--g-dim);color:var(--g)}
.t-watch{background:var(--b-dim);color:var(--b)}
.pump-r{margin-left:auto;text-align:right}
.pump-n{font-size:26px;font-weight:900;letter-spacing:-1px;line-height:1;
background:linear-gradient(135deg,var(--y),var(--r));-webkit-background-clip:text;
-webkit-text-fill-color:transparent;background-clip:text}
.pump-n.big{background:linear-gradient(135deg,var(--r),#ff007a);
-webkit-background-clip:text;background-clip:text}
.pump-l{font-size:9px;color:var(--txt3);font-weight:600;letter-spacing:.06em;margin-top:1px}
.rsi-c{display:inline-block;margin-top:4px;font-family:var(--mono);font-size:10px;font-weight:700;
padding:2px 7px;border-radius:5px;background:var(--r-dim);color:var(--r);border:1px solid rgba(255,59,92,.2)}
/* Levels */
.levels{margin:0 12px 12px;border:1px solid var(--bdr);border-radius:11px;overflow:hidden;background:var(--bg3)}
.lv{display:flex;align-items:center;padding:10px 14px;border-bottom:1px solid var(--bdr);gap:10px}
.lv:last-child{border-bottom:none}
.lv-ico{width:30px;height:30px;border-radius:8px;display:grid;place-items:center;font-size:14px;flex-shrink:0}
.lvi-e{background:var(--b-dim)}.lvi-sl{background:var(--r-dim)}.lvi-tp{background:var(--g-dim)}
.lv-inf{flex:1}
.lv-nm{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--txt3);margin-bottom:1px}
.lv-pct{font-size:10px;color:var(--txt3)}
.lv-pct.pr{color:var(--r)}.lv-pct.pg{color:var(--g)}
.lv-p{font-family:var(--mono);font-size:16px;font-weight:700;letter-spacing:-.3px;text-align:right}
.lp-e{color:var(--txt)}.lp-sl{color:var(--r)}.lp-tp{color:var(--g)}
/* Price bar */
.pbar{margin:0 12px 12px;background:var(--bg3);border:1px solid var(--bdr);border-radius:10px;padding:10px 14px}
.pbar-lbl{display:flex;justify-content:space-between;font-size:9px;font-family:var(--mono);margin-bottom:8px}
.pbar-track{position:relative;height:5px;border-radius:3px;background:linear-gradient(to right,var(--g-dim),var(--r-dim))}
.pb-tp{position:absolute;left:0;top:-3px;width:11px;height:11px;border-radius:50%;background:var(--g);border:2px solid var(--bg2)}
.pb-entry{position:absolute;top:-4px;width:13px;height:13px;border-radius:50%;background:var(--b);
border:2px solid var(--bg2);transform:translateX(-50%);box-shadow:0 0 8px rgba(59,130,246,.5)}
.pb-cur{position:absolute;top:-5px;width:15px;height:15px;border-radius:50%;background:var(--y);
border:2px solid var(--bg2);transform:translateX(-50%);
animation:curPulse 1.5s ease-in-out infinite}
@keyframes curPulse{0%,100%{box-shadow:0 0 0 2px rgba(247,183,49,.3)}50%{box-shadow:0 0 0 5px rgba(247,183,49,.0)}}
.pb-sl{position:absolute;right:0;top:-3px;width:11px;height:11px;border-radius:50%;background:var(--r);border:2px solid var(--bg2)}
.pbar-dist{display:flex;justify-content:space-between;margin-top:7px;font-size:9px}
/* Card footer */
.cfoot{display:flex;align-items:center;gap:10px;padding:11px 16px;background:rgba(0,0,0,.22);border-top:1px solid var(--bdr)}
.kamt{font-family:var(--mono);font-size:16px;font-weight:700;color:var(--g)}
.klbl{font-size:9px;color:var(--txt3);font-weight:600;text-transform:uppercase;letter-spacing:.07em;margin-bottom:1px}
.krisk{font-size:10px;color:var(--txt2);font-family:var(--mono)}
.tw{margin-left:auto;text-align:right}
.tseen{font-size:11px;font-weight:600;color:var(--txt2);font-family:var(--mono)}
.tago{font-size:9px;color:var(--txt3);margin-top:1px}
/* History */
.hist-row{display:flex;align-items:center;padding:11px 16px;border-bottom:1px solid var(--bdr);gap:12px}
.hist-row:last-child{border-bottom:none}
.hcoin{font-size:15px;font-weight:900;color:var(--txt);width:42px;flex-shrink:0}
.hinfo{flex:1}
.hprice{font-size:10px;color:var(--txt3);font-family:var(--mono)}
.hdur{font-size:9px;color:var(--txt3);margin-top:2px}
.hpnl{font-family:var(--mono);font-size:14px;font-weight:700;text-align:right}
.hbadge{font-size:9px;font-weight:700;padding:2px 7px;border-radius:4px;display:block;
margin-top:3px;text-align:right}
.bwin{background:var(--g-dim);color:var(--g)}.bloss{background:var(--r-dim);color:var(--r)}
.bpend{background:var(--b-dim);color:var(--b)}
/* Settings */
.sg{margin-bottom:20px}
.sgt{font-size:9px;font-weight:700;color:var(--txt3);text-transform:uppercase;letter-spacing:.12em;
margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--bdr)}
.si{background:var(--bg2);border:1px solid var(--bdr2);border-radius:12px;
padding:13px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px}
.sil{font-size:14px;font-weight:600;flex:1}.sis{font-size:11px;color:var(--txt3);margin-top:2px}
.inp{background:var(--bg3);border:1px solid var(--bdr2);color:var(--txt);
font-family:var(--mono);font-size:14px;padding:7px 11px;border-radius:8px;width:96px;
text-align:right;font-weight:600}
.inp:focus{outline:none;border-color:var(--g)}
.gbtn{width:100%;padding:15px;background:linear-gradient(135deg,var(--g),#00b8ff);
color:#000;border:none;border-radius:12px;font-family:var(--sans);font-weight:800;
font-size:15px;cursor:pointer;margin-top:8px;box-shadow:0 4px 20px rgba(0,214,143,.25)}
.gbtn:active{opacity:.85}
/* Empty */
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;
padding:60px 24px;text-align:center;gap:12px}
.eico{font-size:52px;opacity:.2}.etit{font-size:18px;font-weight:800;color:var(--txt2)}
.esub{font-size:13px;color:var(--txt3);line-height:1.7;max-width:240px}
.loader{display:flex;flex-direction:column;align-items:center;gap:14px;padding:56px 24px}
.spin{width:38px;height:38px;border:3px solid var(--bdr);border-top-color:var(--g);
border-radius:50%;animation:rot .7s linear infinite}
@keyframes rot{to{transform:rotate(360deg)}}
.stxt{font-size:12px;color:var(--txt3);font-family:var(--mono);text-align:center;line-height:1.6}

.take-row{display:flex;align-items:center;gap:10px;padding:11px 16px;border-bottom:1px solid var(--bdr);background:rgba(0,0,0,.15)}
.take-lbl{font-size:12px;font-weight:700;color:var(--txt2);flex:1}
.take-toggle{display:flex;gap:6px}
.take-btn{font-size:11px;font-weight:700;padding:5px 13px;border-radius:8px;border:1px solid;cursor:pointer;font-family:var(--sans);transition:all .15s}
.take-btn.yes{background:var(--g-dim);color:var(--g);border-color:rgba(0,214,143,.3)}
.take-btn.yes.active{background:var(--g);color:#000;border-color:var(--g)}
.take-btn.no{background:rgba(74,85,104,.1);color:var(--txt3);border-color:var(--bdr2)}
.take-btn.no.active{background:rgba(74,85,104,.25);color:var(--txt2)}
.outcome-row{display:flex;align-items:center;gap:7px;padding:9px 16px;background:rgba(0,0,0,.12);flex-wrap:wrap}
.outcome-lbl{font-size:11px;color:var(--txt3);flex:1;min-width:100px}
.out-btn{font-size:10px;font-weight:700;padding:4px 11px;border-radius:6px;border:1px solid;cursor:pointer;font-family:var(--sans);transition:all .15s}
.out-win{background:var(--g-dim);color:var(--g);border-color:rgba(0,214,143,.3)}
.out-win.active{background:var(--g);color:#000}
.out-loss{background:var(--r-dim);color:var(--r);border-color:rgba(255,59,92,.3)}
.out-loss.active{background:var(--r);color:#fff}
.out-clear{background:rgba(74,85,104,.15);color:var(--txt3);border-color:var(--bdr2)}
.card.taken{border-color:rgba(0,214,143,.35)}
.card.not-taken{opacity:.72}
.t-win{background:var(--g-dim);color:var(--g)}
.t-loss{background:var(--r-dim);color:var(--r)}
.t-open{background:var(--b-dim);color:var(--b)}

/* Nav */
.bnav{position:fixed;bottom:0;left:0;right:0;background:rgba(7,11,18,.97);
backdrop-filter:blur(20px);border-top:1px solid var(--bdr);display:flex;
padding-bottom:env(safe-area-inset-bottom)}
.ni{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;padding:10px 0;
cursor:pointer;border:none;background:none;color:var(--txt3);font-family:var(--sans);
font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;transition:color .15s}
.ni.on{color:var(--g)}.ni-ico{font-size:21px}
.toast{position:fixed;bottom:72px;left:50%;transform:translateX(-50%) translateY(16px);
background:var(--bg3);border:1px solid var(--bdr2);color:var(--txt);padding:10px 22px;
border-radius:100px;font-size:13px;font-weight:700;opacity:0;transition:all .2s;
pointer-events:none;white-space:nowrap;z-index:200}
.toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
.spacer{height:82px}
</style>
</head>
<body>

<div class="hdr">
  <div class="logo">📈</div>
  <div class="htitle">Pump<b>Scan</b></div>
  <div class="hdr-r">
    <div class="live-pill off" id="lPill"><div class="ldot"></div><span id="lLbl">OFFLINE</span></div>
    <button class="sbtn" id="sbtn" onclick="doScan()">↻ Scan</button>
  </div>
</div>

<div class="fbar" id="fbar"><span>Starter Binance feed...</span></div>

<div class="stats">
  <div class="st"><div class="stl">Faktisk WR</div><div class="stv" id="sWR">—</div></div>
  <div class="st"><div class="stl">Lukkede</div><div class="stv" id="sClosed">—</div></div>
  <div class="st"><div class="stl">Net P&L</div><div class="stv" id="sPnl">—</div></div>
  <div class="st"><div class="stl">Aktive</div><div class="stv" id="sActive">—</div></div>
  <div class="st"><div class="stl">Sidst scan</div><div class="stv" id="sLast">—</div></div>
</div>

<div class="tabs">
  <button class="tab on" id="tSig" onclick="tab('Sig')">📡 Signaler</button>
  <button class="tab" id="tHis" onclick="tab('His')">📊 Historik</button>
  <button class="tab" id="tRep" onclick="tab('Rep');loadReplay()">🔄 Replay</button>
  <button class="tab" id="tSet" onclick="tab('Set')">⚙️ Indstillinger</button>
</div>

<!-- Signaler -->
<div id="pSig" class="panel on">
  <div style="padding:14px 14px 0">
    <div id="wrCard"></div>
    <div class="sec" id="sigSec" style="display:none">Under observation</div>
    <div id="sigList"></div>
  </div>
  <div class="spacer"></div>
</div>

<!-- Replay -->
<div id="pRep" class="panel">
  <div style="display:flex;align-items:center;gap:10px;padding:14px 14px 0">
    <select id="replayHours" style="background:var(--bg2);border:1px solid var(--bdr2);
      color:var(--txt);border-radius:8px;padding:6px 10px;font-size:13px">
      <option value="24">Seneste 24 timer</option>
      <option value="48" selected>Seneste 48 timer</option>
      <option value="72">Seneste 72 timer</option>
    </select>
    <button onclick="loadReplay()" style="background:var(--g);color:#000;border:none;
      font-weight:700;font-size:12px;padding:8px 14px;border-radius:8px;cursor:pointer">
      ↻ Scan
    </button>
    <span id="replayStatus" style="font-size:12px;color:var(--txt3)"></span>
  </div>
  <div id="replayList" style="padding:14px"></div>
  <div class="spacer"></div>
</div>

<!-- Historik -->
<div id="pHis" class="panel">
  <div id="histList"></div>
  <div class="spacer"></div>
</div>

<!-- Indstillinger -->
<div id="pSet" class="panel">
  <div class="sg">
    <div class="sgt">Strategi</div>
    <div class="si"><div><div class="sil">Pump minimum</div><div class="sis">Mindste stigning % (24t)</div></div>
      <input class="inp" id="sPump" type="number" value="20" min="5" max="200" step="5"></div>
    <div class="si"><div><div class="sil">Stop-Loss</div><div class="sis">% over entry</div></div>
      <input class="inp" id="sSL" type="number" value="8" min="2" max="30" step="0.5"></div>
    <div class="si"><div><div class="sil">TP multiplier</div><div class="sis">N × ATR</div></div>
      <input class="inp" id="sTP" type="number" value="1.5" min="0.5" max="5" step="0.1"></div>
    <div class="si"><div><div class="sil">Antal coins</div><div class="sis">Top N efter volumen</div></div>
      <input class="inp" id="sTop" type="number" value="40" min="10" max="100" step="10"></div>
  </div>
  <div class="sg">
    <div class="sgt">Kapital</div>
    <div class="si"><div><div class="sil">Kapital (USD)</div><div class="sis">Til Kelly beregning</div></div>
      <input class="inp" id="sCap" type="number" value="10000" min="100" step="500"></div>
  </div>
  <button class="gbtn" onclick="save()">💾  Gem indstillinger</button>
  <div class="spacer"></div>
</div>

<div class="bnav">
  <button class="ni on" id="nSig" onclick="tab('Sig')"><span class="ni-ico">📡</span>Signaler</button>
  <button class="ni" id="nHis" onclick="tab('His')"><span class="ni-ico">📊</span>Historik</button>
  <button class="ni" id="nRep" onclick="tab('Rep');loadReplay()"><span class="ni-ico">🔄</span>Replay</button>
  <button class="ni" id="nSet" onclick="tab('Set')"><span class="ni-ico">⚙️</span>Indstillinger</button>
</div>
<div class="toast" id="toast"></div>

<script>
let data=null,hist=null;

async function loadReplay(){
  const hours = document.getElementById('replayHours').value;
  const status = document.getElementById('replayStatus');
  const list   = document.getElementById('replayList');
  status.textContent = 'Scanner...';
  list.innerHTML = '<div class="loader"><div class="spin"></div><div class="stxt">Scanner '+(hours)+'t historik...</div></div>';
  try{
    const r  = await fetch('/api/replay?hours='+hours);
    const d  = await r.json();
    if(d.error){ status.textContent=d.error; list.innerHTML=''; return; }
    const sigs = d.signals||[];
    status.textContent = `${d.n} signaler · ${d.wr}% WR · P&L $${d.pnl>=0?'+':''}${d.pnl}`;
    if(!sigs.length){
      list.innerHTML='<div class="empty"><div class="eico">📭</div><div class="etit">Ingen signaler</div>'+
        '<div class="esub">Ingen A-grade signaler fundet<br>i de seneste '+hours+' timer</div></div>';
      return;
    }
    // Stats banner
    const winCol = d.wr>=60?'var(--g)':d.wr>=50?'var(--y)':'var(--r)';
    let html = `<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px">
      ${[['Signaler',d.n,''],['Win rate',d.wr+'%',winCol],
         ['P&L',`$${d.pnl>=0?'+':''}${d.pnl}`,d.pnl>=0?'var(--g)':'var(--r)'],
         ['Åbne',d.open,'var(--b)']].map(([l,v,c])=>`
      <div style="background:var(--bg2);border:1px solid var(--bdr2);border-radius:12px;padding:10px 12px">
        <div style="font-size:10px;color:var(--txt3);margin-bottom:3px;text-transform:uppercase;letter-spacing:.07em">${l}</div>
        <div style="font-family:var(--mono);font-size:16px;font-weight:700;color:${c||'var(--txt)'}">${v}</div>
      </div>`).join('')}
    </div>`;
    html += sigs.map(s=>{
      const win  = s.outcome==='TP';
      const open = s.outcome==='OPEN';
      const col  = win?'var(--g)':open?'var(--y)':'var(--r)';
      const ico  = win?'✓':open?'⏳':'✗';
      const pnlStr = s.pnl>=0?'+$'+s.pnl:'$'+s.pnl;
      return`<div style="background:var(--bg2);border:1px solid ${win?'rgba(0,214,143,.3)':open?'rgba(59,130,246,.2)':'var(--bdr2)'};
        border-radius:14px;margin-bottom:10px;overflow:hidden">
        <div style="display:flex;align-items:center;gap:10px;padding:12px 14px;border-bottom:1px solid var(--bdr)">
          <div style="width:32px;height:32px;border-radius:8px;background:${win?'var(--g-dim)':open?'var(--b-dim)':'var(--r-dim)'};
            display:grid;place-items:center;font-size:16px;flex-shrink:0">${ico}</div>
          <div>
            <div style="font-size:17px;font-weight:900;letter-spacing:-.5px">${s.symbol}</div>
            <div style="font-size:10px;color:var(--txt3);margin-top:2px">${s.ts_str} · pump +${s.pump_size}% · RSI ${s.rsi}</div>
          </div>
          <div style="margin-left:auto;text-align:right">
            <div style="font-family:var(--mono);font-size:15px;font-weight:700;color:${col}">${open?'Åben':pnlStr}</div>
            <div style="font-size:10px;color:var(--txt3)">${s.outcome}${s.exit_time?' · '+s.exit_time:''}</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;font-size:11px">
          <div style="padding:8px 12px;border-right:1px solid var(--bdr)">
            <div style="color:var(--txt3);margin-bottom:2px">Entry</div>
            <div style="font-family:var(--mono);font-weight:700">$${s.entry}</div>
          </div>
          <div style="padding:8px 12px;border-right:1px solid var(--bdr)">
            <div style="color:var(--r);margin-bottom:2px">SL +${s.sl_pct}%</div>
            <div style="font-family:var(--mono);font-weight:700;color:var(--r)">$${s.sl}</div>
          </div>
          <div style="padding:8px 12px">
            <div style="color:var(--g);margin-bottom:2px">TP ${s.tp_pct}%</div>
            <div style="font-family:var(--mono);font-weight:700;color:var(--g)">$${s.tp}</div>
          </div>
        </div>
        <div style="padding:8px 14px;background:rgba(0,0,0,.15);font-size:11px;
          display:flex;justify-content:space-between;color:var(--txt3)">
          <span>Varighed: ${s.duration}</span>
          <span>Kelly pos: $${Number(s.pos_usd).toLocaleString()}</span>
          <span style="color:var(--txt3)">Ikke taget</span>
        </div>
      </div>`;
    }).join('');
    list.innerHTML=html;
  }catch(e){ status.textContent='Fejl: '+e; list.innerHTML=''; }
}

function tab(t){
  ['Sig','His','Rep','Set'].forEach(x=>{
    document.getElementById('p'+x).classList.toggle('on',x===t);
    document.getElementById('t'+x).classList.toggle('on',x===t);
    document.getElementById('n'+x).classList.toggle('on',x===t);
  });
  if(t==='His') loadHist();
}

function toast(m,d=2500){
  const e=document.getElementById('toast');
  e.textContent=m;e.classList.add('on');
  setTimeout(()=>e.classList.remove('on'),d);
}

async function doScan(){
  await fetch('/api/scan',{method:'POST'});
  toast('⟳ Scanner...');
  document.getElementById('sbtn').classList.add('busy');
  setTimeout(load,2000);
}

async function save(){
  await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      pump_pct:+document.getElementById('sPump').value,
      stop_loss_pct:+document.getElementById('sSL').value,
      tp_atr:+document.getElementById('sTP').value,
      capital:+document.getElementById('sCap').value,
      top_n:+document.getElementById('sTop').value,
    })});
  toast('✓ Gemt');tab('Sig');doScan();
}

async function setTaken(key, taken){
  await fetch('/api/take/'+key,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({taken})});
  setTimeout(load,400);
}
async function setOutcome(key, outcome){
  await fetch('/api/outcome/'+key,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({outcome})});
  setTimeout(load,400);
}
async function load(){
  try{const r=await fetch('/api/signals');data=await r.json();render();}catch(e){}
}

async function loadHist(){
  try{const r=await fetch('/api/history?n=50');hist=await r.json();renderHist();}catch(e){}
}

function fAge(h){
  if(h<1/60)return'Lige nu';
  if(h<1)return Math.round(h*60)+' min siden';
  return h.toFixed(1)+'t siden';
}

function wrRing(wr){
  const r=34,c=2*Math.PI*r;
  const arc=wr/100*c;
  const col=wr>=60?'var(--g)':wr>=45?'var(--y)':'var(--r)';
  return`<svg width="90" height="90" viewBox="0 0 90 90" class="wr-ring">
    <circle cx="45" cy="45" r="${r}" fill="none" stroke="var(--bdr)" stroke-width="9"/>
    <circle cx="45" cy="45" r="${r}" fill="none" stroke="${col}" stroke-width="9"
      stroke-dasharray="${arc} ${c}" stroke-dashoffset="${c/4}" stroke-linecap="round"
      transform="rotate(-90 45 45)"/>
    <text x="45" y="50" text-anchor="middle" font-family="JetBrains Mono"
      font-size="15" font-weight="700" fill="${col}">${wr}%</text>
  </svg>`;
}

function renderWrCard(ts){
  if(!ts||ts.n===0){
    document.getElementById('wrCard').innerHTML=
      `<div class="wr-card"><div style="color:var(--txt3);font-size:13px;text-align:center;width:100%">
      Ingen lukkede signaler endnu.<br>Win rate beregnes automatisk når TP/SL rammes.</div></div>`;
    return;
  }
  const wr=ts.wr||0;
  const pnlCol=ts.net_pnl>=0?'var(--g)':'var(--r)';
  document.getElementById('wrCard').innerHTML=`
  <div class="wr-card">
    ${wrRing(wr)}
    <div class="wr-info">
      <div class="wr-num">${wr}%</div>
      <div class="wr-lbl">Faktisk win rate</div>
      <div class="wr-sub">${ts.n} lukkede signaler</div>
      <div class="wr-row">
        <div class="wr-item">
          <div class="wv" style="color:${pnlCol}">${ts.net_pnl>=0?'+':''}$${ts.net_pnl}</div>
          <div class="wl">Net P&L</div>
        </div>
        <div class="wr-item">
          <div class="wv" style="color:var(--y)">${ts.pf}x</div>
          <div class="wl">Profit faktor</div>
        </div>
        <div class="wr-item">
          <div class="wv" style="color:var(--g)">${ts.wins}W</div>
          <div class="wl">${ts.losses}L</div>
        </div>
      </div>
    </div>
  </div>`;
}

function renderSigCard(s){
  // Sæt defaults for felter der kan mangle fra tracker
  s.age_h      = s.age_h      ?? 0;
  s.is_new     = s.is_new     ?? false;
  s.first_seen = s.first_seen || '—';
  s.pump_size  = s.pump_size  || 0;
  s.rsi        = s.rsi        || 0;
  s.sl_pct     = s.sl_pct     || 8;
  s.tp_pct     = s.tp_pct     || 0;
  s.kelly_pct  = s.kelly_pct  || 0;
  s.risk_usd   = s.risk_usd   || 0;
  s.pos_usd    = s.pos_usd    || 0;
  s.cur_price  = s.cur_price  || s.entry;
  s.dist_tp_pct= s.dist_tp_pct ?? null;
  s.dist_sl_pct= s.dist_sl_pct ?? null;
  const pc=s.pump_size>60?'big':'';
  const nb=s.is_new?'<span class="tag t-new">● NY</span>':'';
  const wb=s.dist_tp_pct!=null?'<span class="tag t-watch">● Live</span>':'';
  const cur=s.cur_price||s.entry;
  const entry=s.entry,tp=s.tp,sl=s.sl;
  const range=sl-tp;
  const ePos=range>0?Math.max(3,Math.min(95,(sl-entry)/range*100)):60;
  const cPos=s.cur_pos??( range>0?Math.max(2,Math.min(97,(sl-cur)/range*100)):ePos );
  const distTP=s.dist_tp_pct!=null?s.dist_tp_pct.toFixed(1):'—';
  const distSL=s.dist_sl_pct!=null?s.dist_sl_pct.toFixed(1):'—';
  const dtpCol=parseFloat(distTP)<2?'color:var(--g);font-weight:700':'color:var(--txt3)';

  return`<div class="sig-card${s.is_new?' new':' watching'}">
<div class="sig-top">
  <div>
    <div class="coin-n">${s.symbol}</div>
    <div class="tags"><span class="tag t-short">SHORT ▼</span>${nb}${outcomeTag}</div>
  </div>
  <div class="pump-r">
    <div class="pump-n ${pc}">+${s.pump_size}%</div>
    <div class="pump-l">PUMP</div>
    <div class="rsi-c">RSI ${s.rsi}</div>
  </div>
</div>
<div class="levels">
  <div class="lv"><div class="lv-ico lvi-e">🎯</div>
    <div class="lv-inf"><div class="lv-nm">Entry</div><div class="lv-pct">Short her</div></div>
    <div class="lv-p lp-e">$${s.entry}</div></div>
  <div class="lv"><div class="lv-ico lvi-sl">🛑</div>
    <div class="lv-inf"><div class="lv-nm">Stop-Loss</div><div class="lv-pct pr">+${s.sl_pct}% over entry</div></div>
    <div class="lv-p lp-sl">$${s.sl}</div></div>
  <div class="lv"><div class="lv-ico lvi-tp">✅</div>
    <div class="lv-inf"><div class="lv-nm">Take-Profit</div><div class="lv-pct pg">${s.tp_pct}% under entry</div></div>
    <div class="lv-p lp-tp">$${s.tp}</div></div>
</div>
<div class="pbar">
  <div class="pbar-lbl">
    <span style="color:var(--g)">TP $${s.tp}</span>
    <span style="color:var(--b)">Entry $${s.entry}</span>
    <span style="color:var(--r)">SL $${s.sl}</span>
  </div>
  <div class="pbar-track">
    <div class="pb-tp"></div>
    <div class="pb-entry" style="left:${ePos}%"></div>
    ${s.cur_price?`<div class="pb-cur" style="left:${cPos}%"></div>`:''}
    <div class="pb-sl"></div>
  </div>
  <div class="pbar-dist">
    <span style="${dtpCol}">▸ TP: ${distTP}%</span>
    <span style="color:var(--txt3);font-family:var(--mono);font-size:9px">${s.cur_price?'Nu $'+s.cur_price:''}</span>
    <span style="color:var(--txt3)">SL: ${distSL}% ◂</span>
  </div>
</div>
<div class="cfoot">
  <div><div class="klbl">Kelly position</div>
    <div class="kamt">$${Number(s.pos_usd).toLocaleString()}</div>
    <div class="krisk">risiker $${s.risk_usd} · ${s.kelly_pct}%</div></div>
  <div class="tw">
    <div class="tseen">set ${s.first_seen}</div>
    <div class="tago">${fAge(s.age_h)}</div>
  </div>
</div></div>`;
}

function render(){
  if(!data)return;
  const live=data.feed_connected;
  const pill=document.getElementById('lPill');
  pill.className='live-pill'+(live?'':' off');
  document.getElementById('lLbl').textContent=live?'LIVE':'OFFLINE';
  document.getElementById('fbar').innerHTML=
    `<span style="color:${live?'var(--g)':'var(--txt3)'}">
    ${data.feed_status||'—'}</span>`;

  const ts=data.tracker_stats||{};
  const wr=ts.wr||0;
  const pnl=ts.net_pnl||0;
    document.getElementById('sWR').innerHTML=
    ts.n?`<span style="color:${ts.wr>=50?'var(--g)':'var(--r)'}">${ts.wr}%</span>`:'—%';
  document.getElementById('sClosed').innerHTML=
    `${ts.n||0}<small style="color:var(--txt3);font-size:9px;margin-left:2px">tagne</small>`;
  document.getElementById('sPnl').innerHTML=
    ts.net_pnl!=null?`<span style="color:${ts.net_pnl>=0?'var(--g)':'var(--r)'}">${ts.net_pnl>=0?'+':''}$${ts.net_pnl}</span>`:'—';
  document.getElementById('sActive').innerHTML=
    `<span style="color:var(--b)">${ts.taken_open||0}</span>/<span style="color:var(--txt3)">${ts.active||0}</span>`;
  document.getElementById('sLast').textContent=data.last_scan||'—';

  const b=document.getElementById('sbtn');
  data.scanning?b.classList.add('busy'):b.classList.remove('busy');
  b.textContent=data.scanning?'⟳':'↻ Scan';

  if(data.params){
    document.getElementById('sPump').value=data.params.pump_pct;
    document.getElementById('sSL').value=data.params.stop_loss_pct;
    document.getElementById('sTP').value=data.params.tp_atr;
    document.getElementById('sCap').value=data.capital;
    document.getElementById('sTop').value=data.params.top_n||40;
  }

  renderWrCard(ts);

  // Active signals (from tracker + new scanner signals)
  const active=data.active_signals_all||data.active_signals||[];
  const scanner=data.signals||[];
  // Merge: prefer tracker version (has live price), fallback to scanner
  const tracked=new Set(active.map(s=>s.symbol_full+'_'+s.ts?.slice(0,13)));
  const allSigs=[...active,...scanner.filter(s=>!tracked.has(s.symbol_full+'_'+s.ts?.slice(0,13)))];
  allSigs.sort((a,b)=>(b.pump_size||0)-(a.pump_size||0));

  const sec=document.getElementById('sigSec');
  const list=document.getElementById('sigList');

  if(!live){
    sec.style.display='none';
    list.innerHTML='<div class="loader"><div class="spin"></div><div class="stxt">'+
      (data.feed_status||'Forbinder...')+'</div></div>';
    return;
  }
  if(!allSigs.length){
    sec.style.display='none';
    list.innerHTML='<div class="empty"><div class="eico">🔍</div>'+
      '<div class="etit">Ingen signaler</div>'+
      '<div class="esub">Ingen coins pumpet ≥'+(data.params?.pump_pct||20)+
      '% de seneste 24t.<br><br><span style="color:var(--g)">●</span> Binance feed er live</div></div>';
    return;
  }
  sec.style.display='flex';
  list.innerHTML=allSigs.map(renderSigCard).join('');
}

function renderHist(){
  if(!hist)return;
  const rows=hist.history||[];
  const list=document.getElementById('histList');
  if(!rows.length){
    list.innerHTML='<div class="empty"><div class="eico">📊</div>'+
      '<div class="etit">Ingen historik endnu</div>'+
      '<div class="esub">Lukkede signaler vises her<br>automatisk når TP eller SL rammes</div></div>';
    return;
  }
  list.innerHTML='<div style="background:var(--bg2);border:1px solid var(--bdr2);'+
    'border-radius:16px;overflow:hidden;margin:14px">'+
    rows.map(h=>{
      const win=h.outcome==='WIN';
      const pnl=h.realized_pnl||0;
      const pnlCol=win?'var(--g)':'var(--r)';
      const date=h.closed_at?new Date(h.closed_at).toLocaleString('da-DK',{
        day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'—';
      return`<div class="hist-row">
        <div class="hcoin">${h.symbol||'—'}</div>
        <div class="hinfo">
          <div class="hprice">${h.entry}→${h.exit_price||'—'}</div>
          <div class="hdur">${h.duration||'—'} · ${date}</div>
        </div>
        <div>
          <div class="hpnl" style="color:${pnlCol}">${pnl>=0?'+':''}$${pnl}</div>
          <span class="hbadge ${win?'bwin':'bloss'}">${win?'✓ WIN':'✗ LOSS'}</span>
        </div>
      </div>`;
    }).join('')+'</div>';
}

if('serviceWorker' in navigator)
  navigator.serviceWorker.register('/sw.js').catch(()=>{});

load();
setInterval(load,10000);
</script>
</body></html>"""

# ── Auto-start ved import (gunicorn + lokal) ──
def startup():
    time.sleep(2)
    try:
        if not SCANNER_OK:
            state["feed_status"]="utils.py ikke fundet"; return
        print("  Henter top coins fra Binance...")
        top=fetch_top_altcoins()
        symbols=[t["symbol"] for t in top[:int(PARAMS["top_n"])]]
        state["n_symbols"]=len(symbols)
        print(f"  {len(symbols)} symboler fundet")
        init_feed(symbols)
        threading.Thread(target=continuous_scan,daemon=True).start()
        print("  PumpScan kørende ✓")
    except Exception as e:
        state["feed_status"]=f"Fejl: {e}"
        print(f"  Startup fejl: {e}")

threading.Thread(target=startup,daemon=True).start()

if __name__=="__main__":
    try:
        import socket
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(("8.8.8.8",80));ip=s.getsockname()[0];s.close()
    except:ip="127.0.0.1"
    port=int(os.environ.get("PORT",5000))
    print(f"\n  PumpScan → http://localhost:{port}  /  http://{ip}:{port}\n")
    app.run(host="0.0.0.0",port=port,debug=False)
