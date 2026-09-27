import os
import time
import threading
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request, render_template, send_from_directory

app = Flask(__name__)
SYMBOL = "GC=F"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/" + SYMBOL
PRICE_OFFSET = -35.26
HEADERS = {"User-Agent": "Mozilla/5.0"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
RANGE_MAP = {"1m": "5d", "5m": "60d", "15m": "60d", "30m": "60d", "1h": "1mo", "1d": "1y"}
VALID_TFS = {"1m", "5m", "15m", "30m", "1h", "1d"}
YAHOO_INTERVAL_MAP = {"1m": "1m","5m": "5m","15m": "15m","30m": "30m","1h": "60m","1d": "1d"}
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 1

def safe_float(v,d=0.0):
    try: return float(v) if v is not None else d
    except: return d
def clamp(v,mi,ma): return max(mi,min(ma,v))
def now_utc_iso(): return datetime.now(timezone.utc).isoformat()
def is_market_open():
    now=datetime.now(timezone.utc)
    d,h=now.weekday(),now.hour
    if d==5: return False
    if d==4 and h>=21: return False
    if d==6 and h<22: return False
    return True
def normalize_tf(tf):
    tf=str(tf or "5m").lower().strip()
    aliases={"1":"1m","1m":"1m","5":"5m","5m":"5m","15":"15m","15m":"15m","30":"30m","30m":"30m","30min":"30m","60":"1h","1h":"1h","1hr":"1h","hour":"1h","2h":"1h","4h":"1h","45m":"30m","day":"1d","daily":"1d","1d":"1d"}
    tf=aliases.get(tf,tf)
    return tf if tf in VALID_TFS else "5m"

def get_yahoo_raw_close():
    try:
        params={"interval":"5m","range":"5d","includePrePost":"false"}
        r=SESSION.get(YAHOO_URL,params=params,timeout=4)
        r.raise_for_status()
        closes=r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes=[c for c in closes if c is not None]
        if closes: return float(closes[-1])
    except: pass
    return None

def get_spot_live():
    try:
        r=SESSION.get("https://api.gold-api.com/price/XAU",timeout=2)
        if r.status_code==200:
            p=float(r.json().get('price',0))
            if p>1000: return p
    except: pass
    return None

def auto_sync_loop():
    global PRICE_OFFSET
    while True:
        try:
            if is_market_open():
                y_raw=get_yahoo_raw_close()
                spot=get_spot_live()
                if y_raw and spot:
                    new_off=spot-y_raw
                    if -70<new_off<-5:
                        PRICE_OFFSET=round(new_off,2)
            time.sleep(2)
        except: time.sleep(2)
threading.Thread(target=auto_sync_loop,daemon=True).start()

def get_gold(interval="5m"):
    interval=normalize_tf(interval)
    now=time.time()
    with _cache_lock:
        if interval in _cache:
            c,t=_cache[interval]
            if now-t<CACHE_TTL: return c
    tf_range=RANGE_MAP[interval]
    yahoo_interval=YAHOO_INTERVAL_MAP.get(interval,interval)
    params={"interval":yahoo_interval,"range":tf_range,"includePrePost":"false","events":"div,splits"}
    try:
        response=SESSION.get(YAHOO_URL,params=params,timeout=5)
        response.raise_for_status()
        result=response.json()["chart"]["result"][0]
        timestamps=result.get("timestamp",[])
        quote=result.get("indicators",{}).get("quote",[{}])[0]
        opens,highs,lows,closes,volumes=quote.get("open",[]),quote.get("high",[]),quote.get("low",[]),quote.get("close",[]),quote.get("volume",[])
        candles=[]
        length=min(len(timestamps),len(opens),len(highs),len(lows),len(closes))
        for i in range(length):
            o,h,l,c=opens[i],highs[i],lows[i],closes[i]
            if None in (o,h,l,c): continue
            candles.append({"time":int(timestamps[i]),"open":round(float(o)+PRICE_OFFSET,2),"high":round(float(h)+PRICE_OFFSET,2),"low":round(float(l)+PRICE_OFFSET,2),"close":round(float(c)+PRICE_OFFSET,2),"volume":int(volumes[i]) if i<len(volumes) and volumes[i] is not None else 0})

        # Robust Fallback if 1m returns empty due to Yahoo intraday limits
        if not candles and interval == "1m":
            params_fallback = {"interval": "5m", "range": "5d", "includePrePost": "false", "events": "div,splits"}
            response_fb = SESSION.get(YAHOO_URL, params=params_fallback, timeout=5)
            response_fb.raise_for_status()
            result_fb = response_fb.json()["chart"]["result"][0]
            timestamps = result_fb.get("timestamp", [])
            quote_fb = result_fb.get("indicators", {}).get("quote", [{}])[0]
            opens, highs, lows, closes, volumes = quote_fb.get("open", []), quote_fb.get("high", []), quote_fb.get("low", []), quote_fb.get("close", []), quote_fb.get("volume", [])
            length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
            for i in range(length):
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                if None in (o, h, l, c): continue
                candles.append({"time": int(timestamps[i]), "open": round(float(o) + PRICE_OFFSET, 2), "high": round(float(h) + PRICE_OFFSET, 2), "low": round(float(l) + PRICE_OFFSET, 2), "close": round(float(c) + PRICE_OFFSET, 2), "volume": int(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0})

        result_candles=candles[-250:]
        with _cache_lock: _cache[interval]=(result_candles,time.time())
        return result_candles
    except Exception as exc:
        print(f"[DATA ERROR] {interval}: {exc}")
        return []

_last_live_price=0
_last_live_fetch=0
def get_oanda_live():
    global _last_live_price,_last_live_fetch
    if time.time()-_last_live_fetch<0.8 and _last_live_price: return _last_live_price
    try:
        r=SESSION.get("https://api.gold-api.com/price/XAU",timeout=2)
        if r.status_code==200:
            p=float(r.json().get('price',0))
            if p>1000:
                _last_live_price=p
                _last_live_fetch=time.time()
                return _last_live_price
    except: pass
    return _last_live_price

def get_gold_with_live(interval="5m"):
    candles=get_gold(interval)
    if not candles or not is_market_open(): return candles
    live=get_oanda_live()
    if live and live>100:
        last=candles[-1]
        last["close"]=round(live,2)
        last["high"]=round(max(last["high"],live),2)
        last["low"]=round(min(last["low"],live),2)
    return candles

def calculate_rsi(closes,period=14):
    if len(closes)<=period: return 50.0
    gains,losses=[],[]
    for i in range(1,len(closes)):
        ch=closes[i]-closes[i-1]
        gains.append(max(ch,0));losses.append(max(-ch,0))
    avg_gain=sum(gains[:period])/period
    avg_loss=sum(losses[:period])/period
    for i in range(period,len(gains)):
        avg_gain=((avg_gain*(period-1))+gains[i])/period
        avg_loss=((avg_loss*(period-1))+losses[i])/period
    if avg_loss==0: return 100.0
    rs=avg_gain/avg_loss
    return round(100-(100/(1+rs)),2)

def calculate_atr(candles,period=14):
    if len(candles)<period+1: return 0.0
    true_ranges=[]
    for i in range(1,len(candles)):
        c,p=candles[i],candles[i-1]
        tr=max(c["high"]-c["low"],abs(c["high"]-p["close"]),abs(c["low"]-p["close"]))
        true_ranges.append(tr)
    return round(sum(true_ranges[-period:])/period,4) if true_ranges else 0.0

def find_swing_highs(candles,left=2,right=2):
    res=[]
    for i in range(left,len(candles)-right):
        h=candles[i]["high"]
        if h>=max(candles[j]["high"] for j in range(i-left,i)) and h>max(candles[j]["high"] for j in range(i+1,i+right+1)):
            res.append(i)
    return res

def find_swing_lows(candles,left=2,right=2):
    res=[]
    for i in range(left,len(candles)-right):
        l=candles[i]["low"]
        if l<=min(candles[j]["low"] for j in range(i-left,i)) and l<min(candles[j]["low"] for j in range(i+1,i+right+1)):
            res.append(i)
    return res

def get_single_tf_bias(tf):
    candles=get_gold(tf)
    if len(candles)<30: return "NEUTRAL"
    sh=find_swing_highs(candles,2,2); sl=find_swing_lows(candles,2,2)
    if len(sh)<2 or len(sl)<2: return "NEUTRAL"
    last_high=candles[sh[-1]]["high"]; prev_high=candles[sh[-2]]["high"]
    last_low=candles[sl[-1]]["low"]; prev_low=candles[sl[-2]]["low"]
    price=candles[-1]["close"]
    if last_high>prev_high and last_low>prev_low and price>=last_low: return "BULLISH"
    if last_high<prev_high and last_low<prev_low and price<=last_high: return "BEARISH"
    if price>last_high: return "BULLISH"
    if price<last_low: return "BEARISH"
    return "NEUTRAL"

def build_mtf_matrix():
    return {"1m":get_single_tf_bias("1m"),"5m":get_single_tf_bias("5m"),"15m":get_single_tf_bias("15m"),"30m":get_single_tf_bias("30m"),"1h":get_single_tf_bias("1h"),"1d":get_single_tf_bias("1d")}

def detect_structure(candles):
    if len(candles)<20: return {"state":"NEUTRAL","event":None,"level":None,"index":None}
    sh=find_swing_highs(candles,2,2); sl=find_swing_lows(candles,2,2)
    if not sh or not sl: return {"state":"NEUTRAL","event":None,"level":None,"index":None}
    last_high_idx=sh[-1]; last_low_idx=sl[-1]
    last_high=candles[last_high_idx]["high"]; last_low=candles[last_low_idx]["low"]
    prev_state="NEUTRAL"
    if len(sh)>=2 and len(sl)>=2:
        h1=candles[sh[-2]]["high"]; h2=candles[sh[-1]]["high"]; l1=candles[sl[-2]]["low"]; l2=candles[sl[-1]]["low"]
        if h2>h1 and l2>l1: prev_state="BULLISH"
        elif h2<h1 and l2<l1: prev_state="BEARISH"
    close=candles[-1]["close"]
    if close>last_high: return {"state":"BULLISH","event":"CHoCH" if prev_state=="BEARISH" else "BOS","level":last_high,"index":last_high_idx}
    if close<last_low: return {"state":"BEARISH","event":"CHoCH" if prev_state=="BULLISH" else "BOS","level":last_low,"index":last_low_idx}
    return {"state":prev_state,"event":None,"level":last_high if prev_state=="BULLISH" else last_low if prev_state=="BEARISH" else None,"index":last_high_idx if prev_state=="BULLISH" else last_low_idx if prev_state=="BEARISH" else None}

def detect_liquidity(candles,tolerance_factor=0.12):
    if len(candles)<30: return [],[]
    atr=calculate_atr(candles) or 1.0
    tol=atr*tolerance_factor
    sh=find_swing_highs(candles,2,2); sl=find_swing_lows(candles,2,2)
    lines,events=[],[]
    if len(sh)>=2:
        a,b=sh[-1],sh[-2]
        p1,p2=candles[a]["high"],candles[b]["high"]
        if abs(p1-p2)<=tol:
            level=round((p1+p2)/2,2)
            if candles[-1]["high"]<=level: lines.append({"price":level,"color":"#00e5ff","title":"EQUAL HIGH","lineStyle":2,"layer":"liquidity","active":True})
            else: events.append({"type":"BUY_SIDE_SWEEP","price":level,"time":candles[-1]["time"],"label":"BUY-SIDE SWEEP"})
    if len(sl)>=2:
        a,b=sl[-1],sl[-2]
        p1,p2=candles[a]["low"],candles[b]["low"]
        if abs(p1-p2)<=tol:
            level=round((p1+p2)/2,2)
            if candles[-1]["low"]>=level: lines.append({"price":level,"color":"#00e5ff","title":"EQUAL LOW","lineStyle":2,"layer":"liquidity","active":True})
            else: events.append({"type":"SELL_SIDE_SWEEP","price":level,"time":candles[-1]["time"],"label":"SELL-SIDE SWEEP"})
    return lines,events

def is_zone_violated(zone,candles,created_index,confirm_bars=3):
    if not candles: return False
    top=safe_float(zone.get("top"),None); bottom=safe_float(zone.get("bottom"),None)
    direction=str(zone.get("direction","")).lower()
    if top is None or bottom is None: return True
    if created_index is None or created_index<0: created_index=0
    if len(candles)-(created_index+1)<confirm_bars: return False
    last_closes=[safe_float(candles[j].get("close"),None) for j in range(len(candles)-confirm_bars,len(candles))]
    if direction=="bullish": return all(c is not None and c<bottom for c in last_closes)
    elif direction=="bearish": return all(c is not None and c>top for c in last_closes)
    return False

def detect_fvgs(candles,max_zones=6):
    active=[]
    if len(candles)<5: return active
    atr=calculate_atr(candles) or 1.0; min_gap=atr*0.10; start=max(2,len(candles)-80); cands=[]
    for i in range(start,len(candles)):
        left,right=candles[i-2],candles[i]
        if right["low"]>left["high"]:
            gap=right["low"]-left["high"]
            if gap>=min_gap: cands.append({"type":"Bullish FVG","label":"BULLISH FVG","direction":"bullish","top":round(right["low"],2),"bottom":round(left["high"],2),"timeStart":left["time"],"createdTime":right["time"],"createdIndex":i,"color":"rgba(0,255,136,0.16)","borderColor":"#00ff88","layer":"fvg","status":"active","valid":True})
        elif right["high"]<left["low"]:
            gap=left["low"]-right["high"]
            if gap>=min_gap: cands.append({"type":"Bearish FVG","label":"BEARISH FVG","direction":"bearish","top":round(left["low"],2),"bottom":round(right["high"],2),"timeStart":left["time"],"createdTime":right["time"],"createdIndex":i,"color":"rgba(255,68,68,0.16)","borderColor":"#ff4444","layer":"fvg","status":"active","valid":True})
    for zone in cands:
        if is_zone_violated(zone,candles,zone["createdIndex"],3): continue
        zone.pop("createdIndex",None); active.append(zone)
    return active[-max_zones:]

def detect_order_blocks(candles,max_zones=6):
    active=[]
    if len(candles)<10: return active
    atr=calculate_atr(candles) or 1.0
    avg_body=sum(abs(c["close"]-c["open"]) for c in candles[-20:])/min(20,len(candles))
    start=max(2,len(candles)-80); cands=[]
    for i in range(start,len(candles)-2):
        cur=candles[i]; nxt=candles[i+1]
        if abs(nxt["close"]-nxt["open"])<=avg_body*1.25 or abs(nxt["close"]-nxt["open"])<=atr*0.35: continue
        if cur["close"]<cur["open"] and nxt["close"]>cur["high"]:
            cands.append({"type":"Bullish Order Block","label":"BULLISH OB","direction":"bullish","top":round(cur["high"],2),"bottom":round(cur["low"],2),"timeStart":cur["time"],"createdTime":nxt["time"],"createdIndex":i+1,"color":"rgba(34,197,94,0.18)","borderColor":"#22c55e","layer":"ob","status":"active","valid":True})
        elif cur["close"]>cur["open"] and nxt["close"]<cur["low"]:
            cands.append({"type":"Bearish Order Block","label":"BEARISH OB","direction":"bearish","top":round(cur["high"],2),"bottom":round(cur["low"],2),"timeStart":cur["time"],"createdTime":nxt["time"],"createdIndex":i+1,"color":"rgba(239,68,68,0.18)","borderColor":"#ef4444","layer":"ob","status":"active","valid":True})
    for zone in cands:
        if is_zone_violated(zone,candles,zone["createdIndex"],3): continue
        zone.pop("createdIndex",None); active.append(zone)
    return active[-max_zones:]

def calculate_pd(candles):
    lookback=min(80,len(candles)); recent=candles[-lookback:]
    swing_high=max(c["high"] for c in recent); swing_low=min(c["low"] for c in recent)
    eq=(swing_high+swing_low)/2
    zone="PREMIUM" if candles[-1]["close"]>eq else "DISCOUNT"
    return {"swing_high":round(swing_high,2),"swing_low":round(swing_low,2),"equilibrium":round(eq,2),"current_zone":zone}

def build_signal(candles,interval,mtf,structure,liq_events,fvg_zones,ob_zones,pd):
    price=candles[-1]["close"]; rsi=calculate_rsi([c["close"] for c in candles]); score=0; reasons=[]
    bull=sum(1 for v in mtf.values() if v=="BULLISH"); bear=sum(1 for v in mtf.values() if v=="BEARISH")
    if bull>=3: score+=2; reasons.append(f"MTF alignment: {bull}/5 bullish")
    elif bear>=3: score-=2; reasons.append(f"MTF alignment: {bear}/5 bearish")
    if structure["state"]=="BULLISH": score+=2; reasons.append(f"Bullish {structure['event']}" if structure["event"] else "Bullish market structure")
    elif structure["state"]=="BEARISH": score-=2; reasons.append(f"Bearish {structure['event']}" if structure["event"] else "Bearish market structure")
    for ev in liq_events:
        if ev["type"]=="SELL_SIDE_SWEEP": score+=2; reasons.append("Sell-side liquidity swept")
        elif ev["type"]=="BUY_SIDE_SWEEP": score-=2; reasons.append("Buy-side liquidity swept")
    if score>=6: sig="STRONG BUY"
    elif score>=3: sig="BUY"
    elif score<=-6: sig="STRONG SELL"
    elif score<=-3: sig="SELL"
    else: sig="WAIT"
    atr=calculate_atr(candles) or max(price*0.001,0.10)
    return {"signal":sig,"score":score,"confidence":round(clamp(50+abs(score)*5,50,95)),"rsi":rsi,"reasons":reasons,"bullish_mtf":bull,"bearish_mtf":bear,"atr":round(atr,4)}

def smc_analysis(interval="5m"):
    interval=normalize_tf(interval)
    static_candles=get_gold(interval)
    live_candles=get_gold_with_live(interval)
    if len(static_candles)<40:
        return {"status":"INSUFFICIENT_DATA","symbol":SYMBOL,"timeframe":interval,"price":live_candles[-1]["close"] if live_candles else 0,"signal":"WAIT","score":0,"confidence":0,"rsi":50,"candles":live_candles,"markers":[],"lines":[],"zones":[],"reasons":["Insufficient history"],"mtf_matrix":{},"pd_zones":{}}
    price=live_candles[-1]["close"] if live_candles else static_candles[-1]["close"]
    mtf=build_mtf_matrix()
    structure=detect_structure(static_candles)
    liq_lines,liq_events=detect_liquidity(static_candles)
    fvg_zones=detect_fvgs(static_candles)
    ob_zones=detect_order_blocks(static_candles)
    pd=calculate_pd(static_candles)
    signal_data=build_signal(live_candles,interval,mtf,structure,liq_events,fvg_zones,ob_zones,pd)
    lines=[]
    if structure["level"] is not None:
        lines.append({"price":structure["level"],"color":"#00ff88" if structure["state"]=="BULLISH" else "#ff4444","title":structure["event"] or structure["state"],"lineStyle":0,"layer":"structure","active":True})
    lines.extend(liq_lines)
    zones=[]; zones.extend(fvg_zones); zones.extend(ob_zones)
    markers=[]
    if structure["event"]: markers.append({"time":static_candles[structure["index"]]["time"] if structure["index"] is not None else static_candles[-1]["time"],"label":structure["event"]})
    for ev in liq_events: markers.append({"time":ev["time"],"label":ev["label"]})
    return {"status":"OK","symbol":SYMBOL,"timeframe":interval,"price":price,"signal":signal_data["signal"],"score":signal_data["score"],"confidence":signal_data["confidence"],"rsi":signal_data["rsi"],"candles":live_candles if live_candles else static_candles,"markers":markers,"lines":lines,"zones":zones,"reasons":signal_data["reasons"],"mtf_matrix":mtf,"pd_zones":pd,"atr":signal_data["atr"],"bullish_mtf":signal_data["bullish_mtf"],"bearish_mtf":signal_data["bearish_mtf"],"market_open":is_market_open(),"timestamp":now_utc_iso(),"offset":PRICE_OFFSET}

# ============================================================
# ===================== NEWS MACHINE =========================
# ============================================================
# ADDITIVE ONLY.
# Existing SMC / price / candle / news code is left untouched.
# ============================================================

import sqlite3 as _nm_sqlite3
import json as _nm_json
import re as _nm_re
import urllib.parse as _nm_urlparse
import xml.etree.ElementTree as _nm_ET
from statistics import mean as _nm_mean

NM_DB = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "news_machine.db"
)

NM_EVENT_BEFORE_MINUTES = 30
NM_EVENT_AFTER_MINUTES = 30
NM_CELEBRATION_START = 10
NM_CELEBRATION_END = 20
NM_CALENDAR_REFRESH = 300
NM_RESEARCH_REFRESH = 180

_nm_last_calendar = []
_nm_calendar_time = 0
_nm_research_cache = {}
_nm_research_cache_time = {}
_nm_lock = threading.Lock()

def _nm_db():
    conn = _nm_sqlite3.connect(NM_DB, timeout=10)
    conn.row_factory = _nm_sqlite3.Row
    return conn

def _nm_init_db():
    try:
        conn = _nm_db()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT UNIQUE,
                event_title TEXT,
                event_country TEXT,
                event_time TEXT,
                prediction TEXT,
                confidence REAL,
                evidence_score REAL,
                reason TEXT,
                research_summary TEXT,
                historical_matches INTEGER DEFAULT 0,
                price_before REAL,
                price_5m REAL,
                price_15m REAL,
                price_30m REAL,
                outcome TEXT,
                aligned INTEGER,
                created_at TEXT,
                completed_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_event_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT,
                event_title TEXT,
                event_time TEXT,
                actual REAL,
                forecast REAL,
                previous REAL,
                price_before REAL,
                price_5m REAL,
                price_15m REAL,
                price_30m REAL,
                direction TEXT,
                created_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_research (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT,
                source TEXT,
                title TEXT,
                url TEXT,
                published TEXT,
                summary TEXT,
                sentiment REAL,
                created_at TEXT
            )
        """)

        conn.commit()
        conn.close()

    except Exception as exc:
        print(f"[NEWS MACHINE DB] {exc}")

_nm_init_db()

def _nm_parse_time(value):
    try:
        if not value:
            return None

        text = str(value).strip()

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None

def _nm_event_key(event):
    title = str(event.get("title", "")).strip().lower()
    date = str(event.get("date", "")).strip()
    raw = f"{title}|{date}"
    return _nm_re.sub(r"[^a-z0-9|]+", "-", raw)

def _nm_safe_number(value):
    try:
        if value is None:
            return None

        text = str(value).strip()

        if not text:
            return None

        text = text.replace(",", "")
        text = text.replace("%", "")
        text = _nm_re.sub(r"[^0-9.\-+]", "", text)

        if not text:
            return None

        return float(text)

    except Exception:
        return None

def _nm_is_usd_event(event):
    country = str(event.get("country", "")).upper()
    return country in {"USD", "US", "UNITED STATES", "USA"}

def _nm_get_calendar():
    global _nm_last_calendar, _nm_calendar_time

    now = time.time()

    with _nm_lock:
        if now - _nm_calendar_time < NM_CALENDAR_REFRESH:
            return list(_nm_last_calendar)

    try:
        response = SESSION.get(
            "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
            timeout=5
        )

        if response.status_code!= 200:
            return list(_nm_last_calendar)

        data = response.json()
        events = []

        for raw in data:
            impact = raw.get("impact", "")
            country = raw.get("country", "")

            if str(impact).lower()!= "high":
                continue

            if not _nm_is_usd_event({"country": country}):
                continue

            events.append({
                "id": raw.get("id"),
                "title": raw.get("title", ""),
                "country": country or "USD",
                "date": raw.get("date", ""),
                "impact": "High",
                "previous": raw.get("previous"),
                "forecast": raw.get("forecast"),
                "actual": raw.get("actual")
            })

        with _nm_lock:
            _nm_last_calendar = events
            _nm_calendar_time = now

        return events

    except Exception as exc:
        print(f"[NEWS MACHINE CALENDAR] {exc}")
        return list(_nm_last_calendar)

def _nm_google_news(query):
    try:
        encoded = _nm_urlparse.quote_plus(query)

        url = (
            "https://news.google.com/rss/search?"
            f"q={encoded}&hl=en-US&gl=US&ceid=US:en"
        )

        response = SESSION.get(url, timeout=7)

        if response.status_code!= 200:
            return []

        root = _nm_ET.fromstring(response.text)
        results = []

        for item in root.findall(".//item")[:10]:
            title = item.findtext("title") or ""
            link = item.findtext("link") or ""
            published = item.findtext("pubDate") or ""
            description = item.findtext("description") or ""

            clean_description = _nm_re.sub(
                r"<[^>]+>",
                " ",
                description
            )

            results.append({
                "title": title.strip(),
                "url": link.strip(),
                "published": published.strip(),
                "summary": clean_description.strip()[:800]
            })

        return results

    except Exception as exc:
        print(f"[NEWS MACHINE RESEARCH] {exc}")
        return []

def _nm_x_search(query):
    token = os.environ.get("X_BEARER_TOKEN", "").strip()

    if not token:
        return []

    try:
        endpoint = "https://api.x.com/2/tweets/search/recent"

        params = {
            "query": f"({query}) lang:en -is:retweet",
            "max_results": "25",
            "tweet.fields": "created_at,public_metrics,text"
        }

        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = SESSION.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=7
        )

        if response.status_code!= 200:
            print(
                f"[NEWS MACHINE X] API returned "
                f"{response.status_code}"
            )
            return []

        payload = response.json()
        results = []

        for tweet in payload.get("data", []):
            results.append({
                "text": tweet.get("text", ""),
                "created_at": tweet.get("created_at"),
                "metrics": tweet.get("public_metrics", {})
            })

        return results

    except Exception as exc:
        print(f"[NEWS MACHINE X] {exc}")
        return []

_NM_BULLISH_WORDS = {
    "bullish", "surge", "surges", "rally", "rallies", "higher",
    "rise", "rising", "strong", "hawkish", "inflation",
    "safe haven", "geopolitical", "uncertainty", "dovish",
    "rate cut", "cuts rates"
}

_NM_BEARISH_WORDS = {
    "bearish", "drop", "drops", "fall", "falling", "lower",
    "weak", "selloff", "sell-off", "hawkish dollar",
    "strong dollar", "higher yields", "yield surge", "rate hike"
}

def _nm_sentiment(text):
    text = str(text or "").lower()

    bull = sum(
        1 for word in _NM_BULLISH_WORDS
        if word in text
    )

    bear = sum(
        1 for word in _NM_BEARISH_WORDS
        if word in text
    )

    total = bull + bear

    if total == 0:
        return 0.0

    return round((bull - bear) / total, 3)

def _nm_research_event(event):
    key = _nm_event_key(event)
    now = time.time()

    if (
        key in _nm_research_cache
        and now - _nm_research_cache_time.get(key, 0)
        < NM_RESEARCH_REFRESH
    ):
        return _nm_research_cache[key]

    title = str(event.get("title", "")).strip()

    queries = [
        f"{title} gold XAUUSD",
        f"{title} Federal Reserve dollar gold",
        f"{title} USD Treasury yields gold"
    ]

    articles = []

    for query in queries:
        articles.extend(_nm_google_news(query))

    unique = {}

    for article in articles:
        url = article.get("url", "")
        if url:
            unique[url] = article

    articles = list(unique.values())[:20]

    article_scores = [
        _nm_sentiment(
            article.get("title", "") + " " +
            article.get("summary", "")
        )
        for article in articles
    ]

    news_sentiment = (
        round(_nm_mean(article_scores), 3)
        if article_scores
        else None
    )

    x_posts = _nm_x_search(
        f'"{title}" gold OR XAUUSD'
    )

    x_scores = [
        _nm_sentiment(post.get("text", ""))
        for post in x_posts
    ]

    x_sentiment = (
        round(_nm_mean(x_scores), 3)
        if x_scores
        else None
    )

    result = {
        "articles": articles,
        "x_posts_count": len(x_posts),
        "news_sentiment": news_sentiment,
        "x_sentiment": x_sentiment,
        "research_available": bool(articles or x_posts)
    }

    _nm_research_cache[key] = result
    _nm_research_cache_time[key] = now

    try:
        conn = _nm_db()

        for article in articles[:20]:
            conn.execute("""
                INSERT INTO news_research
                (query, source, title, url, published, summary,
                 sentiment, created_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                title,
                "Google News",
                article.get("title", ""),
                article.get("url", ""),
                article.get("published", ""),
                article.get("summary", ""),
                _nm_sentiment(
                    article.get("title", "") + " " +
                    article.get("summary", "")
                ),
                now_utc_iso()
            ))

        conn.commit()
        conn.close()

    except Exception as exc:
        print(f"[NEWS MACHINE RESEARCH DB] {exc}")

    return result

def _nm_market_context():
    try:
        candles = get_gold_with_live("5m")

        if len(candles) < 30:
            return {"available": False}

        closes = [c["close"] for c in candles]
        price = closes[-1]
        short = _nm_mean(closes[-10:])
        medium = _nm_mean(closes[-20:])
        rsi = calculate_rsi(closes)
        atr = calculate_atr(candles)
        structure = detect_structure(candles)

        try:
            smc = smc_analysis("5m")
        except Exception:
            smc = {}

        trend = "NEUTRAL"

        if short > medium:
            trend = "BULLISH"
        elif short < medium:
            trend = "BEARISH"

        return {
            "available": True,
            "price": round(price, 2),
            "short_average": round(short, 2),
            "medium_average": round(medium, 2),
            "rsi": rsi,
            "atr": atr,
            "trend": trend,
            "structure": structure,
            "smc_signal": smc.get("signal"),
            "smc_score": smc.get("score"),
            "smc_confidence": smc.get("confidence")
        }

    except Exception as exc:
        print(f"[NEWS MACHINE MARKET] {exc}")
        return {"available": False}

def _nm_find_historical_matches(event):
    title = str(event.get("title", "")).lower()

    try:
        conn = _nm_db()

        rows = conn.execute("""
            SELECT *
            FROM news_event_history
            ORDER BY event_time DESC
            LIMIT 250
        """).fetchall()

        conn.close()

    except Exception:
        return []

    matches = []

    words = [
        w for w in _nm_re.findall(r"[a-z0-9]+", title)
        if len(w) > 2
    ]

    if not words:
        return []

    for row in rows:
        old_title = str(row["event_title"] or "").lower()

        overlap = sum(
            1 for word in words
            if word in old_title
        )

        similarity = overlap / max(len(words), 1)

        if similarity >= 0.45:
            matches.append(dict(row))

    return matches[:30]

def _nm_historical_bias(matches):
    if not matches:
        return {
            "direction": None,
            "strength": 0,
            "sample": 0
        }

    bullish = 0
    bearish = 0

    for match in matches:
        direction = str(
            match.get("direction") or ""
        ).upper()

        if direction == "BUY":
            bullish += 1
        elif direction == "SELL":
            bearish += 1

    total = bullish + bearish

    if total == 0:
        return {
            "direction": None,
            "strength": 0,
            "sample": 0
        }

    if bullish > bearish:
        direction = "BUY"
        strength = bullish / total
    elif bearish > bullish:
        direction = "SELL"
        strength = bearish / total
    else:
        direction = None
        strength = 0

    return {
        "direction": direction,
        "strength": round(strength, 3),
        "sample": total
    }

def _nm_macro_context(event):
    title = str(event.get("title", "")).lower()

    forecast = _nm_safe_number(
        event.get("forecast")
    )

    previous = _nm_safe_number(
        event.get("previous")
    )

    if forecast is None or previous is None:
        return {
            "direction": None,
            "strength": 0
        }

    difference = forecast - previous

    if abs(difference) < 0.000001:
        return {
            "direction": None,
            "strength": 0
        }

    if any(word in title for word in [
        "unemployment",
        "jobless",
        "initial claims",
        "continuing claims"
    ]):
        direction = "BUY" if difference > 0 else "SELL"
    else:
        direction = "SELL" if difference > 0 else "BUY"

    return {
        "direction": direction,
        "strength": 0.20
    }

def _nm_make_prediction(event):
    research = _nm_research_event(event)
    market = _nm_market_context()
    historical_matches = _nm_find_historical_matches(event)
    historical = _nm_historical_bias(historical_matches)
    macro = _nm_macro_context(event)

    buy_score = 0.0
    sell_score = 0.0
    reasons = []

    if historical["direction"] == "BUY":
        weight = 4.0 * historical["strength"]
        buy_score += weight
        reasons.append(
            f"Historical same-event evidence: "
            f"{historical['sample']} matches favour BUY"
        )

    elif historical["direction"] == "SELL":
        weight = 4.0 * historical["strength"]
        sell_score += weight
        reasons.append(
            f"Historical same-event evidence: "
            f"{historical['sample']} matches favour SELL"
        )

    if market.get("trend") == "BULLISH":
        buy_score += 2.0
        reasons.append(
            "Current Gold short-term trend is bullish"
        )

    elif market.get("trend") == "BEARISH":
        sell_score += 2.0
        reasons.append(
            "Current Gold short-term trend is bearish"
        )

    smc_signal = str(
        market.get("smc_signal") or ""
    ).upper()

    if "BUY" in smc_signal:
        buy_score += 2.0
        reasons.append(
            "Existing SMC engine agrees with BUY"
        )

    elif "SELL" in smc_signal:
        sell_score += 2.0
        reasons.append(
            "Existing SMC engine agrees with SELL"
        )

    structure_state = str(
        market.get("structure", {}).get("state", "")
    ).upper()

    if structure_state == "BULLISH":
        buy_score += 1.5
        reasons.append(
            "Current market structure is bullish"
        )

    elif structure_state == "BEARISH":
        sell_score += 1.5
        reasons.append(
            "Current market structure is bearish"
        )

    rsi = safe_float(
        market.get("rsi"),
        50
    )

    if 52 <= rsi <= 68:
        buy_score += 0.75
        reasons.append(
            "RSI supports bullish momentum"
        )

    elif 32 <= rsi <= 48:
        sell_score += 0.75
        reasons.append(
            "RSI supports bearish momentum"
        )

    if macro["direction"] == "BUY":
        buy_score += macro["strength"]
        reasons.append(
            "Forecast/previous macro context mildly favours BUY"
        )

    elif macro["direction"] == "SELL":
        sell_score += macro["strength"]
        reasons.append(
            "Forecast/previous macro context mildly favours SELL"
        )

    news_sentiment = research.get(
        "news_sentiment"
    )

    if news_sentiment is not None:

        if news_sentiment > 0.20:
            buy_score += 0.75
            reasons.append(
                "Online news research has a bullish Gold bias"
            )

        elif news_sentiment < -0.20:
            sell_score += 0.75
            reasons.append(
                "Online news research has a bearish Gold bias"
            )

    x_sentiment = research.get(
        "x_sentiment"
    )

    if x_sentiment is not None:

        if x_sentiment > 0.25:
            buy_score += 0.50
            reasons.append(
                "X research provides additional bullish context"
            )

        elif x_sentiment < -0.25:
            sell_score += 0.50
            reasons.append(
                "X research provides additional bearish context"
            )

    if buy_score >= sell_score:
        prediction = "BUY"
        winning_score = buy_score
        losing_score = sell_score
    else:
        prediction = "SELL"
        winning_score = sell_score
        losing_score = buy_score

    total = buy_score + sell_score

    if total <= 0:
        confidence = 50.0
    else:
        separation = (
            abs(winning_score - losing_score)
            / total
        )

        confidence = 50 + (
            separation * 45
        )

    evidence_count = sum([
        1 if historical["sample"] > 0 else 0,
        1 if market.get("available") else 0,
        1 if smc_signal in {
            "BUY",
            "SELL",
            "STRONG BUY",
            "STRONG SELL"
        } else 0,
        1 if structure_state in {
            "BULLISH",
            "BEARISH"
        } else 0,
        1 if news_sentiment is not None else 0,
        1 if x_sentiment is not None else 0
    ])

    confidence += min(
        evidence_count * 1.5,
        7
    )

    confidence = round(
        clamp(confidence, 50, 97),
        1
    )

    return {
        "prediction": prediction,
        "confidence": confidence,
        "evidence_score": round(
            max(buy_score, sell_score),
            2
        ),
        "buy_score": round(
            buy_score,
            2
        ),
        "sell_score": round(
            sell_score,
            2
        ),
        "reason": " | ".join(reasons[:8]),
        "historical_matches": historical["sample"],
        "historical_bias": historical["direction"],
        "historical_strength": historical["strength"],
        "news_sentiment": news_sentiment,
        "x_sentiment": x_sentiment,
        "market": market,
        "research_available": research[
            "research_available"
        ]
    }

def _nm_get_prediction(event_key):
    try:
        conn = _nm_db()

        row = conn.execute("""
            SELECT *
            FROM news_predictions
            WHERE event_key =?
        """, (event_key,)).fetchone()

        conn.close()

        return dict(row) if row else None

    except Exception:
        return None

def _nm_save_prediction(event, prediction):
    try:
        event_key = _nm_event_key(event)

        if _nm_get_prediction(event_key):
            return

        market = prediction.get(
            "market",
            {}
        )

        price = market.get("price")

        conn = _nm_db()

        conn.execute("""
            INSERT OR IGNORE INTO news_predictions
            (
                event_key,
                event_title,
                event_country,
                event_time,
                prediction,
                confidence,
                evidence_score,
                reason,
                research_summary,
                historical_matches,
                price_before,
                created_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event_key,
            event.get("title", ""),
            event.get("country", "USD"),
            event.get("date", ""),
            prediction["prediction"],
            prediction["confidence"],
            prediction["evidence_score"],
            prediction["reason"],
            _nm_json.dumps({
                "news_sentiment": prediction.get(
                    "news_sentiment"
                ),
                "x_sentiment": prediction.get(
                    "x_sentiment"
                ),
                "research_available": prediction.get(
                    "research_available"
                )
            }),
            prediction.get(
                "historical_matches",
                0
            ),
            price,
            now_utc_iso()
        ))

        conn.commit()
        conn.close()

    except Exception as exc:
        print(
            f"[NEWS MACHINE SAVE] {exc}"
        )

def _nm_capture_outcomes():
    try:
        conn = _nm_db()

        rows = conn.execute("""
            SELECT *
            FROM news_predictions
            WHERE outcome IS NULL
            ORDER BY id DESC
            LIMIT 50
        """).fetchall()

        for row in rows:

            event_time = _nm_parse_time(
                row["event_time"]
            )

            if not event_time:
                continue

            now = datetime.now(timezone.utc)

            minutes_after = (
                now - event_time
            ).total_seconds() / 60

            if minutes_after < NM_EVENT_AFTER_MINUTES:
                continue

            candles = get_gold("5m")

            if not candles:
                continue

            event_ts = event_time.timestamp()

            before = None
            after_5 = None
            after_15 = None
            after_30 = None

            for candle in candles:

                t = candle["time"]

                if t <= event_ts:
                    before = candle["close"]

                if (
                    event_ts < t <=
                    event_ts + 5 * 60
                ):
                    after_5 = candle["close"]

                if (
                    event_ts < t <=
                    event_ts + 15 * 60
                ):
                    after_15 = candle["close"]

                if (
                    event_ts < t <=
                    event_ts + 30 * 60
                ):
                    after_30 = candle["close"]

            if before is None or after_30 is None:
                continue

            prediction = str(
                row["prediction"]
            ).upper()

            if after_30 > before:
                actual_direction = "BUY"
            elif after_30 < before:
                actual_direction = "SELL"
            else:
                actual_direction = None

            if actual_direction == prediction:
                outcome = "WIN"
                aligned = 1
            elif actual_direction:
                outcome = "LOSS"
                aligned = 0
            else:
                outcome = "UNRESOLVED"
                aligned = 0

            conn.execute("""
                UPDATE news_predictions
                SET
                    price_5m =?,
                    price_15m =?,
                    price_30m =?,
                    outcome =?,
                    aligned =?,
                    completed_at =?
                WHERE id =?
            """, (
                after_5,
                after_15,
                after_30,
                outcome,
                aligned,
                now_utc_iso(),
                row["id"]
            ))

            conn.execute("""
                INSERT INTO news_event_history
                (
                    event_key,
                    event_title,
                    event_time,
                    price_before,
                    price_5m,
                    price_15m,
                    price_30m,
                    direction,
                    created_at
                )
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                row["event_key"],
                row["event_title"],
                row["event_time"],
                before,
                after_5,
                after_15,
                after_30,
                actual_direction,
                now_utc_iso()
            ))

        conn.commit()
        conn.close()

    except Exception as exc:
        print(
            f"[NEWS MACHINE OUTCOME] {exc}"
        )

def _nm_get_current():
    events = _nm_get_calendar()
    now = datetime.now(timezone.utc)

    high_events_today = []

    for event in events:

        event_time = _nm_parse_time(
            event.get("date")
        )

        if not event_time:
            continue

        if event_time.date()!= now.date():
            continue

        high_events_today.append(
            (event, event_time)
        )

    high_events_today.sort(
        key=lambda x: x[1]
    )

    for event, event_time in high_events_today:

        minutes_until = (
            event_time - now
        ).total_seconds() / 60

        if (
            0 <
            minutes_until <=
            NM_EVENT_BEFORE_MINUTES
        ):

            prediction = _nm_make_prediction(
                event
            )

            _nm_save_prediction(
                event,
                prediction
            )

            return {
                "status": "PREDICTION",
                "event": event,
                "prediction": prediction[
                    "prediction"
                ],
                "confidence": prediction[
                    "confidence"
                ],
                "evidence_score": prediction[
                    "evidence_score"
                ],
                "reason": prediction[
                    "reason"
                ],
                "historical_matches": prediction[
                    "historical_matches"
                ],
                "aligned": False,
                "celebration": False,
                "server_time": now_utc_iso()
            }

        minutes_after = (
            now - event_time
        ).total_seconds() / 60

        if (
            0 <= minutes_after
            <= NM_EVENT_AFTER_MINUTES
        ):

            stored = _nm_get_prediction(
                _nm_event_key(event)
            )

            aligned = False
            celebration = False

            if stored:

                aligned = (
                    stored.get("aligned") == 1
                )

                celebration = (
                    aligned
                    and NM_CELEBRATION_START
                    <= minutes_after
                    <= NM_CELEBRATION_END
                )

            later_events = [
                e
                for e, t
                in high_events_today
                if t > event_time
            ]

            return {
                "status": (
                    "ANALYSIS ONGOING"
                    if later_events
                    else "NO NEWS"
                ),
                "event": event,
                "prediction": None,
                "confidence": (
                    stored.get("confidence")
                    if stored else None
                ),
                "evidence_score": (
                    stored.get("evidence_score")
                    if stored else None
                ),
                "reason": (
                    stored.get("reason")
                    if stored else None
                ),
                "historical_matches": (
                    stored.get(
                        "historical_matches",
                        0
                    )
                    if stored else 0
                ),
                "aligned": aligned,
                "celebration": celebration,
                "server_time": now_utc_iso()
            }

    future_events = [
        (event, event_time)
        for event, event_time
        in high_events_today
        if event_time > now
    ]

    if future_events:

        event, event_time = future_events[0]

        return {
            "status": "ANALYSIS ONGOING",
            "event": event,
            "prediction": None,
            "confidence": None,
            "evidence_score": None,
            "reason": None,
            "historical_matches": 0,
            "aligned": False,
            "celebration": False,
            "server_time": now_utc_iso()
        }

    return {
        "status": "NO NEWS",
        "event": None,
        "prediction": None,
        "confidence": None,
        "evidence_score": None,
        "reason": None,
        "historical_matches": 0,
        "aligned": False,
        "celebration": False,
        "server_time": now_utc_iso()
    }

def _nm_statistics():
    try:
        conn = _nm_db()

        total = conn.execute("""
            SELECT COUNT(*)
            FROM news_predictions
            WHERE outcome IN ('WIN', 'LOSS')
        """).fetchone()[0]

        wins = conn.execute("""
            SELECT COUNT(*)
            FROM news_predictions
            WHERE outcome = 'WIN'
        """).fetchone()[0]

        losses = conn.execute("""
            SELECT COUNT(*)
            FROM news_predictions
            WHERE outcome = 'LOSS'
        """).fetchone()[0]

        avg_confidence = conn.execute("""
            SELECT AVG(confidence)
            FROM news_predictions
            WHERE outcome IN ('WIN', 'LOSS')
        """).fetchone()[0]

        conn.close()

        win_rate = (
            round((wins / total) * 100, 2)
            if total else None
        )

        avg_confidence = (
            round(float(avg_confidence), 2)
            if avg_confidence is not None
            else None
        )

        return {
            "signals": total,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "average_confidence": avg_confidence
        }

    except Exception as exc:
        print(
            f"[NEWS MACHINE STATS] {exc}"
        )

        return {
            "signals": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "average_confidence": None
        }

def _nm_background_worker():

    while True:

        try:
            _nm_get_calendar()
            _nm_capture_outcomes()

            events = _nm_get_calendar()
            now = datetime.now(timezone.utc)

            for event in events:

                event_time = _nm_parse_time(
                    event.get("date")
                )

                if not event_time:
                    continue

                minutes_until = (
                    event_time - now
                ).total_seconds() / 60

                if (
                    0 <
                    minutes_until <=
                    NM_EVENT_BEFORE_MINUTES
                ):
                    try:
                        _nm_research_event(
                            event
                        )
                    except Exception:
                        pass

            time.sleep(20)

        except Exception as exc:
            print(
                f"[NEWS MACHINE WORKER] {exc}"
            )
            time.sleep(20)

threading.Thread(
    target=_nm_background_worker,
    daemon=True
).start()

# ============================================================
# NEWS MACHINE ROUTES
# ============================================================

@app.route("/api/news-analysis")
def api_news_analysis():
    try:
        return jsonify(
            _nm_get_current()
        )

    except Exception as exc:

        print(
            f"[NEWS MACHINE API] {exc}"
        )

        return jsonify({
            "status": "ANALYSIS ONGOING",
            "event": None,
            "prediction": None,
            "confidence": None,
            "evidence_score": None,
            "reason": None,
            "historical_matches": 0,
            "aligned": False,
            "celebration": False,
            "server_time": now_utc_iso()
        })

@app.route("/api/news-analysis/statistics")
def api_news_analysis_statistics():
    return jsonify(
        _nm_statistics()
    )

# ============================================================
# ===================== EXISTING ROUTES ======================
# ============================================================

_news_cache={"data":[],"time":0}
@app.route("/api/news")
def api_news():
    now=time.time()
    if now-_news_cache["time"]<300: return jsonify(_news_cache["data"])
    try:
        r=SESSION.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json",timeout=2)
        if r.status_code==200:
            data=r.json(); high=[]
            for ev in data[:20]:
                if ev.get("impact")=="High" and ev.get("country") in ["USD","US"]:
                    high.append({"title":ev.get("title",""),"date":ev.get("date",""),"country":ev.get("country","USD"),"impact":"High"})
            _news_cache["data"]=high[:5]; _news_cache["time"]=now
            return jsonify(high[:5])
    except Exception as e: print(f"[NEWS] {e}")
    return jsonify(_news_cache["data"])

@app.route("/")
def index(): return render_template("index.html")
@app.route("/api/candles")
def api_candles(): return jsonify(get_gold_with_live(request.args.get("tf","5m")))
@app.route("/api/analysis")
def api_analysis(): return jsonify(smc_analysis(request.args.get("tf","5m")))
@app.route("/api/price")
def api_price():
    tf=request.args.get("tf","5m")
    candles=get_gold_with_live(tf)
    if not candles: return jsonify({"price":0,"market_open":is_market_open(),"offset":PRICE_OFFSET})
    return jsonify({"price":candles[-1]["close"],"market_open":is_market_open(),"timestamp":now_utc_iso(),"offset":PRICE_OFFSET})

@app.route('/robots.txt')
def robots_txt():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'robots.txt')

@app.route('/sitemap.xml')
def sitemap_xml():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'sitemap.xml')

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)