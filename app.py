import os
import time
import threading
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)
SYMBOL = "GC=F"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/" + SYMBOL
PRICE_OFFSET = -35.26
HEADERS = {"User-Agent": "Mozilla/5.0"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
RANGE_MAP = {"1m": "1d", "5m": "5d", "15m": "5d", "30m": "5d", "1h": "1mo", "1d": "1y"}
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
    tf=str(tf or "15m").lower().strip()
    aliases={"1":"1m","1m":"1m","5":"5m","5m":"5m","15":"15m","15m":"15m","30":"30m","30m":"30m","30min":"30m","60":"1h","1h":"1h","1hr":"1h","hour":"1h","2h":"1h","4h":"1h","45m":"30m","day":"1d","daily":"1d","1d":"1d"}
    tf=aliases.get(tf,tf)
    return tf if tf in VALID_TFS else "15m"

def get_yahoo_raw_close():
    try:
        params={"interval":"1m","range":"1d","includePrePost":"false"}
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

def get_gold(interval="15m"):
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

def get_gold_with_live(interval="15m"):
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

def smc_analysis(interval="15m"):
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
def api_candles(): return jsonify(get_gold_with_live(request.args.get("tf","15m")))
@app.route("/api/analysis")
def api_analysis(): return jsonify(smc_analysis(request.args.get("tf","15m")))
@app.route("/api/price")
def api_price():
    tf=request.args.get("tf","15m")
    candles=get_gold_with_live(tf)
    if not candles: return jsonify({"price":0,"market_open":is_market_open(),"offset":PRICE_OFFSET})
    return jsonify({"price":candles[-1]["close"],"market_open":is_market_open(),"timestamp":now_utc_iso(),"offset":PRICE_OFFSET})

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)
