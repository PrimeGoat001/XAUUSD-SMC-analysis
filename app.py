import time
import threading
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

SYMBOL = "XAUUSD Spot"
PRICE_OFFSET = 0.00

HEADERS = {"User-Agent": "Mozilla/5.0"}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Try multiple Binance endpoints - fixes 451 block
BINANCE_URLS = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
]

INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m",
    "30m": "30m", "45m": "45m",
    "1h": "1h", "2h": "2h", "4h": "4h", "1d": "1d"
}
VALID_TFS = set(INTERVAL_MAP.keys())

_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 2

def safe_float(v, d=0.0):
    try: return float(v) if v is not None else d
    except: return d

def clamp(v, mn, mx): return max(mn, min(mx, v))
def now_utc_iso(): return datetime.now(timezone.utc).isoformat()

def normalize_tf(tf):
    tf = str(tf or "1m").lower().strip()
    aliases = {"1":"1m","5":"5m","15":"15m","30":"30m","45":"45m","60":"1h","1hr":"1h","2hr":"2h","4hr":"4h","hour":"1h","day":"1d"}
    tf = aliases.get(tf, tf)
    return tf if tf in VALID_TFS else "1m"

def is_market_open():
    now = datetime.now(timezone.utc)
    d,h = now.weekday(), now.hour
    if d==5: return False
    if d==4 and h>=21: return False
    if d==6 and h<22: return False
    return True

def aggregate_candles(candles, factor):
    if not candles or factor<=1: return candles
    agg=[]
    for i in range(0,len(candles),factor):
        chunk=candles[i:i+factor]
        if len(chunk)<factor: continue
        agg.append({
            "time":chunk[0]["time"],
            "open":chunk[0]["open"],
            "high":round(max(c["high"] for c in chunk),2),
            "low":round(min(c["low"] for c in chunk),2),
            "close":chunk[-1]["close"],
            "volume":sum(c["volume"] for c in chunk)
        })
    return agg

def fetch_binance_klines(symbol, interval, limit):
    for url in BINANCE_URLS:
        try:
            r = SESSION.get(url, params={"symbol":symbol,"interval":interval,"limit":limit}, timeout=10)
            if r.status_code==200:
                data=r.json()
                if isinstance(data,list) and len(data)>0:
                    return data
        except Exception as e:
            print(f"Fail {url}: {e}")
            continue
    return None

def get_gold(interval="1m"):
    interval = normalize_tf(interval)
    now=time.time()
    with _cache_lock:
        if interval in _cache:
            c,t = _cache[interval]
            if now-t < CACHE_TTL:
                return c

    # 45m custom
    if interval=="45m":
        data = fetch_binance_klines("PAXGUSDT","15m",750)
        if not data: return []
        candles=[]
        for k in data:
            candles.append({
                "time":int(k[0]/1000),
                "open":round(float(k[1])+PRICE_OFFSET,2),
                "high":round(float(k[2])+PRICE_OFFSET,2),
                "low":round(float(k[3])+PRICE_OFFSET,2),
                "close":round(float(k[4])+PRICE_OFFSET,2),
                "volume":int(float(k[5])) if k[5] else 0,
            })
        res = aggregate_candles(candles,3)[-250:]
        with _cache_lock: _cache[interval]=(res,time.time())
        return res

    bin_interval = INTERVAL_MAP.get(interval,"1m")
    data = fetch_binance_klines("PAXGUSDT", bin_interval, 250)
    if not data:
        print(f"NO DATA for {interval}")
        return []
    candles=[]
    for k in data:
        candles.append({
            "time":int(k[0]/1000),
            "open":round(float(k[1])+PRICE_OFFSET,2),
            "high":round(float(k[2])+PRICE_OFFSET,2),
            "low":round(float(k[3])+PRICE_OFFSET,2),
            "close":round(float(k[4])+PRICE_OFFSET,2),
            "volume":int(float(k[5])) if k[5] else 0,
        })
    res = sorted(candles, key=lambda x:x["time"])[-250:]
    with _cache_lock: _cache[interval]=(res,time.time())
    return res

_last_live_price=0
_last_live_fetch=0
def get_oanda_live():
    global _last_live_price,_last_live_fetch
    if time.time()-_last_live_fetch<1 and _last_live_price: return _last_live_price
    for url in ["https://api.gold-api.com/price/XAU","https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT","https://data-api.binance.vision/api/v3/ticker/price?symbol=PAXGUSDT"]:
        try:
            r=SESSION.get(url,timeout=4)
            if r.status_code==200:
                j=r.json()
                p=float(j.get('price',0))
                if p>1000:
                    _last_live_price=p
                    _last_live_fetch=time.time()
                    return p
        except: continue
    return _last_live_price

def get_gold_with_live(interval="1m"):
    candles=get_gold(interval)
    if not candles: return candles
    if not is_market_open(): return candles
    live=get_oanda_live()
    if live and live>100:
        lc=live+PRICE_OFFSET
        last=candles[-1]
        last["close"]=round(lc,2)
        last["high"]=round(max(last["high"],lc),2)
        last["low"]=round(min(last["low"],lc),2)
    return candles

def calculate_rsi(closes, period=14):
    if len(closes)<=period: return 50.0
    gains,losses=[],[]
    for i in range(1,len(closes)):
        ch=closes[i]-closes[i-1]
        gains.append(max(ch,0)); losses.append(max(-ch,0))
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    for i in range(period,len(gains)):
        ag=((ag*(period-1))+gains[i])/period; al=((al*(period-1))+losses[i])/period
    if al==0: return 100.0
    return round(100-(100/(1+ag/al)),2)

def calculate_atr(candles, period=14):
    if len(candles)<period+1: return 0.0
    tr=[]
    for i in range(1,len(candles)):
        c,p=candles[i],candles[i-1]
        tr.append(max(c["high"]-c["low"],abs(c["high"]-p["close"]),abs(c["low"]-p["close"])))
    return round(sum(tr[-period:])/period,4) if tr else 0.0

def find_swing_highs(candles,l=2,r=2):
    res=[]
    for i in range(l,len(candles)-r):
        h=candles[i]["high"]
        if h>=max(candles[j]["high"] for j in range(i-l,i)) and h>max(candles[j]["high"] for j in range(i+1,i+r+1)):
            res.append(i)
    return res

def find_swing_lows(candles,l=2,r=2):
    res=[]
    for i in range(l,len(candles)-r):
        lo=candles[i]["low"]
        if lo<=min(candles[j]["low"] for j in range(i-l,i)) and lo<min(candles[j]["low"] for j in range(i+1,i+r+1)):
            res.append(i)
    return res

def get_single_tf_bias(tf):
    candles=get_gold(tf)
    if len(candles)<30: return "NEUTRAL"
    sh=find_swing_highs(candles); sl=find_swing_lows(candles)
    if len(sh)<2 or len(sl)<2: return "NEUTRAL"
    lh,ph=candles[sh[-1]]["high"],candles[sh[-2]]["high"]
    ll,pl=candles[sl[-1]]["low"],candles[sl[-2]]["low"]
    price=candles[-1]["close"]
    if lh>ph and ll>pl and price>=ll: return "BULLISH"
    if lh<ph and ll<pl and price<=lh: return "BEARISH"
    return "BULLISH" if price>lh else "BEARISH" if price<ll else "NEUTRAL"

def build_mtf_matrix():
    return {tf:get_single_tf_bias(tf) for tf in ["1m","5m","15m","30m","45m","1h","2h","4h","1d"]}

def detect_structure(candles):
    if len(candles)<20: return {"state":"NEUTRAL","event":None,"level":None,"index":None}
    sh=find_swing_highs(candles); sl=find_swing_lows(candles)
    if not sh or not sl: return {"state":"NEUTRAL","event":None,"level":None,"index":None}
    lhi,lli=sh[-1],sl[-1]
    lh,ll=candles[lhi]["high"],candles[lli]["low"]
    prev="NEUTRAL"
    if len(sh)>=2 and len(sl)>=2:
        if candles[sh[-1]]["high"]>candles[sh[-2]]["high"] and candles[sl[-1]]["low"]>candles[sl[-2]]["low"]: prev="BULLISH"
        elif candles[sh[-1]]["high"]<candles[sh[-2]]["high"] and candles[sl[-1]]["low"]<candles[sl[-2]]["low"]: prev="BEARISH"
    close=candles[-1]["close"]
    if close>lh: return {"state":"BULLISH","event":"CHoCH" if prev=="BEARISH" else "BOS","level":lh,"index":lhi}
    if close<ll: return {"state":"BEARISH","event":"CHoCH" if prev=="BULLISH" else "BOS","level":ll,"index":lli}
    return {"state":prev,"event":None,"level":lh if prev=="BULLISH" else ll if prev=="BEARISH" else None,"index":lhi if prev=="BULLISH" else lli if prev=="BEARISH" else None}

def detect_liquidity(candles, tol=0.12):
    if len(candles)<30: return [],[]
    atr=calculate_atr(candles) or 1.0; tolerance=atr*tol
    sh=find_swing_highs(candles); sl=find_swing_lows(candles)
    lines,events=[],[]
    if len(sh)>=2:
        a,b=sh[-1],sh[-2]; p1,p2=candles[a]["high"],candles[b]["high"]
        if abs(p1-p2)<=tolerance:
            lvl=round((p1+p2)/2,2)
            if candles[-1]["high"]<=lvl: lines.append({"price":lvl,"color":"#00e5ff","title":"EQUAL HIGH","lineStyle":2,"layer":"liquidity","active":True})
            else: events.append({"type":"BUY_SIDE_SWEEP","price":lvl,"time":candles[-1]["time"],"label":"BUY-SIDE SWEEP"})
    if len(sl)>=2:
        a,b=sl[-1],sl[-2]; p1,p2=candles[a]["low"],candles[b]["low"]
        if abs(p1-p2)<=tolerance:
            lvl=round((p1+p2)/2,2)
            if candles[-1]["low"]>=lvl: lines.append({"price":lvl,"color":"#00e5ff","title":"EQUAL LOW","lineStyle":2,"layer":"liquidity","active":True})
            else: events.append({"type":"SELL_SIDE_SWEEP","price":lvl,"time":candles[-1]["time"],"label":"SELL-SIDE SWEEP"})
    return lines,events

def is_zone_violated(zone,candles,idx):
    if not candles: return False
    top=safe_float(zone.get("top"),None); bot=safe_float(zone.get("bottom"),None); d=str(zone.get("direction","")).lower()
    if top is None or bot is None: return True
    if idx is None or idx<0: idx=0
    for j in range(idx+1,len(candles)):
        c=safe_float(candles[j].get("close"),None)
        if c is None: continue
        if d=="bullish" and c<bot: return True
        if d=="bearish" and c>top: return True
    return False

def detect_fvgs(candles,max_zones=6):
    active=[]
    if len(candles)<5: return active
    atr=calculate_atr(candles) or 1.0; ming=atr*0.10
    start=max(2,len(candles)-80); cands=[]
    for i in range(start,len(candles)):
        left,middle,right=candles[i-2],candles[i-1],candles[i]
        if right["low"]>left["high"]:
            gap=right["low"]-left["high"]
            if gap>=ming:
                cands.append({"type":"Bullish FVG","label":"BULLISH FVG","direction":"bullish","top":round(right["low"],2),"bottom":round(left["high"],2),"timeStart":left["time"],"createdTime":right["time"],"createdIndex":i,"color":"rgba(0,255,136,0.16)","borderColor":"#00ff88","layer":"fvg","status":"active","valid":True})
        elif right["high"]<left["low"]:
            gap=left["low"]-right["high"]
            if gap>=ming:
                cands.append({"type":"Bearish FVG","label":"BEARISH FVG","direction":"bearish","top":round(left["low"],2),"bottom":round(right["high"],2),"timeStart":left["time"],"createdTime":right["time"],"createdIndex":i,"color":"rgba(255,68,68,0.16)","borderColor":"#ff4444","layer":"fvg","status":"active","valid":True})
    for z in cands:
        if is_zone_violated(z,candles,z["createdIndex"]): continue
        z.pop("createdIndex",None); active.append(z)
    return active[-max_zones:]

def detect_order_blocks(candles,max_zones=6):
    active=[]
    if len(candles)<10: return active
    atr=calculate_atr(candles) or 1.0; avg_body=sum(abs(c["close"]-c["open"]) for c in candles[-20:])/min(20,len(candles))
    start=max(2,len(candles)-80); cands=[]
    for i in range(start,len(candles)-2):
        cur=candles[i]; nxt=candles[i+1]; nb=abs(nxt["close"]-nxt["open"])
        if not (nb>avg_body*1.25 and nb>atr*0.35): continue
        if cur["close"]<cur["open"] and nxt["close"]>cur["high"]:
            cands.append({"type":"Bullish Order Block","label":"BULLISH OB","direction":"bullish","top":round(cur["high"],2),"bottom":round(cur["low"],2),"timeStart":cur["time"],"createdTime":nxt["time"],"createdIndex":i+1,"color":"rgba(34,197,94,0.18)","borderColor":"#22c55e","layer":"ob","status":"active","valid":True})
        elif cur["close"]>cur["open"] and nxt["close"]<cur["low"]:
            cands.append({"type":"Bearish Order Block","label":"BEARISH OB","direction":"bearish","top":round(cur["high"],2),"bottom":round(cur["low"],2),"timeStart":cur["time"],"createdTime":nxt["time"],"createdIndex":i+1,"color":"rgba(239,68,68,0.18)","borderColor":"#ef4444","layer":"ob","status":"active","valid":True})
    for z in cands:
        if is_zone_violated(z,candles,z["createdIndex"]): continue
        z.pop("createdIndex",None); active.append(z)
    return active[-max_zones:]

def calculate_pd(candles):
    lb=min(80,len(candles)); recent=candles[-lb:]
    sh=max(c["high"] for c in recent); sl=min(c["low"] for c in recent)
    eq=(sh+sl)/2; price=candles[-1]["close"]
    return {"swing_high":round(sh,2),"swing_low":round(sl,2),"equilibrium":round(eq,2),"current_zone":"PREMIUM" if price>eq else "DISCOUNT"}

def build_signal(candles,interval,mtf,structure,liq_events,fvg_zones,ob_zones,pd):
    price=candles[-1]["close"]; rsi=calculate_rsi([c["close"] for c in candles])
    score=0; reasons=[]
    bull=sum(1 for v in mtf.values() if v=="BULLISH"); bear=sum(1 for v in mtf.values() if v=="BEARISH")
    if bull>=3: score+=2; reasons.append(f"MTF: {bull}/9 bullish")
    elif bear>=3: score-=2; reasons.append(f"MTF: {bear}/9 bearish")
    if structure["state"]=="BULLISH": score+=2; reasons.append(f"Bullish {structure['event']}" if structure['event'] else "Bullish structure")
    elif structure["state"]=="BEARISH": score-=2; reasons.append(f"Bearish {structure['event']}" if structure['event'] else "Bearish structure")
    for ev in liq_events:
        if ev["type"]=="SELL_SIDE_SWEEP": score+=2; reasons.append("Sell-side swept")
        elif ev["type"]=="BUY_SIDE_SWEEP": score-=2; reasons.append("Buy-side swept")
    if score>=6: sig="STRONG BUY"
    elif score>=3: sig="BUY"
    elif score<=-6: sig="STRONG SELL"
    elif score<=-3: sig="SELL"
    else: sig="WAIT"
    atr=calculate_atr(candles) or max(price*0.001,0.10); entry=round(price,2); sld=atr*1.5
    if sig in ("BUY","STRONG BUY"):
        sl=round(entry-sld,2); risk=entry-sl; tp1,tp2,tp3=round(entry+risk,2),round(entry+risk*2,2),round(entry+risk*3,2)
    elif sig in ("SELL","STRONG SELL"):
        sl=round(entry+sld,2); risk=sl-entry; tp1,tp2,tp3=round(entry-risk,2),round(entry-risk*2,2),round(entry-risk*3,2)
    else: sl,tp1,tp2,tp3=None,None,None,None
    conf=round(clamp(50+abs(score)*5,50,95))
    return {"signal":sig,"score":score,"confidence":conf,"rsi":rsi,"entry":entry,"stopLoss":sl,"takeProfit1":tp1,"takeProfit2":tp2,"takeProfit3":tp3,"reasons":reasons,"bullish_mtf":bull,"bearish_mtf":bear,"atr":round(atr,4)}

def smc_analysis(interval="1m"):
    interval=normalize_tf(interval)
    candles=get_gold_with_live(interval)
    if len(candles)<40:
        return {"status":"INSUFFICIENT_DATA","symbol":SYMBOL,"timeframe":interval,"price":0,"signal":"WAIT","score":0,"confidence":0,"rsi":50,"candles":[],"markers":[],"lines":[],"zones":[],"reasons":[f"Binance blocked? Got {len(candles)} candles. Check server logs."],"mtf_matrix":{},"pd_zones":{},"market_open":is_market_open()}
    price=candles[-1]["close"]; mtf=build_mtf_matrix(); struct=detect_structure(candles)
    liq_lines,liq_events=detect_liquidity(candles); fvg=detect_fvgs(candles); ob=detect_order_blocks(candles); pd=calculate_pd(candles)
    sig=build_signal(candles,interval,mtf,struct,liq_events,fvg,ob,pd)
    lines=[]
    if struct["level"] is not None:
        lines.append({"price":struct["level"],"color":"#00ff88" if struct["state"]=="BULLISH" else "#ff4444","title":struct["event"] or "STRUCTURE","lineStyle":0,"layer":"bos_choch"})
    lines.extend(liq_lines)
    lines.append({"price":pd["swing_low"],"color":"#22c55e","title":"DEMAND","lineStyle":1,"layer":"structure"})
    lines.append({"price":pd["swing_high"],"color":"#ef4444","title":"SUPPLY","lineStyle":1,"layer":"structure"})
    markers=[]
    if struct["event"]:
        markers.append({"time":candles[-1]["time"],"position":"belowBar" if struct["state"]=="BULLISH" else "aboveBar","color":"#ffffff" if struct["event"]=="CHoCH" else ("#00ff88" if struct["state"]=="BULLISH" else "#ff4444"),"shape":"arrowUp" if struct["state"]=="BULLISH" else "arrowDown","text":f"{struct['event']} ↑" if struct["state"]=="BULLISH" else f"{struct['event']} ↓","layer":"bos_choch"})
    return {"status":"OK","symbol":SYMBOL,"timeframe":interval,"price":price,"signal":sig["signal"],"score":sig["score"],"confidence":sig["confidence"],"rsi":sig["rsi"],"atr":sig["atr"],"entry":sig["entry"],"stopLoss":sig["stopLoss"],"takeProfit1":sig["takeProfit1"],"takeProfit2":sig["takeProfit2"],"takeProfit3":sig["takeProfit3"],"htf_bias":mtf["1d"],"htf_tf":"1D","structure":struct,"demand":pd["swing_low"],"supply":pd["swing_high"],"pd_zones":pd,"mtf_matrix":mtf,"rationale":f"{interval.upper()} SMC Analysis complete.","candles":candles,"markers":markers,"lines":lines,"zones":fvg+ob,"reasons":sig["reasons"],"liquidity":liq_events,"generated_at":now_utc_iso(),"market_open":is_market_open()}

@app.route("/api")
def api_full():
    tf=normalize_tf(request.args.get("tf","1m"))
    try: return jsonify(smc_analysis(tf))
    except Exception as e: return jsonify({"status":"ERROR","reasons":[str(e)],"candles":[]}),500

@app.route("/api/tick")
def api_tick():
    tf=normalize_tf(request.args.get("tf","1m"))
    candles=get_gold_with_live(tf)
    if not candles: return jsonify({"market_open":is_market_open()})
    return jsonify({**candles[-1],"market_open":is_market_open()})

@app.route("/api/health")
def api_health():
    return jsonify({"status":"online","engine":"GoldSMC Pro 9TF Fixed","symbol":SYMBOL,"market_open":is_market_open(),"server_time":now_utc_iso()})

@app.route("/")
def index():
    return render_template("index.html")

if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
