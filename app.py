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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ONLY 6 TFS - 1 SEC REFRESH
RANGE_MAP = {"1m": "1d", "5m": "5d", "15m": "5d", "30m": "5d", "1h": "1mo", "1d": "1y"}
VALID_TFS = {"1m", "5m", "15m", "30m", "1h", "1d"}

YAHOO_INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "1d": "1d"
}

_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 1

def safe_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except Exception:
        return default

def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def is_market_open():
    now = datetime.now(timezone.utc)
    d, h = now.weekday(), now.hour
    if d == 5:
        return False
    if d == 4 and h >= 21:
        return False
    if d == 6 and h < 22:
        return False
    return True

def normalize_tf(tf):
    tf = str(tf or "15m").lower().strip()
    aliases = {
        "1": "1m", "1m": "1m",
        "5": "5m", "5m": "5m",
        "15": "15m", "15m": "15m",
        "30": "30m", "30m": "30m", "30min": "30m",
        "60": "1h", "1h": "1h", "1hr": "1h", "hour": "1h",
        "day": "1d", "daily": "1d", "1d": "1d"
    }
    tf = aliases.get(tf, tf)
    return tf if tf in VALID_TFS else "15m"

def get_yahoo_raw_close():
    try:
        params = {"interval": "1m", "range": "1d", "includePrePost": "false"}
        r = SESSION.get(YAHOO_URL, params=params, timeout=4)
        r.raise_for_status()
        payload = r.json()
        result = payload["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            return float(closes[-1])
    except Exception as e:
        print(f"[RAW YAHOO ERROR] {e}")
    return None

def get_spot_live():
    try:
        r = SESSION.get("https://api.gold-api.com/price/XAU", timeout=2)
        if r.status_code == 200:
            j = r.json()
            p = float(j.get('price', 0))
            if p > 1000:
                return p
    except Exception as e:
        print(f"[SPOT ERROR] {e}")
    return None

def auto_sync_loop():
    global PRICE_OFFSET
    while True:
        try:
            if is_market_open():
                yahoo_raw = get_yahoo_raw_close()
                spot = get_spot_live()
                if yahoo_raw and spot:
                    new_offset = spot - yahoo_raw
                    if -70 < new_offset < -5:
                        PRICE_OFFSET = round(new_offset, 2)
                        print(f"[1SEC SYNC] JustMarkets: {spot} | offset: {PRICE_OFFSET}")
            time.sleep(2)
        except Exception as e:
            print(f"[AUTO-SYNC ERROR] {e}")
            time.sleep(2)

threading.Thread(target=auto_sync_loop, daemon=True).start()

def get_gold(interval="15m"):
    interval = normalize_tf(interval)
    now = time.time()
    with _cache_lock:
        if interval in _cache:
            cached_candles, timestamp = _cache[interval]
            if now - timestamp < CACHE_TTL:
                return cached_candles
    tf_range = RANGE_MAP[interval]
    yahoo_interval = YAHOO_INTERVAL_MAP.get(interval, interval)
    params = {
        "interval": yahoo_interval,
        "range": tf_range,
        "includePrePost": "false",
        "events": "div,splits",
    }
    try:
        response = SESSION.get(YAHOO_URL, params=params, timeout=5)
        response.raise_for_status()
        payload = response.json()
        chart = payload.get("chart", {})
        results = chart.get("result")
        if not results:
            return []
        result = results[0]
        timestamps = result.get("timestamp", [])
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        candles = []
        length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
        for i in range(length):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            if o is None or h is None or l is None or c is None:
                continue
            candles.append({
                "time": int(timestamps[i]),
                "open": round(float(o) + PRICE_OFFSET, 2),
                "high": round(float(h) + PRICE_OFFSET, 2),
                "low": round(float(l) + PRICE_OFFSET, 2),
                "close": round(float(c) + PRICE_OFFSET, 2),
                "volume": int(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0,
            })
        result_candles = candles[-250:]
        with _cache_lock:
            _cache[interval] = (result_candles, time.time())
        return result_candles
    except Exception as exc:
        print(f"[DATA ERROR] {interval}: {exc}")
        return []

_last_live_price = 0
_last_live_fetch = 0

def get_oanda_live():
    global _last_live_price, _last_live_fetch
    if time.time() - _last_live_fetch < 0.8 and _last_live_price:
        return _last_live_price
    try:
        r = SESSION.get("https://api.gold-api.com/price/XAU", timeout=2)
        if r.status_code == 200:
            j = r.json()
            p = float(j.get('price', 0))
            if p > 1000:
                _last_live_price = p
                _last_live_fetch = time.time()
                return _last_live_price
    except Exception as e:
        print(f"[LIVE ERROR] {e}")
    return _last_live_price

def get_gold_with_live(interval="15m"):
    candles = get_gold(interval)
    if not candles:
        return candles
    if not is_market_open():
        return candles
    live = get_oanda_live()
    if live and live > 100:
        last = candles[-1]
        last["close"] = round(live, 2)
        last["high"] = round(max(last["high"], live), 2)
        last["low"] = round(min(last["low"], live), 2)
    return candles

def calculate_rsi(closes, period=14):
    if len(closes) <= period:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    true_ranges = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        tr = max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"]))
        true_ranges.append(tr)
    return round(sum(true_ranges[-period:]) / period, 4) if true_ranges else 0.0)

def find_swing_highs(candles, left=2, right=2):
    result = []
    for i in range(left, len(candles) - right):
        high = candles[i]["high"]
        left_highs = [candles[j]["high"] for j in range(i - left, i)]
        right_highs = [candles[j]["high"] for j in range(i + 1, i + right + 1)]
        if high >= max(left_highs) and high > max(right_highs):
            result.append(i)
    return result

def find_swing_lows(candles, left=2, right=2):
    result = []
    for i in range(left, len(candles) - right):
        low = candles[i]["low"]
        left_lows = [candles[j]["low"] for j in range(i - left, i)]
        right_lows = [candles[j]["low"] for j in range(i + 1, i + right + 1)]
        if low <= min(left_lows) and low < min(right_lows):
            result.append(i)
    return result

def get_single_tf_bias(tf):
    candles = get_gold(tf)
    if len(candles) < 30:
        return "NEUTRAL"
    swing_highs = find_swing_highs(candles, 2, 2)
    swing_lows = find_swing_lows(candles, 2, 2)
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "NEUTRAL"
    last_high = candles[swing_highs[-1]]["high"]
    previous_high = candles[swing_highs[-2]]["high"]
    last_low = candles[swing_lows[-1]]["low"]
    previous_low = candles[swing_lows[-2]]["low"]
    price = candles[-1]["close"]
    if last_high > previous_high and last_low > previous_low and price >= last_low:
        return "BULLISH"
    if last_high < previous_high and last_low < previous_low and price <= last_high:
        return "BEARISH"
    if price > last_high:
        return "BULLISH"
    if price < last_low:
        return "BEARISH"
    return "NEUTRAL"

def build_mtf_matrix():
    return {
        "1m": get_single_tf_bias("1m"),
        "5m": get_single_tf_bias("5m"),
        "15m": get_single_tf_bias("15m"),
        "30m": get_single_tf_bias("30m"),
        "1h": get_single_tf_bias("1h"),
        "1d": get_single_tf_bias("1d"),
    }

def detect_structure(candles):
    if len(candles) < 20:
        return {"state": "NEUTRAL", "event": None, "level": None, "index": None}
    swing_highs = find_swing_highs(candles, 2, 2)
    swing_lows = find_swing_lows(candles, 2, 2)
    if not swing_highs or not swing_lows:
        return {"state": "NEUTRAL", "event": None, "level": None, "index": None}
    last_high_idx = swing_highs[-1]
    last_low_idx = swing_lows[-1]
    last_high = candles[last_high_idx]["high"]
    last_low = candles[last_low_idx]["low"]
    previous_state = "NEUTRAL"
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        h1 = candles[swing_highs[-2]]["high"]
        h2 = candles[swing_highs[-1]]["high"]
        l1 = candles[swing_lows[-2]]["low"]
        l2 = candles[swing_lows[-1]]["low"]
        if h2 > h1 and l2 > l1:
            previous_state = "BULLISH"
        elif h2 < h1 and l2 < l1:
            previous_state = "BEARISH"
    close = candles[-1]["close"]
    if close > last_high:
        event = "CHoCH" if previous_state == "BEARISH" else "BOS"
        return {"state": "BULLISH", "event": event, "level": last_high, "index": last_high_idx}
    if close < last_low:
        event = "CHoCH" if previous_state == "BULLISH" else "BOS"
        return {"state": "BEARISH", "event": event, "level": last_low, "index": last_low_idx}
    return {
        "state": previous_state, "event": None,
        "level": last_high if previous_state == "BULLISH" else last_low if previous_state == "BEARISH" else None,
        "index": last_high_idx if previous_state == "BULLISH" else last_low_idx if previous_state == "BEARISH" else None,
    }

def detect_liquidity(candles, tolerance_factor=0.12):
    if len(candles) < 30:
        return [], []
    atr = calculate_atr(candles) or 1.0
    tolerance = atr * tolerance_factor
    swing_highs = find_swing_highs(candles, 2, 2)
    swing_lows = find_swing_lows(candles, 2, 2)
    lines, events = [], []
    if len(swing_highs) >= 2:
        a, b = swing_highs[-1], swing_highs[-2]
        p1, p2 = candles[a]["high"], candles[b]["high"]
        if abs(p1 - p2) <= tolerance:
            level = round((p1 + p2) / 2, 2)
            swept = candles[-1]["high"] > level
            if not swept:
                lines.append({"price": level, "color": "#00e5ff", "title": "EQUAL HIGH", "lineStyle": 2, "layer": "liquidity", "active": True})
            else:
                events.append({"type": "BUY_SIDE_SWEEP", "price": level, "time": candles[-1]["time"], "label": "BUY-SIDE SWEEP"})
    if len(swing_lows) >= 2:
        a, b = swing_lows[-1], swing_lows[-2]
        p1, p2 = candles[a]["low"], candles[b]["low"]
        if abs(p1 - p2) <= tolerance:
            level = round((p1 + p2) / 2, 2)
            swept = candles[-1]["low"] < level
            if not swept:
                lines.append({"price": level, "color": "#00e5ff", "title": "EQUAL LOW", "lineStyle": 2, "layer": "liquidity", "active": True})
            else:
                events.append({"type": "SELL_SIDE_SWEEP", "price": level, "time": candles[-1]["time"], "label": "SELL-SIDE SWEEP"})
    return lines, events

# === UPDATED: 3 CANDLE CONFIRMATION ===
def is_zone_violated(zone, candles, created_index, confirm_bars=3):
    if not candles:
        return False
    top = safe_float(zone.get("top"), None)
    bottom = safe_float(zone.get("bottom"), None)
    direction = str(zone.get("direction", "")).lower()
    if top is None or bottom is None:
        return True
    if created_index is None or created_index < 0:
        created_index = 0
    # Need at least confirm_bars after creation
    if len(candles) - (created_index + 1) < confirm_bars:
        return False
    # Last 3 closes must ALL break zone
    last_closes = [safe_float(candles[j].get("close"), None) for j in range(len(candles)-confirm_bars, len(candles))]
    if direction == "bullish":
        return all(c is not None and c < bottom for c in last_closes)
    elif direction == "bearish":
        return all(c is not None and c > top for c in last_closes)
    return False

def detect_fvgs(candles, max_zones=6):
    active_zones = []
    if len(candles) < 5:
        return active_zones
    atr = calculate_atr(candles) or 1.0
    minimum_gap = atr * 0.10
    start = max(2, len(candles) - 80)
    candidates = []
    for i in range(start, len(candles)):
        left, middle, right = candles[i - 2], candles[i - 1], candles[i]
        if right["low"] > left["high"]:
            gap = right["low"] - left["high"]
            if gap >= minimum_gap:
                candidates.append({
                    "type": "Bullish FVG", "label": "BULLISH FVG", "direction": "bullish",
                    "top": round(right["low"], 2), "bottom": round(left["high"], 2),
                    "timeStart": left["time"], "createdTime": right["time"], "createdIndex": i,
                    "color": "rgba(0,255,136,0.16)", "borderColor": "#00ff88", "layer": "fvg", "status": "active", "valid": True
                })
        elif right["high"] < left["low"]:
            gap = left["low"] - right["high"]
            if gap >= minimum_gap:
                candidates.append({
                    "type": "Bearish FVG", "label": "BEARISH FVG", "direction": "bearish",
                    "top": round(left["low"], 2), "bottom": round(right["high"], 2),
                    "timeStart": left["time"], "createdTime": right["time"], "createdIndex": i,
                    "color": "rgba(255,68,68,0.16)", "borderColor": "#ff4444", "layer": "fvg", "status": "active", "valid": True
                })
    for zone in candidates:
        if is_zone_violated(zone, candles, zone["createdIndex"], confirm_bars=3):
            continue
        zone.pop("createdIndex", None)
        active_zones.append(zone)
    return active_zones[-max_zones:]

def detect_order_blocks(candles, max_zones=6):
    active_zones = []
    if len(candles) < 10:
        return active_zones
    atr = calculate_atr(candles) or 1.0
    avg_body = sum(abs(c["close"] - c["open"]) for c in candles[-20:]) / min(20, len(candles))
    start = max(2, len(candles) - 80)
    candidates = []
    for i in range(start, len(candles) - 2):
        current = candles[i]
        next_candle = candles[i + 1]
        next_body = abs(next_candle["close"] - next_candle["open"])
        displacement = next_body > avg_body * 1.25 and next_body > atr * 0.35
        if not displacement:
            continue
        if current["close"] < current["open"] and next_candle["close"] > current["high"]:
            candidates.append({
                "type": "Bullish Order Block", "label": "BULLISH OB", "direction": "bullish",
                "top": round(current["high"], 2), "bottom": round(current["low"], 2),
                "timeStart": current["time"], "createdTime": next_candle["time"], "createdIndex": i + 1,
                "color": "rgba(34,197,94,0.18)", "borderColor": "#22c55e", "layer": "ob", "status": "active", "valid": True
            })
        elif current["close"] > current["open"] and next_candle["close"] < current["low"]:
            candidates.append({
                "type": "Bearish Order Block", "label": "BEARISH OB", "direction": "bearish",
                "top": round(current["high"], 2), "bottom": round(current["low"], 2),
                "timeStart": current["time"], "createdTime": next_candle["time"], "createdIndex": i + 1,
                "color": "rgba(239,68,68,0.18)", "borderColor": "#ef4444", "layer": "ob", "status": "active", "valid": True
            })
    for zone in candidates:
        if is_zone_violated(zone, candles, zone["createdIndex"], confirm_bars=3):
            continue
        zone.pop("createdIndex", None)
        active_zones.append(zone)
    return active_zones[-max_zones:]

def calculate_pd(candles):
    lookback = min(80, len(candles))
    recent = candles[-lookback:]
    swing_high = max(c["high"] for c in recent)
    swing_low = min(c["low"] for c in recent)
    equilibrium = (swing_high + swing_low) / 2
    price = candles[-1]["close"]
    zone = "PREMIUM" if price > equilibrium else "DISCOUNT"
    return {"swing_high": round(swing_high, 2), "swing_low": round(swing_low, 2), "equilibrium": round(equilibrium, 2), "current_zone": zone}

def build_signal(candles, interval, mtf, structure, liquidity_events, fvg_zones, ob_zones, pd):
    price = candles[-1]["close"]
    rsi = calculate_rsi([c["close"] for c in candles])
    score = 0
    reasons = []
    bullish_count = sum(1 for value in mtf.values() if value == "BULLISH")
    bearish_count = sum(1 for value in mtf.values() if value == "BEARISH")
    if bullish_count >= 3:
        score += 2
        reasons.append(f"MTF alignment: {bullish_count}/5 bullish")
    elif bearish_count >= 3:
        score -= 2
        reasons.append(f"MTF alignment: {bearish_count}/5 bearish")
    if structure["state"] == "BULLISH":
        score += 2
        reasons.append(f"Bullish {structure['event']}" if structure["event"] else "Bullish market structure")
    elif structure["state"] == "BEARISH":
        score -= 2
        reasons.append(f"Bearish {structure['event']}" if structure["event"] else "Bearish market structure")
    for event in liquidity_events:
        if event["type"] == "SELL_SIDE_SWEEP":
            score += 2
            reasons.append("Sell-side liquidity swept")
        elif event["type"] == "BUY_SIDE_SWEEP":
            score -= 2
            reasons.append("Buy-side liquidity swept")
    if score >= 6:
        signal = "STRONG BUY"
    elif score >= 3:
        signal = "BUY"
    elif score <= -6:
        signal = "STRONG SELL"
    elif score <= -3:
        signal = "SELL"
    else:
        signal = "WAIT"
    atr = calculate_atr(candles) or max(price * 0.001, 0.10)
    entry = round(price, 2)
    sl_distance = atr * 1.5
    if signal in ("BUY", "STRONG BUY"):
        stop_loss = round(entry - sl_distance, 2)
        risk = entry - stop_loss
        tp1, tp2, tp3 = round(entry + risk, 2), round(entry + risk * 2, 2), round(entry + risk * 3, 2)
    elif signal in ("SELL", "STRONG SELL"):
        stop_loss = round(entry + sl_distance, 2)
        risk = stop_loss - entry
        tp1, tp2, tp3 = round(entry - risk, 2), round(entry - risk * 2, 2), round(entry - risk * 3, 2)
    else:
        stop_loss, tp1, tp2, tp3 = None, None, None, None
    confidence = round(clamp(50 + abs(score) * 5, 50, 95))
    return {
        "signal": signal, "score": score, "confidence": confidence, "rsi": rsi,
        "entry": entry, "stopLoss": stop_loss, "takeProfit1": tp1, "takeProfit2": tp2, "takeProfit3": tp3,
        "reasons": reasons, "bullish_mtf": bullish_count, "bearish_mtf": bearish_count, "atr": round(atr, 4),
    }

# === UPDATED: STABLE SMC ANALYSIS ===
def smc_analysis(interval="15m"):
    interval = normalize_tf(interval)
    static_candles = get_gold(interval)
    live_candles = get_gold_with_live(interval)
    if len(static_candles) < 40:
        return {
            "status": "INSUFFICIENT_DATA", "symbol": SYMBOL, "timeframe": interval,
            "price": live_candles[-1]["close"] if live_candles else 0, "signal": "WAIT", "score": 0, "confidence": 0, "rsi": 50,
            "candles": live_candles, "markers": [], "lines": [], "zones": [],
            "reasons": ["Insufficient candle history fetched."], "mtf_matrix": {}, "pd_zones": {},
        }
    price = live_candles[-1]["close"] if live_candles else static_candles[-1]["close"]
    mtf = build_mtf_matrix()
    structure = detect_structure(static_candles)
    liquidity_lines, liquidity_events = detect_liquidity(static_candles)
    fvg_zones = detect_fvgs(static_candles)
    ob_zones = detect_order_blocks(static_candles)
    pd = calculate_pd(static_candles)
    signal_data = build_signal(live_candles, interval, mtf, structure, liquidity_events, fvg_zones, ob_zones, pd)
    lines = []
    if structure["level"] is not None:
        lines.append({
            "price": structure["level"],
            "color": "#00ff88" if structure["state"] == "BULLISH" else "#ff4444",
            "title": structure["event"] or structure["state"],
            "lineStyle": 0, "layer": "structure", "active": True
        })
    lines.extend(liquidity_lines)
    zones = []
    zones.extend(fvg_zones)
    zones.extend(ob_zones)
    markers = []
    if structure["event"]:
        markers.append({"time": static_candles[structure["index"]]["time"] if structure["index"] is not None else static_candles[-1]["time"], "label": structure["event"]})
    for ev in liquidity_events:
        markers.append({"time": ev["time"], "label": ev["label"]})
    return {
        "status": "OK", "symbol": SYMBOL, "timeframe": interval, "price": price,
        "signal": signal_data["signal"], "score": signal_data["score"], "confidence": signal_data["confidence"],
        "rsi": signal_data["rsi"], "entry": signal_data["entry"], "stopLoss": signal_data["stopLoss"],
        "takeProfit1": signal_data["takeProfit1"], "takeProfit2": signal_data["takeProfit2"], "takeProfit3": signal_data["takeProfit3"],
        "candles": static_candles if not live_candles else live_candles, "markers": markers, "lines": lines, "zones": zones,
        "reasons": signal_data["reasons"], "mtf_matrix": mtf, "pd_zones": pd,
        "atr": signal_data["atr"], "bullish_mtf": signal_data["bullish_mtf"], "bearish_mtf": signal_data["bearish_mtf"],
        "market_open": is_market_open(), "timestamp": now_utc_iso(), "offset": PRICE_OFFSET
    }

_news_cache = {"data": [], "time": 0}
@app.route("/api/news")
def api_news():
    now = time.time()
    if now - _news_cache["time"] < 300:
        return jsonify(_news_cache["data"])
    try:
        r = SESSION.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=2)
        if r.status_code == 200:
            data = r.json()
            high = []
            for ev in data[:20]:
                if ev.get("impact") == "High" and ev.get("country") in ["USD", "US"]:
                    high.append({"title": ev.get("title", ""), "date": ev.get("date", ""), "country": ev.get("country", "USD"), "impact": "High"})
            _news_cache["data"] = high[:5]
            _news_cache["time"] = now
            return jsonify(high[:5])
    except Exception as e:
        print(f"[NEWS] {e}")
    return jsonify(_news_cache["data"])

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/candles")
def api_candles():
    tf = request.args.get("tf", "15m")
    candles = get_gold_with_live(tf)
    return jsonify(candles)

@app.route("/api/analysis")
def api_analysis():
    tf = request.args.get("tf", "15m")
    data = smc_analysis(tf)
    return jsonify(data)

@app.route("/api/price")
def api_price():
    tf = request.args.get("tf", "15m")
    candles = get_gold_with_live(tf)
    if not candles:
        return jsonify({"price": 0, "market_open": is_market_open(), "offset": PRICE_OFFSET})
    return jsonify({"price": candles[-1]["close"], "market_open": is_market_open(), "timestamp": now_utc_iso(), "offset": PRICE_OFFSET})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
