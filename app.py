import os
import time
import threading
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# ============================================================
# GOLD SMC PRO TERMINAL - WITH PRICE OFFSET CALIBRATION
# ============================================================

SYMBOL = "GC=F"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/" + SYMBOL

# 🔧 CALIBRATION OFFSET: Adjust this value (+ or -) to bridge the 
# gap between Yahoo Futures (GC=F) and your Broker's Spot Price (XAUUSD)
PRICE_OFFSET = 0.00  # e.g., if futures is 4311 and spot is 4268, set this to -43.00

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RANGE_MAP = {"1m": "1d", "5m": "5d", "15m": "5d", "1h": "1mo", "1d": "1y"}
VALID_TFS = set(RANGE_MAP.keys())

_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 15  # Cache expiration in seconds

def safe_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except Exception:
        return default

def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def normalize_tf(tf):
    tf = str(tf or "15m").lower().strip()
    aliases = {
        "1": "1m", "5": "5m", "15": "15m",
        "60": "1h", "1hr": "1h", "hour": "1h",
        "day": "1d", "daily": "1d"
    }
    tf = aliases.get(tf, tf)
    return tf if tf in VALID_TFS else "15m"


# ============================================================
# DATA ENGINE (YAHOO FINANCE FETCH + OFFSET APPLICATION)
# ============================================================

def get_gold(interval="15m"):
    interval = normalize_tf(interval)
    now = time.time()

    with _cache_lock:
        if interval in _cache:
            cached_candles, timestamp = _cache[interval]
            if now - timestamp < CACHE_TTL:
                return cached_candles

    tf_range = RANGE_MAP[interval]
    params = {
        "interval": interval,
        "range": tf_range,
        "includePrePost": "false",
        "events": "div,splits",
    }

    try:
        response = SESSION.get(YAHOO_URL, params=params, timeout=10)
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
            
            # Apply the offset to shift futures data down/up to match spot pricing
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


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

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
        tr = max(
            c["high"] - c["low"],
            abs(c["high"] - p["close"]),
            abs(c["low"] - p["close"])
        )
        true_ranges.append(tr)

    return round(sum(true_ranges[-period:]) / period, 4) if true_ranges else 0.0


# ============================================================
# SWING DETECTION & MTF BIAS
# ============================================================

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
        "1h": get_single_tf_bias("1h"),
        "1d": get_single_tf_bias("1d"),
    }


# ============================================================
# STRUCTURE & LIQUIDITY ENGINES
# ============================================================

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
        "state": previous_state,
        "event": None,
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


# ============================================================
# ZONES & ANALYSIS
# ============================================================

def is_zone_violated(zone, candles, created_index):
    if not candles:
        return False
    top = safe_float(zone.get("top"), None)
    bottom = safe_float(zone.get("bottom"), None)
    direction = str(zone.get("direction", "")).lower()

    if top is None or bottom is None:
        return True
    if created_index is None or created_index < 0:
        created_index = 0

    for j in range(created_index + 1, len(candles)):
        close = safe_float(candles[j].get("close"), None)
        if close is None:
            continue
        if direction == "bullish" and close < bottom:
            return True
        elif direction == "bearish" and close > top:
            return True
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
        if is_zone_violated(zone, candles, zone["createdIndex"]):
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
        if is_zone_violated(zone, candles, zone["createdIndex"]):
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
    return {
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
        "equilibrium": round(equilibrium, 2),
        "current_zone": zone,
    }


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


def smc_analysis(interval="15m"):
    interval = normalize_tf(interval)
    candles = get_gold(interval)

    if len(candles) < 40:
        return {
            "status": "INSUFFICIENT_DATA", "symbol": SYMBOL, "timeframe": interval,
            "price": 0, "signal": "WAIT", "score": 0, "confidence": 0, "rsi": 50,
            "candles": [], "markers": [], "lines": [], "zones": [],
            "reasons": ["Insufficient candle history fetched."], "mtf_matrix": {}, "pd_zones": {},
        }

    price = candles[-1]["close"]
    mtf = build_mtf_matrix()
    structure = detect_structure(candles)
    liquidity_lines, liquidity_events = detect_liquidity(candles)
    fvg_zones = detect_fvgs(candles)
    ob_zones = detect_order_blocks(candles)
    pd = calculate_pd(candles)
    signal_data = build_signal(candles, interval, mtf, structure, liquidity_events, fvg_zones, ob_zones, pd)

    lines = []
    if structure["level"] is not None:
        lines.append({
            "price": structure["level"],
            "color": "#00ff88" if structure["state"] == "BULLISH" else "#ff4444",
            "title": structure["event"] or "STRUCTURE",
            "lineStyle": 0, "layer": "bos_choch",
        })

    lines.extend(liquidity_lines)
    lines.append({"price": pd["swing_low"], "color": "#22c55e", "title": "DEMAND", "lineStyle": 1, "layer": "structure"})
    lines.append({"price": pd["swing_high"], "color": "#ef4444", "title": "SUPPLY", "lineStyle": 1, "layer": "structure"})

    markers = []
    if structure["event"]:
        markers.append({
            "time": candles[-1]["time"],
            "position": "belowBar" if structure["state"] == "BULLISH" else "aboveBar",
            "color": "#ffffff" if structure["event"] == "CHoCH" else ("#00ff88" if structure["state"] == "BULLISH" else "#ff4444"),
            "shape": "arrowUp" if structure["state"] == "BULLISH" else "arrowDown",
            "text": f"{structure['event']} ↑" if structure["state"] == "BULLISH" else f"{structure['event']} ↓",
            "layer": "bos_choch",
        })

    zones = fvg_zones + ob_zones

    return {
        "status": "OK", "symbol": SYMBOL, "timeframe": interval, "price": price,
        "signal": signal_data["signal"], "score": signal_data["score"], "confidence": signal_data["confidence"],
        "rsi": signal_data["rsi"], "atr": signal_data["atr"], "entry": signal_data["entry"],
        "stopLoss": signal_data["stopLoss"], "takeProfit1": signal_data["takeProfit1"],
        "takeProfit2": signal_data["takeProfit2"], "takeProfit3": signal_data["takeProfit3"],
        "htf_bias": mtf["1d"] if interval == "1h" else mtf["1h"],
        "htf_tf": "1D" if interval == "1h" else "1H",
        "structure": structure, "demand": pd["swing_low"], "supply": pd["swing_high"],
        "pd_zones": pd, "mtf_matrix": mtf, "rationale": f"{interval.upper()} SMC Analysis complete.",
        "candles": candles, "markers": markers, "lines": lines, "zones": zones,
        "reasons": signal_data["reasons"], "liquidity": liquidity_events, "generated_at": now_utc_iso(),
    }


@app.route("/api")
def api_full():
    tf = normalize_tf(request.args.get("tf", "15m"))
    try:
        return jsonify(smc_analysis(tf))
    except Exception as exc:
        return jsonify({"status": "ERROR", "reasons": [str(exc)]}), 500


@app.route("/api/tick")
def api_tick():
    tf = normalize_tf(request.args.get("tf", "15m"))
    candles = get_gold(tf)
    if not candles:
        return jsonify({})
    return jsonify(candles[-1])


@app.route("/api/health")
def api_health():
    return jsonify({
        "status": "online",
        "engine": "GoldSMC Pro Pydroid Edition",
        "symbol": SYMBOL,
        "server_time": now_utc_iso(),
    })


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    print("=" * 60)
    print(" GOLD SMC PRO TERMINAL - PYDROID EDITION")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=True)
