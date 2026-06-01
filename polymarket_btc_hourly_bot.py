import os, time, logging, requests, re
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.expanduser("~/Desktop/poly/.env"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("btc_hourly")
API_KEY = os.getenv("POLYMARKET_API_KEY","")
API_KEY_ADDRESS = os.getenv("POLYMARKET_API_KEY_ADDRESS","")
MAX_BET_USDC = 2.0
MIN_EDGE = 0.04
POLL_INTERVAL = 20
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
DRY_RUN = False

def get_btc_price():
    for fn in [
        lambda: float(requests.get("https://api.binance.com/api/v3/ticker/price",params={"symbol":"BTCUSDT"},timeout=10).json()["price"]),
        lambda: requests.get("https://api.coingecko.com/api/v3/simple/price",params={"ids":"bitcoin","vs_currencies":"usd"},timeout=10).json()["bitcoin"]["usd"],
    ]:
        try:
            return float(fn())
        except:
            continue
    return None

def get_momentum():
    try:
        resp = requests.get("https://api.binance.com/api/v3/klines",params={"symbol":"BTCUSDT","interval":"1m","limit":10},timeout=10)
        closes = [float(c[4]) for c in resp.json()]
        return (closes[-1] - closes[-5]) / closes[-5]
    except:
        return 0.0

def get_imbalance():
    try:
        resp = requests.get("https://api.binance.com/api/v3/depth",params={"symbol":"BTCUSDT","limit":10},timeout=10)
        data = resp.json()
        bid_vol = sum(float(b[1]) for b in data["bids"][:5])
        ask_vol = sum(float(a[1]) for a in data["asks"][:5])
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total else 0.0
    except:
        return 0.0

def get_current_slugs():
    et_offset = timedelta(hours=-4)
    et_now = datetime.now(timezone.utc) + et_offset
    months = ["january","february","march","april","may","june","july","august","september","october","november","december"]
    month = months[et_now.month-1]
    day = et_now.day
    year = et_now.year
    hour = et_now.hour
    ampm = "am" if hour < 12 else "pm"
    h = hour if hour <= 12 else hour - 12
    if h == 0: h = 12
    slugs = [
        f"bitcoin-up-or-down-{month}-{day}-{year}-{h}{ampm}-et",
        f"bitcoin-up-or-down-{month}-{day}-{year}-{h}-{ampm}-et",
        f"btc-updown-1h-{int(datetime.now(timezone.utc).timestamp()//3600*3600)}",
    ]
    return slugs

def get_hourly_markets():
    slugs = get_current_slugs()
    for slug in slugs:
        try:
            resp = requests.get(f"{GAMMA_HOST}/events",params={"slug":slug},timeout=15)
            data = resp.json()
            if data and isinstance(data, list):
                markets = data[0].get("markets",[])
                if markets:
                    log.info(f"Found market via slug: {slug}")
                    return markets
        except:
            continue
    return []

def get_order_book(token_id):
    try:
        resp = requests.get(f"{CLOB_HOST}/book",params={"token_id":token_id},timeout=10)
        data = resp.json()
        bids = data.get("bids",[])
        asks = data.get("asks",[])
        return float(bids[0]["price"]) if bids else 0.0, float(asks[0]["price"]) if asks else 1.0
    except:
        return 0.0, 1.0

def compute_fair(momentum, imbalance):
    base = 0.50
    mom = max(-0.15, min(0.15, momentum * 5))
    imb = max(-0.10, min(0.10, imbalance * 0.10))
    fair_up = round(max(0.25, min(0.75, base + mom + imb)), 4)
    return fair_up, round(1 - fair_up, 4)

def place_order(token_id, side, price, size_usdc):
    import json
    size = round(size_usdc / price, 2)
    log.info(f"ORDER: {side} {size} @ ${price:.4f}")
    if DRY_RUN:
        return
    body = json.dumps({"token_id":token_id,"price":str(price),"size":str(size),"side":side,"type":"GTC"})
    headers = {"POLY_ADDRESS":API_KEY_ADDRESS,"RELAYER_API_KEY":API_KEY,"Content-Type":"application/json"}
    try:
        resp = requests.post(f"{CLOB_HOST}/order",headers=headers,data=body,timeout=10)
        log.info(f"Order: {resp.json()}")
    except Exception as e:
        log.error(f"Order failed: {e}")

def run():
    log.info("BTC Hourly Bot - " + ("LIVE" if not DRY_RUN else "DRY RUN"))
    while True:
        btc = get_btc_price()
        if not btc:
            time.sleep(POLL_INTERVAL)
            continue
        momentum = get_momentum()
        imbalance = get_imbalance()
        fair_up, fair_down = compute_fair(momentum, imbalance)
        log.info(f"BTC=${btc:,.0f} mom={momentum*100:.2f}% imb={imbalance:.3f} fair_up={fair_up:.3f}")
        markets = get_hourly_markets()
        log.info(f"Found {len(markets)} hourly outcomes")
        signals = 0
        for m in markets:
            question = (m.get("question") or "").lower()
            tokens = m.get("clobTokenIds") or []
            if len(tokens) < 2:
                continue
            if "up" in question:
                up_id, down_id = tokens[0], tokens[1]
            else:
                up_id, down_id = tokens[1], tokens[0]
            for tid, fair, label in [(up_id, fair_up, "UP"), (down_id, fair_down, "DOWN")]:
                bid, ask = get_order_book(tid)
                if bid == 0.0 and ask == 1.0:
                    continue
                edge = fair - ask
                log.info(f"{label} bid={bid:.3f} ask={ask:.3f} fair={fair:.3f} edge={edge:.3f}")
                if edge >= MIN_EDGE:
                    log.info(f"*** BUY {label}! ***")
                    place_order(tid,"BUY",ask,MAX_BET_USDC)
                    signals += 1
        if signals == 0:
            log.info("No edge found.")
        log.info(f"Sleeping {POLL_INTERVAL}s...")
        try:
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Stopped.")
            break

if __name__ == "__main__":
    run()
