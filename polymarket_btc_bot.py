import os, time, logging, requests, re
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.expanduser("~/Desktop/poly/.env"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("btc_bot")
API_KEY = os.getenv("POLYMARKET_API_KEY","")
API_KEY_ADDRESS = os.getenv("POLYMARKET_API_KEY_ADDRESS","")
MAX_BET_USDC = 5.0
MIN_EDGE = 0.04
POLL_INTERVAL = 60
DRY_RUN = False
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
EVENT_SLUG = "what-price-will-bitcoin-hit-before-2027"

def get_btc_price():
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price",params={"ids":"bitcoin","vs_currencies":"usd"},timeout=10)
        return r.json()["bitcoin"]["usd"]
    except Exception as e:
        log.warning(f"Price fetch failed: {e}")
        return None

def get_markets():
    try:
        resp = requests.get(f"{GAMMA_HOST}/events",params={"slug":EVENT_SLUG},timeout=15)
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0].get("markets", [])
        return []
    except Exception as e:
        log.error(f"Market fetch failed: {e}")
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

def compute_fair(question, btc_price):
    up = re.search(r"[\u2191\+]\s*([0-9,]+)", question)
    down = re.search(r"[\u2193\-]\s*([0-9,]+)", question)
    if up:
        strike = float(up.group(1).replace(",",""))
        ratio = btc_price / strike
        fair = min(0.97, 0.70+(ratio-1.0)*0.2) if ratio >= 1.0 else max(0.02, ratio*0.70)
        return round(fair,4), "UP", strike
    elif down:
        strike = float(down.group(1).replace(",",""))
        ratio = btc_price / strike
        fair = min(0.97, 0.70+(1.0-ratio)*0.2) if ratio <= 1.0 else max(0.02, (1/ratio)*0.70)
        return round(fair,4), "DOWN", strike
    return None, None, None

def place_order(token_id, side, price, size_usdc):
    size = round(size_usdc / price, 2)
    log.info(f"{'[DRY RUN] ' if DRY_RUN else ''}ORDER: {side} {size} shares @ ${price:.4f}")
    if DRY_RUN:
        return
    import json
    body = json.dumps({"token_id":token_id,"price":str(price),"size":str(size),"side":side,"type":"GTC"})
    headers = {"POLY_ADDRESS":API_KEY_ADDRESS,"RELAYER_API_KEY":API_KEY,"Content-Type":"application/json"}
    try:
        resp = requests.post(f"{CLOB_HOST}/order",headers=headers,data=body,timeout=10)
        log.info(f"Order: {resp.json()}")
    except Exception as e:
        log.error(f"Order failed: {e}")

def run():
    log.info("BTC 2026 Price Bot - " + ("LIVE" if not DRY_RUN else "DRY RUN"))
    while True:
        btc = get_btc_price()
        if not btc:
            time.sleep(POLL_INTERVAL)
            continue
        log.info(f"BTC=${btc:,.0f}")
        markets = get_markets()
        log.info(f"Found {len(markets)} outcomes")
        for m in markets:
            question = m.get("question","")
            tokens = m.get("clobTokenIds") or []
            if not tokens:
                continue
            token_id = tokens[0]
            fair, direction, strike = compute_fair(question, btc)
            if not fair:
                continue
            bid, ask = get_order_book(token_id)
            if bid == 0.0 and ask == 1.0:
                continue
            edge_buy = fair - ask
            edge_sell = bid - fair
            log.info(f"{direction} ${strike:,.0f} bid={bid:.3f} ask={ask:.3f} fair={fair:.3f}")
            if edge_buy >= MIN_EDGE:
                log.info(f"BUY signal! edge={edge_buy:.3f}")
                place_order(token_id,"BUY",ask,MAX_BET_USDC)
            elif edge_sell >= MIN_EDGE:
                log.info(f"SELL signal! edge={edge_sell:.3f}")
                place_order(token_id,"SELL",bid,MAX_BET_USDC)
        log.info(f"Sleeping {POLL_INTERVAL}s...")
        try:
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Stopped.")
            break

if __name__ == "__main__":
    run()
