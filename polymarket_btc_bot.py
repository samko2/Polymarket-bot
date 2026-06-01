import os, time, logging, requests, re, math
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.expanduser("~/Desktop/poly/.env"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("btc_bot")
API_KEY = os.getenv("POLYMARKET_API_KEY","")
API_KEY_ADDRESS = os.getenv("POLYMARKET_API_KEY_ADDRESS","")
MAX_BET_USDC = 2.0
MIN_EDGE = 0.025
POLL_INTERVAL = 15
MIN_VOLUME = 50000
DRY_RUN = False
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
EVENT_SLUG = "what-price-will-bitcoin-hit-before-2027"
END_DATE = datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc)
BTC_VOL = 0.60

def norm_cdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))

def bs_prob(S, K, T, sigma, direction="up"):
    if T <= 0 or S <= 0 or K <= 0:
        return None
    lnSK = math.log(S / K)
    sT = sigma * math.sqrt(T)
    if direction == "up":
        if K <= S:
            prob = 0.97
        else:
            prob = 2 * norm_cdf(lnSK / sT)
    else:
        if K >= S:
            prob = 0.97
        else:
            prob = 2 * norm_cdf(-lnSK / sT)
    return round(min(0.97, max(0.02, prob)), 4)

def get_btc_price():
    for fn in [
        lambda: requests.get("https://api.coingecko.com/api/v3/simple/price",params={"ids":"bitcoin","vs_currencies":"usd"},timeout=10).json()["bitcoin"]["usd"],
        lambda: float(requests.get("https://api.binance.com/api/v3/ticker/price",params={"symbol":"BTCUSDT"},timeout=10).json()["price"]),
        lambda: float(requests.get("https://api.coincap.io/v2/assets/bitcoin",timeout=10).json()["data"]["priceUsd"]),
    ]:
        try:
            return fn()
        except:
            continue
    log.warning("All price sources failed")
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
    now = datetime.now(timezone.utc)
    T = max(1/365, (END_DATE - now).days / 365)
    if up:
        strike = float(up.group(1).replace(",",""))
        return bs_prob(btc_price, strike, T, BTC_VOL, "up"), "UP", strike
    elif down:
        strike = float(down.group(1).replace(",",""))
        return bs_prob(btc_price, strike, T, BTC_VOL, "down"), "DOWN", strike
    return None, None, None

def place_order(token_id, side, price, size_usdc):
    import json
    size = round(size_usdc / price, 2)
    log.info(f"ORDER: {side} {size} shares @ ${price:.4f}")
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
    log.info("BTC Volatility Bot - " + ("LIVE" if not DRY_RUN else "DRY RUN"))
    log.info(f"edge={MIN_EDGE*100}% vol={BTC_VOL*100}% bet=${MAX_BET_USDC} poll={POLL_INTERVAL}s")
    while True:
        btc = get_btc_price()
        if not btc:
            time.sleep(POLL_INTERVAL)
            continue
        log.info(f"BTC=${btc:,.0f}")
        markets = get_markets()
        signals = 0
        for m in markets:
            question = m.get("question","")
            tokens = m.get("clobTokenIds") or []
            volume = float(m.get("volume","0") or 0)
            if not tokens or volume < MIN_VOLUME:
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
            log.info(f"{direction} ${strike:,.0f} vol=${volume:,.0f} bid={bid:.3f} ask={ask:.3f} fair={fair:.3f}")
            if edge_buy >= MIN_EDGE:
                log.info(f"*** BUY! edge={edge_buy:.3f} ***")
                place_order(token_id,"BUY",ask,MAX_BET_USDC)
                signals += 1
            elif edge_sell >= MIN_EDGE:
                log.info(f"*** SELL! edge={edge_sell:.3f} ***")
                place_order(token_id,"SELL",bid,MAX_BET_USDC)
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
