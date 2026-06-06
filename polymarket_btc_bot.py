"""
Polymarket Ultimate Volatility Bot  —  Production-Ready
════════════════════════════════════════════════════════
Assets   : BTC, ETH, SOL
Pricing  : European digital (on-date) + barrier/touch (hit-before-date)
Orders   : GTC limit orders placed at fair − edge_buffer; peer-to-peer fills
Tracking : Fill detection via order polling; Telegram on every fill
Sizing   : Live USDC balance → quarter-Kelly
Safety   : Open-order dedup, stale order refresh, 6h hard reset
Reports  : Daily Telegram P&L summary
"""

import os
import re
import math
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field

import requests
from dotenv import load_dotenv

from py_clob_client_v2 import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    OpenOrderParams,
    OrderArgs,
    OrderPayload,
    OrderType,
    PartialCreateOrderOptions,
    Side,
    TradeParams,
)

load_dotenv(dotenv_path=os.path.expanduser("~/Desktop/poly/.env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.expanduser("~/Desktop/poly/bot.log")),
    ],
)
log = logging.getLogger("poly_bot")

# ── Exchange ───────────────────────────────────────────────────────────────────
CLOB_HOST  = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID   = 137
TICK_SIZE  = "0.01"

# ── Bot config ─────────────────────────────────────────────────────────────────
DRY_RUN             = True    # ← flip to False for live trading
POLL_INTERVAL       = 20      # seconds between scans
EDGE_BUFFER         = 0.05    # how far below fair we place our limit (5%)
MIN_EDGE            = 0.02    # minimum net edge after fee to bother placing
TAKER_FEE           = 0.02    # Polymarket taker fee on winnings
KELLY_FRACTION      = 0.25    # quarter-Kelly
MAX_BET_USDC        = 5.0     # hard cap per order regardless of Kelly
MIN_BET_USDC        = 0.50    # skip orders below this size
MIN_VOLUME          = 5_000   # min lifetime volume to consider a market
TRADED_RESET_HOURS  = 6       # cancel stale orders + full reset every N hours
STALE_FAIR_DRIFT    = 0.12    # cancel + re-place if fair moved >12% from placement
PNL_REPORT_HOURS    = 24      # Telegram P&L summary every N hours

# ── Telegram ───────────────────────────────────────────────────────────────────
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")


def tg(msg: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception as e:
        log.warning(f"Telegram failed: {e}")


# ── Asset config ───────────────────────────────────────────────────────────────
ASSETS = {
    "BTC": {
        "vol":            0.60,
        "drift":          0.20,
        "binance_symbol": "BTC",
        "coingecko_id":   "bitcoin",
        "keywords":       ["bitcoin", "btc"],
        "daily_names":    ["bitcoin", "btc"],
        "slugs": [
            "what-price-will-bitcoin-hit-before-2027",
            "what-price-will-bitcoin-hit-in-june-2026",
        ],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "ETH": {
        "vol":            0.65,
        "drift":          0.15,
        "binance_symbol": "ETH",
        "coingecko_id":   "ethereum",
        "keywords":       ["ethereum", "eth"],
        "daily_names":    ["ethereum", "eth"],
        "slugs": [
            "what-price-will-ethereum-hit-before-2027",
            "what-price-will-eth-reach-before-2027",
            "what-price-will-ethereum-hit-in-june-2026",
            "ethereum-price-end-of-hour",
        ],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "SOL": {
        "vol":            0.85,
        "drift":          0.25,
        "binance_symbol": "SOL",
        "coingecko_id":   "solana",
        "keywords":       ["solana", "sol"],
        "daily_names":    ["solana", "sol"],
        "slugs": [
            "what-price-will-solana-hit-before-2027",
            "what-price-will-sol-reach-before-2027",
        ],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
}

# ── Caches ─────────────────────────────────────────────────────────────────────
_slug_cache:  dict = {}   # slug  → (ts, [markets])
_vol_cache:   dict = {}   # sym   → (ts, vol)
_drift_cache: dict = {}   # sym   → (ts, drift)
SLUG_TTL = 300            # seconds


# ══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGER — tracks open orders, detects fills, refreshes stale orders
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TrackedOrder:
    order_id:   str
    token_id:   str
    label:      str
    limit:      float
    size_usdc:  float
    fair:       float        # fair value when we placed the order
    placed_at:  float = field(default_factory=time.time)


class OrderManager:
    def __init__(self):
        self.orders:        dict[str, TrackedOrder] = {}   # order_id → TrackedOrder
        self.open_token_ids: set[str] = set()              # token_ids with live orders
        self.fills_usdc:    float = 0.0
        self.fill_count:    int   = 0
        self.order_count:   int   = 0

    def record(self, order_id: str, token_id: str, label: str,
               limit: float, size_usdc: float, fair: float) -> None:
        self.orders[order_id]   = TrackedOrder(order_id, token_id, label, limit, size_usdc, fair)
        self.open_token_ids.add(token_id)
        self.order_count       += 1

    def has_open_order(self, token_id: str) -> bool:
        return token_id in self.open_token_ids

    def refresh_from_api(self, client: ClobClient) -> list[dict]:
        """
        Poll open orders from API.  Returns list of fill-event dicts.
        Also refreshes open_token_ids so has_open_order() stays accurate.
        """
        if DRY_RUN:
            return []

        fill_events = []
        try:
            open_orders = client.get_open_orders() or []
            open_ids    = {o.get("id") or o.get("orderID") for o in open_orders}
            open_tokens = {o.get("asset_id") for o in open_orders if o.get("asset_id")}
            self.open_token_ids = open_tokens

            gone_ids = [oid for oid in self.orders if oid not in open_ids]
            for oid in gone_ids:
                tracked = self.orders.pop(oid)
                try:
                    detail       = client.get_order(oid) or {}
                    size_matched = float(detail.get("size_matched") or 0)
                    status       = detail.get("status", "UNKNOWN")
                    if size_matched > 0.001:
                        filled_usdc = round(size_matched * tracked.limit, 2)
                        self.fills_usdc += filled_usdc
                        self.fill_count += 1
                        fill_events.append({
                            "label":       tracked.label,
                            "limit":       tracked.limit,
                            "fair":        tracked.fair,
                            "size_matched": size_matched,
                            "filled_usdc": filled_usdc,
                            "edge_at_fill": tracked.fair * (1 - TAKER_FEE) - tracked.limit,
                        })
                        log.info(f"  ✓ FILL: {tracked.label} {size_matched:.2f} shares @ {tracked.limit:.3f} = ${filled_usdc:.2f}")
                    else:
                        log.info(f"  Order {oid[:10]} gone (status={status}, no fill)")
                except Exception as e:
                    log.debug(f"  get_order {oid}: {e}")

        except Exception as e:
            log.warning(f"refresh_from_api failed: {e}")

        return fill_events

    def cancel_stale(self, client: ClobClient, current_fairs: dict[str, float]) -> None:
        """
        Cancel open orders where fair value has drifted >STALE_FAIR_DRIFT from placement.
        current_fairs: token_id → current fair value
        """
        if DRY_RUN:
            return
        for oid, tracked in list(self.orders.items()):
            curr_fair = current_fairs.get(tracked.token_id)
            if curr_fair is None:
                continue
            drift = abs(curr_fair - tracked.fair)
            if drift >= STALE_FAIR_DRIFT:
                log.info(f"  Cancelling stale order {oid[:10]} (fair drifted {drift:.2f})")
                try:
                    client.cancel_order(OrderPayload(orderID=oid))
                    self.orders.pop(oid, None)
                    self.open_token_ids.discard(tracked.token_id)
                except Exception as e:
                    log.warning(f"  Cancel {oid[:10]} failed: {e}")

    def cancel_all(self, client: ClobClient) -> None:
        if DRY_RUN:
            return
        try:
            client.cancel_all()
            log.info(f"  Cancelled all open orders ({len(self.orders)} tracked)")
        except Exception as e:
            log.warning(f"  cancel_all failed: {e}")
        self.orders.clear()
        self.open_token_ids.clear()

    def summary(self) -> str:
        return (f"Orders placed: {self.order_count}  "
                f"Fills: {self.fill_count}  "
                f"USDC filled: ${self.fills_usdc:.2f}")


# ══════════════════════════════════════════════════════════════════════════════
# LIVE USDC BALANCE
# ══════════════════════════════════════════════════════════════════════════════

_balance_cache: list = [0.0, 0.0]   # [value, fetched_at]


def get_usdc_balance(client: ClobClient) -> float:
    """Fetch live USDC balance from Polymarket CLOB. Cached for 5 min."""
    now = time.time()
    if _balance_cache[1] and now - _balance_cache[1] < 300:
        return _balance_cache[0]
    try:
        resp    = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        balance = float(resp.get("balance") or resp.get("available") or 0)
        _balance_cache[0] = balance
        _balance_cache[1] = now
        log.info(f"  USDC balance: ${balance:.2f}")
        return balance
    except Exception as e:
        log.debug(f"Balance fetch failed: {e}")
        return _balance_cache[0] or MAX_BET_USDC * 10   # fallback


# ══════════════════════════════════════════════════════════════════════════════
# LIVE VOL + DRIFT (Binance 30-day realized)
# ══════════════════════════════════════════════════════════════════════════════

def get_live_vol(symbol: str, fallback: float) -> float:
    now = time.time()
    if symbol in _vol_cache and now - _vol_cache[symbol][0] < 3600:
        return _vol_cache[symbol][1]
    try:
        closes  = [float(c[4]) for c in requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol + "USDT", "interval": "1d", "limit": 31},
            timeout=10,
        ).json()]
        returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
        vol     = round((sum(r ** 2 for r in returns) / len(returns)) ** 0.5 * math.sqrt(365), 3)
        _vol_cache[symbol] = (now, vol)
        log.info(f"  Vol  {symbol}: {vol*100:.0f}%")
        return vol
    except Exception:
        return fallback


def get_live_drift(symbol: str, fallback: float) -> float:
    now = time.time()
    if symbol in _drift_cache and now - _drift_cache[symbol][0] < 3600:
        return _drift_cache[symbol][1]
    try:
        closes = [float(c[4]) for c in requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol + "USDT", "interval": "1d", "limit": 31},
            timeout=10,
        ).json()]
        drift  = round(max(-0.80, min(0.80, math.log(closes[-1] / closes[0]) / 30 * 365)), 3)
        _drift_cache[symbol] = (now, drift)
        log.info(f"  Drift {symbol}: {drift*100:.0f}%")
        return drift
    except Exception:
        return fallback


# ══════════════════════════════════════════════════════════════════════════════
# PRICE FEEDS
# ══════════════════════════════════════════════════════════════════════════════

def get_price(cg_id: str, sym: str) -> float | None:
    sources = [
        lambda: float(requests.get("https://api.binance.com/api/v3/ticker/price",
            params={"symbol": sym + "USDT"}, timeout=8).json()["price"]),
        lambda: float(requests.get(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot",
            timeout=8).json()["data"]["amount"]),
        lambda: requests.get("https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"}, timeout=10).json()[cg_id]["usd"],
        lambda: float(requests.get(f"https://api.coincap.io/v2/assets/{cg_id}",
            timeout=8).json()["data"]["priceUsd"]),
    ]
    for fn in sources:
        try:
            p = fn()
            if p and float(p) > 0:
                return float(p)
        except Exception:
            pass
    log.warning(f"All price sources failed for {sym}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SLUG + MARKET DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════

def generate_daily_slugs(names: list) -> list:
    slugs, now = [], datetime.now(timezone.utc)
    for i in range(5):
        d    = now + timedelta(days=i)
        date = f"{d.strftime('%B').lower()}-{d.day}-{d.year}"
        for n in names:
            slugs += [f"{n}-above-on-{date}", f"{n}-up-or-down-on-{date}"]
    return slugs


def generate_weekly_slugs(names: list) -> list:
    now    = datetime.now(timezone.utc)
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    mo, nx = months[now.month - 1], months[now.month % 12]
    slugs  = []
    for n in names:
        slugs += [f"{n}-price-end-of-week", f"{n}-price-end-of-month",
                  f"{n}-price-in-{mo}-{now.year}", f"{n}-price-in-{nx}-{now.year}",
                  f"will-{n}-hit-new-ath-in-{now.year}"]
    return slugs


def get_markets_for_slug(slug: str) -> list:
    now = time.time()
    if slug in _slug_cache and now - _slug_cache[slug][0] < SLUG_TTL:
        return _slug_cache[slug][1]
    try:
        resp = requests.get(f"{GAMMA_HOST}/events", params={"slug": slug}, timeout=12)
        data = resp.json()
        ms   = data[0].get("markets", []) if isinstance(data, list) and data else []
        _slug_cache[slug] = (now, ms)
        return ms
    except Exception:
        _slug_cache[slug] = (now, [])
        return []


def search_gamma(keyword: str, limit: int = 100) -> list:
    try:
        resp = requests.get(f"{GAMMA_HOST}/markets",
            params={"search": keyword, "active": "true", "closed": "false", "limit": limit},
            timeout=15)
        data = resp.json()
        return data if isinstance(data, list) else data.get("markets", [])
    except Exception:
        return []


def collect_markets(asset: str, cfg: dict) -> list:
    seen, markets = set(), []

    def add(ms: list) -> None:
        for m in ms:
            raw = m.get("clobTokenIds") or []
            if isinstance(raw, str):
                try: raw = json.loads(raw)
                except: raw = []
            tid = raw[0] if raw else m.get("id", "")
            if tid and tid not in seen:
                seen.add(tid); markets.append(m)

    for slug in cfg["slugs"]:
        add(get_markets_for_slug(slug))
    for slug in generate_daily_slugs(cfg["daily_names"]):
        add(get_markets_for_slug(slug))
    for slug in generate_weekly_slugs(cfg["daily_names"]):
        add(get_markets_for_slug(slug))
    for kw in cfg["keywords"]:
        add(search_gamma(kw))

    return markets


# ══════════════════════════════════════════════════════════════════════════════
# OPTION PRICING
# ══════════════════════════════════════════════════════════════════════════════

def norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def european_prob(S, K, T, sigma, drift, direction) -> float | None:
    """P(S_T > K) at expiry. For 'above $X on [date]' markets."""
    if T <= 0 or S <= 0 or K <= 0 or sigma <= 0: return None
    d2   = (math.log(S / K) + (drift - 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    prob = norm_cdf(d2) if direction == "up" else norm_cdf(-d2)
    return round(min(0.97, max(0.03, prob)), 4)


def hit_probability(S, K, T, sigma, drift, direction) -> float | None:
    """P(S touches K before T). For 'hit $X before [date]' markets."""
    if T <= 0 or S <= 0 or K <= 0 or sigma <= 0: return None
    mu      = drift - 0.5 * sigma**2
    sqT     = sigma * math.sqrt(T)
    log_KS  = math.log(K / S)
    d1, d2  = (log_KS - mu * T) / sqT, (log_KS + mu * T) / sqT
    alpha   = 2 * mu / sigma**2
    if direction == "up":
        prob = norm_cdf(-d1) + math.exp(alpha * log_KS) * norm_cdf(-d2)
    else:
        log_SK = math.log(S / K)
        d1m    = (log_SK - mu * T) / sqT
        d2m    = (log_SK + mu * T) / sqT
        prob   = norm_cdf(-d1m) + math.exp(alpha * (-log_KS)) * norm_cdf(-d2m)
    return round(min(0.97, max(0.03, prob)), 4)


def is_touch_market(q: str) -> bool:
    ql = q.lower()
    on_date = bool(re.search(
        r"\bon\s+(january|february|march|april|may|june|july|august"
        r"|september|october|november|december)\s+\d", ql))
    eod = any(kw in ql for kw in ["end of day","end of week","end of month","end of hour","at end"])
    touch = any(kw in ql for kw in ["before","by december","by end","by 2027","dip to","reach $","hit $"])
    return touch and not on_date and not eod


# ══════════════════════════════════════════════════════════════════════════════
# END DATE + QUESTION PARSING
# ══════════════════════════════════════════════════════════════════════════════

_MONTHS = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
           "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}


def parse_end_date(q: str, default: datetime) -> datetime:
    ql  = q.lower()
    now = datetime.now(timezone.utc)
    for month, num in _MONTHS.items():
        m = re.search(rf"{month}\s+(\d{{1,2}})[,\s]+(\d{{4}})", ql)
        if m:
            try: return datetime(int(m.group(2)), num, int(m.group(1)), 23, 59, tzinfo=timezone.utc)
            except: pass
        m = re.search(rf"{month}\s+(\d{{1,2}})", ql)
        if m:
            day  = int(m.group(1))
            year = now.year if num > now.month or (num == now.month and day >= now.day) else now.year + 1
            try: return datetime(year, num, day, 23, 59, tzinfo=timezone.utc)
            except: pass
    return default


_UP_PATS   = [r"(?:above|over|reach|hit|exceed|be above)\s*\$?\s*([0-9][0-9,\.]*k?)",
              r"price.*?(?:above|reach)\s*\$?\s*([0-9][0-9,\.]*k?)"]
_DOWN_PATS = [r"(?:below|under|drop\s*to|fall\s*to|dip\s*to)\s*\$?\s*([0-9][0-9,\.]*k?)"]
_BETWEEN   = r"between\s*\$?\s*([0-9][0-9,\.]*k?)\s*and\s*\$?\s*([0-9][0-9,\.]*k?)"


def _num(s: str) -> float:
    s = s.replace(",", "").strip()
    return float(s[:-1]) * 1000 if s.lower().endswith("k") else float(s)


def parse_question(q: str, spot: float, end_date: datetime) -> list:
    ql  = q.lower()
    now = datetime.now(timezone.utc)
    T   = max(1/8760, (end_date - now).total_seconds() / (365.25 * 24 * 3600))

    m = re.search(_BETWEEN, ql)
    if m:
        lo, hi = _num(m.group(1)), _num(m.group(2))
        if lo > 0 and hi > 0:
            return [{"direction":"up","strike":hi,"T":T},{"direction":"down","strike":lo,"T":T}]

    sigs = []
    for pat in _UP_PATS:
        m = re.search(pat, ql)
        if m:
            try: sigs.append({"direction":"up","strike":_num(m.group(1)),"T":T}); break
            except: pass
    if not sigs:
        for pat in _DOWN_PATS:
            m = re.search(pat, ql)
            if m:
                try: sigs.append({"direction":"down","strike":_num(m.group(1)),"T":T}); break
                except: pass

    corrected = []
    for s in sigs:
        d = s["direction"]
        if s["strike"] < spot * 0.97 and d == "up":   d = "down"
        elif s["strike"] > spot * 1.03 and d == "down": d = "up"
        corrected.append({**s, "direction": d})
    return corrected


# ══════════════════════════════════════════════════════════════════════════════
# KELLY SIZING
# ══════════════════════════════════════════════════════════════════════════════

def kelly_buy(fair: float, limit: float, bankroll: float) -> float:
    """Quarter-Kelly USDC size. Bankroll = live USDC balance."""
    if limit <= 0 or limit >= 1 or fair * (1 - TAKER_FEE) <= limit:
        return 0.0
    f_star = (fair * (1 - TAKER_FEE) - limit) / (1 - limit)
    return round(min(MAX_BET_USDC, max(0.0, KELLY_FRACTION * f_star * bankroll)), 2)


# ══════════════════════════════════════════════════════════════════════════════
# CLOB CLIENT
# ══════════════════════════════════════════════════════════════════════════════

def build_client() -> ClobClient:
    pk = os.getenv("PK") or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not pk: raise EnvironmentError("PK not set in .env")
    ak, sec, pw = os.getenv("CLOB_API_KEY"), os.getenv("CLOB_SECRET"), os.getenv("CLOB_PASS_PHRASE")
    if ak and sec and pw:
        creds = ApiCreds(api_key=ak, api_secret=sec, api_passphrase=pw)
        log.info("Using saved API credentials")
    else:
        log.info("Deriving API credentials...")
        tmp   = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
        creds = tmp.create_or_derive_api_key()
        log.info(f"Save to .env:\n  CLOB_API_KEY={creds.api_key}\n  CLOB_SECRET={creds.api_secret}\n  CLOB_PASS_PHRASE={creds.api_passphrase}")
    return ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk, creds=creds)


# ══════════════════════════════════════════════════════════════════════════════
# ORDER PLACEMENT
# ══════════════════════════════════════════════════════════════════════════════

def place_order(client: ClobClient, om: OrderManager,
                token_id: str, limit: float, size_usdc: float,
                fair: float, label: str) -> bool:
    shares = round(size_usdc / limit, 2)
    if shares < 1:
        return False

    prefix = "[DRY RUN] " if DRY_RUN else ""
    log.info(f"{prefix}BUY LIMIT  {label}  {shares} shares @ {limit:.3f}  (${size_usdc:.2f})")

    if DRY_RUN:
        tg(f"🔵 <b>DRY RUN</b>  {label}\nBUY {shares} shares @ {limit:.3f}  (${size_usdc:.2f})\nFair: {fair:.3f}  Edge: {fair*(1-TAKER_FEE)-limit:+.3f}")
        return True

    try:
        resp = client.create_and_post_order(
            order_args=OrderArgs(token_id=token_id, price=round(limit, 4),
                                 side=Side.BUY, size=shares),
            options=PartialCreateOrderOptions(tick_size=TICK_SIZE),
            order_type=OrderType.GTC,
        )
        if resp and resp.get("success"):
            order_id = resp.get("orderID", "")
            log.info(f"  ✓ Order placed: {order_id}")
            tg(f"✅ <b>ORDER PLACED</b>  {label}\nBUY {shares} sh @ {limit:.3f}  (${size_usdc:.2f})\nFair: {fair:.3f}  Edge: {fair*(1-TAKER_FEE)-limit:+.3f}\nID: {order_id}")
            if order_id:
                om.record(order_id, token_id, label, limit, size_usdc, fair)
            return True
        else:
            log.error(f"  ✗ Rejected: {resp}")
            tg(f"❌ <b>REJECTED</b>  {label}\n{resp}")
    except Exception as e:
        log.error(f"  ✗ Failed: {e}")
        tg(f"❌ <b>FAILED</b>  {label}\n{e}")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_market(client: ClobClient, om: OrderManager,
                   market: dict, spot: float, asset: str,
                   cfg: dict, vol: float, drift: float,
                   bankroll: float) -> dict | None:

    question = market.get("question", "")
    volume   = float(market.get("volume") or 0)
    if volume < MIN_VOLUME:
        return None

    raw = market.get("clobTokenIds") or []
    if isinstance(raw, str):
        try: raw = json.loads(raw)
        except: return None
    if not raw:
        return None

    token_yes = raw[0]
    token_no  = raw[1] if len(raw) > 1 else None

    end_date = parse_end_date(question, cfg["end_date"])
    sigs     = parse_question(question, spot, end_date)
    if not sigs:
        return None

    sig = sigs[0]
    direction, strike, T = sig["direction"], sig["strike"], sig["T"]

    if not (0.15 * spot < strike < 6.0 * spot):
        return None

    touch = is_touch_market(question)
    fair  = (hit_probability if touch else european_prob)(spot, strike, T, vol, drift, direction)
    if fair is None or not (0.10 < fair < 0.90):
        return None

    model = "touch" if touch else "euro"
    label = f"{asset} {direction.upper()} ${strike:,.0f} ({model}) T={T*365:.0f}d"

    result = dict(question=question[:65], direction=direction, strike=strike,
                  fair=fair, volume=volume, T_days=T*365, model=model,
                  action=None, traded=False, limit=0.0, net_edge=0.0, side="?")

    if fair >= 0.50:
        # We think YES — BUY YES limit below fair
        limit    = round(max(0.02, fair - EDGE_BUFFER), 2)
        net_edge = fair * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="YES")
        if net_edge >= MIN_EDGE and not om.has_open_order(token_yes):
            size = kelly_buy(fair, limit, bankroll)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_yes, limit, size, fair, label):
                    result.update(action=f"BUY YES @{limit:.2f} ${size:.2f}", traded=True)
    else:
        # We think NO — BUY NO limit below (1-fair)
        fair_no  = 1 - fair
        limit    = round(max(0.02, fair_no - EDGE_BUFFER), 2)
        net_edge = fair_no * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="NO")
        if net_edge >= MIN_EDGE and token_no and not om.has_open_order(token_no):
            size = kelly_buy(fair_no, limit, bankroll)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_no, limit, size, fair_no, f"{label} NO"):
                    result.update(action=f"BUY NO  @{limit:.2f} ${size:.2f}", traded=True)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# P&L REPORTER
# ══════════════════════════════════════════════════════════════════════════════

class PnL:
    def __init__(self):
        self.start      = datetime.now(timezone.utc)
        self.last_report = time.time()

    def maybe_report(self, om: OrderManager, bankroll: float) -> None:
        if time.time() - self.last_report < PNL_REPORT_HOURS * 3600:
            return
        uptime = str(datetime.now(timezone.utc) - self.start).split(".")[0]
        msg = (
            f"📊 <b>Daily Summary</b>\n"
            f"Uptime: {uptime}\n"
            f"Orders placed: {om.order_count}\n"
            f"Fills: {om.fill_count}\n"
            f"USDC filled: ${om.fills_usdc:.2f}\n"
            f"Open orders: {len(om.orders)}\n"
            f"Current balance: ${bankroll:.2f}\n"
            f"(Check Polymarket dashboard for full P&L)"
        )
        log.info(msg.replace("<b>", "").replace("</b>", ""))
        tg(msg)
        self.last_report = time.time()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def run() -> None:
    mode = "*** DRY RUN ***" if DRY_RUN else "*** LIVE TRADING ***"
    log.info("=" * 72)
    log.info(f"Polymarket Ultimate Bot  —  {mode}")
    log.info(f"Assets: BTC · ETH · SOL  |  edge={EDGE_BUFFER*100:.0f}%  min={MIN_EDGE*100:.1f}%  fee={TAKER_FEE*100:.0f}%  Kelly={KELLY_FRACTION}  max=${MAX_BET_USDC}")
    log.info(f"Telegram: {'enabled ✓' if TG_TOKEN else 'disabled ✗'}")
    log.info("=" * 72)

    client = build_client()
    om     = OrderManager()
    pnl    = PnL()

    bankroll   = get_usdc_balance(client) if not DRY_RUN else MAX_BET_USDC * 20
    last_reset = time.time()

    tg(f"🤖 <b>Bot Started</b>  {mode}\nAssets: BTC · ETH · SOL\nBalance: ${bankroll:.2f}\nEdge buffer: {EDGE_BUFFER*100:.0f}%  Max bet: ${MAX_BET_USDC}")

    while True:
        cycle_start = time.time()

        # ── 6h hard reset ──────────────────────────────────────────────────────
        if cycle_start - last_reset > TRADED_RESET_HOURS * 3600:
            log.info(f"[RESET] {TRADED_RESET_HOURS}h reset — cancelling all orders")
            om.cancel_all(client)
            _slug_cache.clear()
            bankroll   = get_usdc_balance(client) if not DRY_RUN else bankroll
            last_reset = cycle_start
            tg(f"🔄 {TRADED_RESET_HOURS}h reset. Balance: ${bankroll:.2f}. {om.summary()}")

        # ── Poll for fills ─────────────────────────────────────────────────────
        fills = om.refresh_from_api(client)
        for f in fills:
            tg(
                f"💰 <b>FILL DETECTED</b>  {f['label']}\n"
                f"{f['size_matched']:.2f} shares @ {f['limit']:.3f}  = ${f['filled_usdc']:.2f}\n"
                f"Fair at placement: {f['fair']:.3f}  Edge: {f['edge_at_fill']:+.3f}"
            )

        # ── Refresh bankroll periodically ──────────────────────────────────────
        if not DRY_RUN and int(cycle_start) % 300 < POLL_INTERVAL:
            bankroll = get_usdc_balance(client)

        current_fairs: dict[str, float] = {}   # token_id → latest fair (for stale detection)
        cycle_orders = 0

        for asset, cfg in ASSETS.items():
            spot = get_price(cfg["coingecko_id"], cfg["binance_symbol"])
            if not spot:
                log.warning(f"No price for {asset}, skipping")
                continue

            log.info(f"\n{'─'*26} {asset} ${spot:,.2f} {'─'*26}")
            vol   = get_live_vol(cfg["binance_symbol"], cfg["vol"])
            drift = get_live_drift(cfg["binance_symbol"], cfg["drift"])

            markets = collect_markets(asset, cfg)
            log.info(f"  {len(markets)} {asset} markets collected")

            results = []
            for m in markets:
                r = process_market(client, om, m, spot, asset, cfg, vol, drift, bankroll)
                if r is not None:
                    results.append(r)
                    # Record current fair for stale-order detection
                    raw = m.get("clobTokenIds") or []
                    if isinstance(raw, str):
                        try: raw = json.loads(raw)
                        except: raw = []
                    if raw:
                        current_fairs[raw[0]] = r["fair"]
                        if len(raw) > 1:
                            current_fairs[raw[1]] = 1 - r["fair"]

            new_orders    = sum(1 for r in results if r["traded"])
            cycle_orders += new_orders

            if not results:
                log.info(f"  [{asset}] No uncertain markets found (vol={vol*100:.0f}% drift={drift*100:.0f}%)")
                continue

            log.info(f"  [{asset}] {len(results)} uncertain · {new_orders} orders · vol={vol*100:.0f}% drift={drift*100:.0f}%")
            log.info(f"  {'DIR':<5} {'STRIKE':>10}  {'FAIR':>5}  {'SIDE':>4}  {'LIMIT':>6}  {'NET_EDGE':>9}  {'MODEL':>5}  {'T':>5}  ACTION")
            for r in sorted(results, key=lambda x: x["net_edge"], reverse=True)[:20]:
                flag = "  ← ORDER" if r["traded"] else ""
                log.info(
                    f"  {r['direction'].upper():<5} ${r['strike']:>10,.0f}"
                    f"  {r['fair']:>5.3f}  {r['side']:>4}  {r['limit']:>6.3f}"
                    f"  {r['net_edge']:>+9.3f}  {r['model']:>5}  {r['T_days']:>4.0f}d"
                    f"  {r['action'] or '-'}{flag}"
                )

        # ── Cancel stale orders ────────────────────────────────────────────────
        if current_fairs:
            om.cancel_stale(client, current_fairs)

        if cycle_orders == 0:
            log.info("No orders placed this cycle.")

        pnl.maybe_report(om, bankroll)

        elapsed   = time.time() - cycle_start
        sleep_for = max(1.0, POLL_INTERVAL - elapsed)
        log.info(f"\nCycle {elapsed:.1f}s — next in {sleep_for:.0f}s  |  {om.summary()}\n{'='*72}")

        try:
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            tg(f"🛑 <b>Bot stopped</b>\n{om.summary()}")
            break


if __name__ == "__main__":
    run()
