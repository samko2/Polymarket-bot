"""
Polymarket Ultimate Edge Bot  —  v2 Production
═══════════════════════════════════════════════
Assets   : BTC, ETH, SOL
Pricing  : European digital + barrier/touch; vol term structure (7d/14d/30d)
Orders   : GTC limits; order book cross-check; book-aware limit placement
Sizing   : (bankroll − committed) → quarter-Kelly
Safety   : Position awareness on start; exponential-backoff retries;
           crash-proof loop with auto-restart; stale order refresh
Alerts   : Telegram only on real fills and bot stop
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

_handlers = [logging.StreamHandler()]
try:
    _handlers.append(logging.FileHandler(os.path.expanduser("~/Desktop/poly/bot.log")))
except OSError:
    pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=_handlers)
log = logging.getLogger("poly_bot")

# ── Exchange ───────────────────────────────────────────────────────────────────
CLOB_HOST  = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
DATA_HOST  = "https://data-api.polymarket.com"
CHAIN_ID   = 137
TICK_SIZE  = "0.01"

# ── Bot config ─────────────────────────────────────────────────────────────────
DRY_RUN               = False   # LIVE TRADING
POLL_INTERVAL         = 20      # seconds between scans
EDGE_BUFFER           = 0.03    # place limit this far below fair (3%)
MIN_EDGE              = 0.02    # minimum net edge after fee
TAKER_FEE             = 0.02    # Polymarket taker fee on winnings
KELLY_FRACTION        = 0.25    # quarter-Kelly
MAX_BET_USDC          = 1.0     # hard cap per order
MIN_BET_USDC          = 0.50    # skip orders below this
MIN_VOLUME            = 5_000   # min market lifetime volume
TRADED_RESET_HOURS    = 6       # full reset every N hours
STALE_FAIR_DRIFT      = 0.12    # cancel order if fair drifted >12%
PNL_REPORT_HOURS      = 24
MAX_MODEL_MARKET_GAP  = 0.30    # skip if model and book-mid disagree >30%
MIN_BOOK_LIQUIDITY    = 0.01    # skip markets with spread wider than 99 cents
MAX_POSITION_TOKENS   = 50      # max distinct token positions held at once
MANUAL_BANKROLL       = float(os.getenv("MANUAL_BANKROLL", "0"))  # override balance check if set

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
        "keywords":       ["bitcoin"],   # "btc" results are a subset of "bitcoin"
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
        "keywords":       ["ethereum"],  # "eth" results are a subset of "ethereum"
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
        "keywords":       ["solana"],    # "sol" results are a subset of "solana"
        "daily_names":    ["solana", "sol"],
        "slugs": [
            "what-price-will-solana-hit-before-2027",
            "what-price-will-sol-reach-before-2027",
        ],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
}

# ── Caches ─────────────────────────────────────────────────────────────────────
_slug_cache:  dict = {}
_vol_cache:   dict = {}
_drift_cache: dict = {}
_book_cache:  dict = {}
_imb_cache:   dict = {}   # Binance book-imbalance cache
SLUG_TTL  = 300
BOOK_TTL  = 15    # order books stale quickly
SLUG_MAX  = 2000  # evict oldest entries beyond this to prevent memory growth
IMB_TTL   = 60    # imbalance re-fetched once per minute


# ══════════════════════════════════════════════════════════════════════════════
# HTTP WITH RETRY + EXPONENTIAL BACKOFF
# ══════════════════════════════════════════════════════════════════════════════

import random as _random

def retry_get(url: str, params: dict = None, timeout: int = 10,
              attempts: int = 3) -> requests.Response:
    """GET with exponential backoff + jitter. Raises on final failure."""
    delay = 2.0
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == attempts - 1:
                raise
            jitter = delay * _random.uniform(0.8, 1.2)   # ±20% jitter
            log.debug(f"Request failed ({e}), retrying in {jitter:.1f}s…")
            time.sleep(jitter)
            delay *= 2


# ══════════════════════════════════════════════════════════════════════════════
# ORDER BOOK
# ══════════════════════════════════════════════════════════════════════════════

def get_order_book(token_id: str) -> tuple[float, float, float, bool]:
    """Returns (best_bid, best_ask, mid, liquid). Cached for BOOK_TTL seconds."""
    now = time.time()
    if token_id in _book_cache and now - _book_cache[token_id][0] < BOOK_TTL:
        return _book_cache[token_id][1]
    try:
        data     = retry_get(f"{CLOB_HOST}/book", params={"token_id": token_id}, timeout=8).json()
        bids     = data.get("bids", [])
        asks     = data.get("asks", [])
        best_bid = float(bids[0]["price"]) if bids else 0.0
        best_ask = float(asks[0]["price"]) if asks else 1.0
        mid      = round((best_bid + best_ask) / 2, 4)
        liquid   = bool(bids and asks and (best_ask - best_bid) <= (1.0 - MIN_BOOK_LIQUIDITY))
        result   = (best_bid, best_ask, mid, liquid)
        _book_cache[token_id] = (now, result)
        return result
    except Exception:
        return 0.0, 1.0, 0.5, False


# ══════════════════════════════════════════════════════════════════════════════
# POSITION AWARENESS
# ══════════════════════════════════════════════════════════════════════════════

def get_wallet_address(pk: str) -> str:
    try:
        from eth_account import Account
        return Account.from_key(pk).address
    except Exception:
        return ""


def get_existing_positions(wallet: str) -> set[str]:
    """Fetch token IDs where we already hold a position. Skips these on entry."""
    if not wallet:
        return set()
    try:
        data = retry_get(
            f"{DATA_HOST}/positions",
            params={"user": wallet, "sizeThreshold": "0.01"},
            timeout=12,
        ).json()
        held = set()
        for p in (data if isinstance(data, list) else []):
            tid  = p.get("asset") or p.get("token_id") or p.get("assetId", "")
            size = float(p.get("size") or p.get("position") or 0)
            if tid and size > 0.01:
                held.add(tid)
        log.info(f"  Existing positions loaded: {len(held)} token(s)")
        return held
    except Exception as e:
        log.warning(f"Could not load positions: {e}")
        return set()


# ══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGER
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TrackedOrder:
    order_id:  str
    token_id:  str
    label:     str
    limit:     float
    size_usdc: float
    fair:      float
    placed_at: float = field(default_factory=time.time)


class OrderManager:
    def __init__(self, existing_positions: set[str] = None):
        self.orders:          dict[str, TrackedOrder] = {}
        self.open_token_ids:  set[str] = set()
        self.held_token_ids:  set[str] = existing_positions or set()  # from API on startup
        self.fills_usdc:      float = 0.0
        self.fill_count:      int   = 0
        self.order_count:     int   = 0

    def record(self, order_id: str, token_id: str, label: str,
               limit: float, size_usdc: float, fair: float) -> None:
        self.orders[order_id]  = TrackedOrder(order_id, token_id, label, limit, size_usdc, fair)
        self.open_token_ids.add(token_id)
        self.order_count      += 1

    def has_open_order(self, token_id: str) -> bool:
        return token_id in self.open_token_ids

    def already_holds(self, token_id: str) -> bool:
        return token_id in self.held_token_ids

    def committed_usdc(self) -> float:
        """USDC locked in open (unfilled) orders."""
        return sum(o.size_usdc for o in self.orders.values())

    def refresh_from_api(self, client: ClobClient) -> list[dict]:
        if DRY_RUN:
            return []
        fill_events = []
        try:
            open_orders = client.get_open_orders() or []
            open_ids    = {o.get("id") or o.get("orderID") for o in open_orders}
            open_tokens = {o.get("asset_id") for o in open_orders if o.get("asset_id")}
            self.open_token_ids = open_tokens

            for oid in [x for x in self.orders if x not in open_ids]:
                tracked = self.orders.pop(oid)
                try:
                    detail       = client.get_order(oid) or {}
                    size_matched = float(detail.get("size_matched") or 0)
                    status       = detail.get("status", "UNKNOWN")
                    if size_matched > 0.001:
                        filled_usdc = round(size_matched * tracked.limit, 2)
                        self.fills_usdc += filled_usdc
                        self.fill_count += 1
                        self.held_token_ids.add(tracked.token_id)
                        fill_events.append({
                            "label":        tracked.label,
                            "limit":        tracked.limit,
                            "fair":         tracked.fair,
                            "size_matched": size_matched,
                            "filled_usdc":  filled_usdc,
                            "edge_at_fill": tracked.fair * (1 - TAKER_FEE) - tracked.limit,
                        })
                        log.info(f"  ✓ FILL: {tracked.label} {size_matched:.2f}sh @ {tracked.limit:.3f} = ${filled_usdc:.2f}")
                    else:
                        log.info(f"  Order {oid[:10]} gone (status={status}, no fill)")
                except Exception as e:
                    log.debug(f"  get_order {oid}: {e}")
        except Exception as e:
            log.warning(f"refresh_from_api failed: {e}")
        return fill_events

    def cancel_stale(self, client: ClobClient, current_fairs: dict[str, float]) -> None:
        if DRY_RUN:
            return
        for oid, tracked in list(self.orders.items()):
            curr = current_fairs.get(tracked.token_id)
            if curr is None:
                continue
            if abs(curr - tracked.fair) >= STALE_FAIR_DRIFT:
                log.info(f"  Cancelling stale {oid[:10]} (fair drifted {abs(curr-tracked.fair):.2f})")
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
            log.info(f"  Cancelled all ({len(self.orders)} tracked)")
        except Exception as e:
            log.warning(f"  cancel_all failed: {e}")
        self.orders.clear()
        self.open_token_ids.clear()

    def summary(self) -> str:
        return (f"Orders: {self.order_count}  Fills: {self.fill_count}  "
                f"Filled: ${self.fills_usdc:.2f}  Committed: ${self.committed_usdc():.2f}  "
                f"Positions held: {len(self.held_token_ids)}")


# ══════════════════════════════════════════════════════════════════════════════
# LIVE USDC BALANCE
# ══════════════════════════════════════════════════════════════════════════════

_balance_cache: list = [0.0, 0.0]


def get_usdc_balance_onchain(wallet: str) -> float:
    """Fallback: read raw USDC balance directly from Polygon chain."""
    if not wallet:
        return 0.0
    for usdc in [
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # native USDC
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e (bridged)
    ]:
        try:
            data    = "0x70a08231" + wallet[2:].lower().zfill(64)
            payload = {"jsonrpc":"2.0","method":"eth_call",
                       "params":[{"to": usdc, "data": data}, "latest"],"id":1}
            result  = requests.post("https://polygon-rpc.com",
                                    json=payload, timeout=10).json().get("result","0x0")
            bal     = round(int(result, 16) / 1e6, 2)
            if bal > 0:
                log.info(f"  On-chain USDC ({usdc[:10]}…): ${bal:.2f}")
                return bal
        except Exception:
            pass
    return 0.0


def get_usdc_balance(client: ClobClient, wallet: str = "") -> float:
    now = time.time()
    if _balance_cache[1] and now - _balance_cache[1] < 300:
        return _balance_cache[0]
    try:
        resp = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        log.info(f"  CLOB balance raw: {resp}")
        balance = float(
            resp.get("balance") or resp.get("available") or
            resp.get("allowance") or resp.get("amount") or 0
        )
        if balance == 0 and wallet:
            balance = get_usdc_balance_onchain(wallet)
        _balance_cache[0] = balance
        _balance_cache[1] = now
        log.info(f"  USDC balance: ${balance:.2f}")
        return balance
    except Exception as e:
        log.warning(f"Balance fetch failed: {e}")
        if wallet:
            balance = get_usdc_balance_onchain(wallet)
            if balance > 0:
                _balance_cache[0] = balance
                _balance_cache[1] = now
                return balance
        return _balance_cache[0] or MAX_BET_USDC * 10


def free_bankroll(bankroll: float, om: OrderManager) -> float:
    """Bankroll minus USDC already committed to open orders."""
    return max(0.0, bankroll - om.committed_usdc())


# ══════════════════════════════════════════════════════════════════════════════
# VOL + DRIFT WITH TERM STRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_realized_vol(symbol: str, interval: str, bars: int) -> float:
    closes  = [float(c[4]) for c in retry_get(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": symbol + "USDT", "interval": interval, "limit": bars + 1},
        timeout=10,
    ).json()]
    returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    # Annualise: daily bars × √365, hourly bars × √(365×24)
    ann     = math.sqrt(365 * 24) if interval == "1h" else math.sqrt(365)
    return round((sum(r ** 2 for r in returns) / len(returns)) ** 0.5 * ann, 3)


def get_live_vol(symbol: str, fallback: float, T_years: float = 1.0) -> float:
    """Returns realized vol matched to the market's time horizon."""
    if T_years < 14 / 365:
        window, interval, label = 168, "1h", "7d-hourly"  # 7d × 24h bars
    elif T_years < 60 / 365:
        window, interval, label = 14,  "1d", "14d-daily"
    else:
        window, interval, label = 30,  "1d", "30d-daily"

    cache_key = f"{symbol}_{window}{interval}"
    now       = time.time()
    if cache_key in _vol_cache and now - _vol_cache[cache_key][0] < 3600:
        return _vol_cache[cache_key][1]
    try:
        vol = _fetch_realized_vol(symbol, interval, window)
        _vol_cache[cache_key] = (now, vol)
        log.info(f"  Vol {symbol} ({label}): {vol*100:.0f}%")
        return vol
    except Exception:
        return fallback


def get_live_drift(symbol: str, fallback: float) -> float:
    now = time.time()
    if symbol in _drift_cache and now - _drift_cache[symbol][0] < 3600:
        return _drift_cache[symbol][1]
    try:
        closes = [float(c[4]) for c in retry_get(
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
            p = float(fn())
            if p > 0:
                return p
        except Exception:
            pass
    log.warning(f"All price sources failed for {sym}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MARKET DISCOVERY
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
        resp = retry_get(f"{GAMMA_HOST}/events", params={"slug": slug}, timeout=12)
        data = resp.json()
        ms   = data[0].get("markets", []) if isinstance(data, list) and data else []
        # Only cache successful (even if genuinely empty) responses — never cache errors
        if len(_slug_cache) >= SLUG_MAX:
            # Evict the oldest quarter to keep memory bounded
            oldest = sorted(_slug_cache, key=lambda k: _slug_cache[k][0])[:SLUG_MAX // 4]
            for k in oldest:
                del _slug_cache[k]
        _slug_cache[slug] = (now, ms)
        return ms
    except Exception:
        # Don't cache failures — a brief API blip won't poison the next 5 minutes
        return []


def search_gamma(keyword: str, limit: int = 100) -> list:
    try:
        data = retry_get(f"{GAMMA_HOST}/markets",
            params={"search": keyword, "active": "true", "closed": "false", "limit": limit},
            timeout=15).json()
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
    if T <= 0 or S <= 0 or K <= 0 or sigma <= 0: return None
    d2   = (math.log(S / K) + (drift - 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    prob = norm_cdf(d2) if direction == "up" else norm_cdf(-d2)
    return round(min(0.97, max(0.03, prob)), 4)


def hit_probability(S, K, T, sigma, drift, direction) -> float | None:
    if T <= 0 or S <= 0 or K <= 0 or sigma <= 0: return None
    mu     = drift - 0.5 * sigma**2
    sqT    = sigma * math.sqrt(T)
    log_KS = math.log(K / S)
    d1, d2 = (log_KS - mu * T) / sqT, (log_KS + mu * T) / sqT
    alpha  = 2 * mu / sigma**2
    if direction == "up":
        prob = norm_cdf(-d1) + math.exp(alpha * log_KS) * norm_cdf(-d2)
    else:
        log_SK = math.log(S / K)
        d1m    = (log_SK - mu * T) / sqT
        d2m    = (log_SK + mu * T) / sqT
        prob   = norm_cdf(-d1m) + math.exp(alpha * (-log_KS)) * norm_cdf(-d2m)
    return round(min(0.97, max(0.03, prob)), 4)


def is_touch_market(q: str) -> bool:
    ql      = q.lower()
    on_date = bool(re.search(
        r"\bon\s+(january|february|march|april|may|june|july|august"
        r"|september|october|november|december)\s+\d", ql))
    eod   = any(kw in ql for kw in ["end of day","end of week","end of month","end of hour","at end"])
    touch = any(kw in ql for kw in ["before","by december","by end","by 2026","by 2027","by 2028","dip to","reach $","hit $"])
    return touch and not on_date and not eod


# ══════════════════════════════════════════════════════════════════════════════
# QUESTION PARSING
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
            return [{"direction":"up","strike":hi,"T":T}, {"direction":"down","strike":lo,"T":T}]

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
        if s["strike"] < spot * 0.97 and d == "up":    d = "down"
        elif s["strike"] > spot * 1.03 and d == "down": d = "up"
        corrected.append({**s, "direction": d})
    return corrected


# ══════════════════════════════════════════════════════════════════════════════
# KELLY SIZING
# ══════════════════════════════════════════════════════════════════════════════

def kelly_buy(fair: float, limit: float, bankroll: float) -> float:
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
        log.info("Deriving API credentials from PK…")
        tmp   = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
        creds = tmp.create_or_derive_api_key()
        log.info(f"Tip — add these to Railway env vars to skip derivation:\n"
                 f"  CLOB_API_KEY={creds.api_key}\n"
                 f"  CLOB_SECRET={creds.api_secret}\n"
                 f"  CLOB_PASS_PHRASE={creds.api_passphrase}")
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

    log.info(f"{'[DRY] ' if DRY_RUN else ''}BUY LIMIT  {label}  {shares}sh @ {limit:.3f}  (${size_usdc:.2f})")

    if DRY_RUN:
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
            log.info(f"  ✓ Placed: {order_id}")
            tg(f"✅ <b>TRADE PLACED</b>  {label}\n"
               f"BUY {shares}sh @ {limit:.3f}  (${size_usdc:.2f})\n"
               f"Fair: {fair:.3f}  Edge: {fair*(1-TAKER_FEE)-limit:+.3f}\n"
               f"ID: {order_id}")
            if order_id:
                om.record(order_id, token_id, label, limit, size_usdc, fair)
            return True
        log.error(f"  ✗ Rejected: {resp}")
    except Exception as e:
        log.error(f"  ✗ Failed: {e}")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_market(client: ClobClient, om: OrderManager,
                   market: dict, spot: float, asset: str,
                   cfg: dict, drift: float, bankroll: float) -> dict | None:

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

    # ── Skip if we already hold this position ──────────────────────────────────
    if om.already_holds(token_yes) or (token_no and om.already_holds(token_no)):
        return None

    # ── Skip if we're at the position cap ─────────────────────────────────────
    if len(om.held_token_ids) >= MAX_POSITION_TOKENS:
        return None

    # ── Parse question ─────────────────────────────────────────────────────────
    end_date = parse_end_date(question, cfg["end_date"])
    sigs     = parse_question(question, spot, end_date)
    if not sigs:
        return None

    sig = sigs[0]
    direction, strike, T = sig["direction"], sig["strike"], sig["T"]

    if not (0.15 * spot < strike < 6.0 * spot):
        return None

    # ── Vol matched to time horizon ────────────────────────────────────────────
    vol = get_live_vol(cfg["binance_symbol"], cfg["vol"], T)

    # ── Price model ───────────────────────────────────────────────────────────
    touch = is_touch_market(question)
    fair  = (hit_probability if touch else european_prob)(spot, strike, T, vol, drift, direction)
    if fair is None or not (0.10 < fair < 0.90):
        return None

    model = "touch" if touch else "euro"
    label = f"{asset} {direction.upper()} ${strike:,.0f} ({model}) T={T*365:.0f}d"

    # ── Order book cross-check ─────────────────────────────────────────────────
    bid, ask, book_mid, liquid = get_order_book(token_yes)
    if not liquid:
        return None  # dead market, no counterparty

    # Always compare YES vs YES — gap is the same whether measured from YES or NO side
    gap = abs(fair - book_mid)
    if gap > MAX_MODEL_MARKET_GAP:
        # Model and market strongly disagree — likely bad question parse, skip
        log.debug(f"  Skip {label[:40]}: model={fair:.2f} book_mid={book_mid:.2f} gap={gap:.2f}")
        return None

    result = dict(question=question[:65], direction=direction, strike=strike,
                  fair=fair, book_mid=book_mid, gap=round(gap, 3), volume=volume,
                  T_days=T*365, model=model, action=None, traded=False,
                  limit=0.0, net_edge=0.0, side="?")

    # ── Free bankroll = balance minus already-committed USDC ──────────────────
    available = free_bankroll(bankroll, om)

    if fair >= 0.50:
        # BUY YES — limit below our fair, but at least 1 tick above best bid
        limit    = round(max(bid + 0.01, fair - EDGE_BUFFER, 0.02), 2)
        limit    = min(limit, ask - 0.01)  # never cross the spread
        net_edge = fair * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="YES")
        if net_edge >= MIN_EDGE and not om.has_open_order(token_yes):
            size = kelly_buy(fair, limit, available)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_yes, limit, size, fair, label):
                    result.update(action=f"BUY YES @{limit:.2f} ${size:.2f}", traded=True)
    else:
        # BUY NO — use NO token's book
        if token_no:
            bid_no, ask_no, _, liquid_no = get_order_book(token_no)
        else:
            bid_no, ask_no, liquid_no = 0.0, 1.0, False

        fair_no  = 1 - fair
        limit    = round(max(bid_no + 0.01, fair_no - EDGE_BUFFER, 0.02), 2)
        if liquid_no:
            limit = min(limit, ask_no - 0.01)
        net_edge = fair_no * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="NO")
        if net_edge >= MIN_EDGE and token_no and not om.has_open_order(token_no):
            size = kelly_buy(fair_no, limit, available)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_no, limit, size, fair_no, f"{label} NO"):
                    result.update(action=f"BUY NO  @{limit:.2f} ${size:.2f}", traded=True)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# HOURLY MARKET SCANNING  (up/down markets — momentum-based, no strike)
# ══════════════════════════════════════════════════════════════════════════════

# Each entry: slug_name used in Polymarket URLs, Binance base symbol
HOURLY_ASSETS = {
    "BTC": {"slug_name": "bitcoin",  "binance": "BTC"},
    "ETH": {"slug_name": "ethereum", "binance": "ETH"},
    "SOL": {"slug_name": "solana",   "binance": "SOL"},
}


def _et_now() -> datetime:
    """Current time in US Eastern (EDT = UTC-4 Mar–Nov, EST = UTC-5 otherwise)."""
    utc = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 10 else -5
    return utc + timedelta(hours=offset)


def generate_hourly_slugs() -> list:
    """
    Return list of (slug, asset, binance_sym) for the current hour + next 2 hours in ET.
    Tries both slug formats Polymarket has used historically.
    """
    et     = _et_now()
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    results = []

    for hour_offset in range(3):
        ts    = et + timedelta(hours=hour_offset)
        month = months[ts.month - 1]
        day   = ts.day
        year  = ts.year
        h24   = ts.hour
        ampm  = "am" if h24 < 12 else "pm"
        h12   = h24 % 12 or 12   # 0→12, 13→1, etc.

        for asset, cfg in HOURLY_ASSETS.items():
            name    = cfg["slug_name"]
            bsym    = cfg["binance"]
            # Format 1: "bitcoin-up-or-down-june-7-2026-2pm-et"
            slug1 = f"{name}-up-or-down-{month}-{day}-{year}-{h12}{ampm}-et"
            # Format 2: "bitcoin-up-or-down-june-7-2026-2-pm-et"
            slug2 = f"{name}-up-or-down-{month}-{day}-{year}-{h12}-{ampm}-et"
            results.append((slug1, asset, bsym))
            results.append((slug2, asset, bsym))

    return results


# ── Momentum / imbalance signals ─────────────────────────────────────────────

_mom_cache: dict[str, tuple[float, float]] = {}   # bsym → (ts, signal)
_MOM_TTL = 60.0  # seconds between momentum re-fetches


def _hourly_momentum(binance_sym: str) -> float:
    """
    Return momentum signal in [-0.15, +0.15] from 1-min Binance klines.
    Positive = bullish (favour UP token).
    """
    now = time.time()
    if binance_sym in _mom_cache:
        ts, val = _mom_cache[binance_sym]
        if now - ts < _MOM_TTL:
            return val

    try:
        klines = retry_get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": binance_sym + "USDT", "interval": "1m", "limit": 30},
            timeout=10,
        ).json()
        closes = [float(k[4]) for k in klines]
        if len(closes) < 12:
            return 0.0
        # Multi-timeframe momentum, normalised
        m10 = (closes[-1] - closes[-11]) / closes[-11]   # 10-bar
        m5  = (closes[-1] - closes[-6])  / closes[-6]    # 5-bar
        m2  = (closes[-1] - closes[-3])  / closes[-3]    # 2-bar
        raw = 0.50 * m10 + 0.30 * m5 + 0.20 * m2
        val = max(-0.15, min(0.15, raw * 8))             # scale ×8 then clamp
    except Exception as e:
        log.debug(f"  Momentum error {binance_sym}: {e}")
        val = 0.0

    _mom_cache[binance_sym] = (now, val)
    return val


def _book_imbalance(binance_sym: str) -> float:
    """
    Return Binance top-10 order-book imbalance in [-1, +1].
    Positive = more bid depth (bullish). Cached for IMB_TTL seconds.
    """
    now = time.time()
    if binance_sym in _imb_cache and now - _imb_cache[binance_sym][0] < IMB_TTL:
        return _imb_cache[binance_sym][1]
    try:
        book    = retry_get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": binance_sym + "USDT", "limit": 20},
            timeout=8,
        ).json()
        bid_vol = sum(float(b[1]) for b in book.get("bids", [])[:10])
        ask_vol = sum(float(a[1]) for a in book.get("asks", [])[:10])
        total   = bid_vol + ask_vol
        val     = (bid_vol - ask_vol) / total if total > 0 else 0.0
    except Exception:
        val = _imb_cache.get(binance_sym, (0, 0.0))[1]  # reuse last known value
    _imb_cache[binance_sym] = (now, val)
    return val


def compute_hourly_fair(binance_sym: str) -> tuple:
    """
    Return (fair_up, fair_down) for an hourly up/down market.
    Momentum is the primary signal; order-book imbalance is secondary.
    Clamped to [0.30, 0.70] — never extreme on a sub-hour bet.
    """
    mom      = _hourly_momentum(binance_sym)
    imb      = _book_imbalance(binance_sym)
    fair_up  = 0.50 + 2.0 * mom + 0.08 * imb
    fair_up  = round(max(0.30, min(0.70, fair_up)), 4)
    return fair_up, round(1 - fair_up, 4)


MIN_HOURLY_VOLUME  = 200    # much lower bar than weekly/monthly markets
MIN_HOURLY_MINS    = 20    # skip hourly markets with <20 min to expiry


def is_hourly_updown(question: str) -> bool:
    """Return True if this is a time-of-day up/down question with no price target."""
    ql = question.lower()
    if "up or down" not in ql and "higher or lower" not in ql:
        return False
    if re.search(r"\$\s*[\d,]+", ql):        # has a price target → not hourly
        return False
    # Flexible time pattern: "2pm", "2 pm", "2:00 pm", "14:00"
    if not re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b|\b\d{2}:\d{2}\b", ql, re.I):
        return False
    return True


def _hourly_mins_remaining(market: dict) -> float:
    """Return minutes until this market's end date, or 999 if unknown."""
    for key in ("endDateIso", "end_date_iso", "endDate", "end_date", "closeTime"):
        val = market.get(key)
        if val:
            try:
                end = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                return (end - datetime.now(timezone.utc)).total_seconds() / 60
            except Exception:
                pass
    return 999.0


def _collect_hourly_markets() -> list[tuple[dict, str, str]]:
    """
    Return list of (market, asset, binance_sym) for all hourly up/down markets.
    Tries slug lookup first, then Gamma API keyword search as fallback.
    """
    seen, results = set(), []

    def add(m: dict, asset: str, bsym: str) -> None:
        raw = m.get("clobTokenIds") or []
        if isinstance(raw, str):
            try: raw = json.loads(raw)
            except: return
        tid = raw[0] if raw else m.get("id", "")
        if tid and tid not in seen and is_hourly_updown(m.get("question", "")):
            seen.add(tid)
            results.append((m, asset, bsym))

    # ── Slug-based lookup ──────────────────────────────────────────────────
    for slug, asset, bsym in generate_hourly_slugs():
        try:
            for m in get_markets_for_slug(slug):
                add(m, asset, bsym)
        except Exception:
            pass

    # ── Gamma keyword fallback ─────────────────────────────────────────────
    if not results:
        kw_map = [("bitcoin up or down", "BTC", "BTC"),
                  ("ethereum up or down", "ETH", "ETH"),
                  ("solana up or down",   "SOL", "SOL")]
        for kw, asset, bsym in kw_map:
            try:
                for m in search_gamma(kw, limit=20):
                    add(m, asset, bsym)
            except Exception:
                pass

    return results


def scan_hourly_markets(client: ClobClient, om: OrderManager, bankroll: float) -> int:
    """
    Scan current + next-2-hour up/down markets for BTC, ETH, SOL.
    Returns number of orders placed.
    """
    orders_placed = 0
    seen_tokens: set = set()

    for m, asset, binance_sym in _collect_hourly_markets():
        question = m.get("question", "")

        # ── Volume gate (lower than weekly markets) ────────────────────────
        if float(m.get("volume") or 0) < MIN_HOURLY_VOLUME:
            continue

        # ── Skip markets about to expire (< 20 min left) ──────────────────
        mins_left = _hourly_mins_remaining(m)
        if mins_left < MIN_HOURLY_MINS:
            log.debug(f"    Skip expiring hourly ({mins_left:.0f}m left): {question[:40]}")
            continue

        raw = m.get("clobTokenIds") or []
        if isinstance(raw, str):
            try: raw = json.loads(raw)
            except: continue
        if len(raw) < 2:
            continue

        token_up, token_down = raw[0], raw[1]
        if token_up in seen_tokens or token_down in seen_tokens:
            continue
        seen_tokens.add(token_up); seen_tokens.add(token_down)

        if om.already_holds(token_up) or om.already_holds(token_down):
            continue
        if len(om.held_token_ids) >= MAX_POSITION_TOKENS:
            continue

        fair_up, fair_down = compute_hourly_fair(binance_sym)
        tm       = re.search(r"\d{1,2}(?::\d{2})?\s*(?:am|pm)", question, re.I)
        time_str = tm.group(0) if tm else "?"
        log.info(f"  ⏰ {asset} hourly [{time_str} ET | {mins_left:.0f}m left] "
                 f"fair_up={fair_up:.3f} fair_dn={fair_down:.3f}  {question[:45]}")

        available = free_bankroll(bankroll, om)

        if fair_up >= 0.50:
            bid, ask, book_mid, liquid = get_order_book(token_up)
            if not liquid:
                continue
            if abs(fair_up - book_mid) > MAX_MODEL_MARKET_GAP:
                log.debug(f"    Skip hourly UP: model={fair_up:.2f} book={book_mid:.2f}")
                continue
            limit    = round(max(bid + 0.01, fair_up - EDGE_BUFFER, 0.02), 2)
            limit    = min(limit, ask - 0.01)
            net_edge = fair_up * (1 - TAKER_FEE) - limit
            label    = f"{asset} UP {time_str} ET (hourly)"
            if net_edge >= MIN_EDGE and not om.has_open_order(token_up):
                size = kelly_buy(fair_up, limit, available)
                if size >= MIN_BET_USDC:
                    if place_order(client, om, token_up, limit, size, fair_up, label):
                        orders_placed += 1
        else:
            bid_dn, ask_dn, book_mid_dn, liquid_dn = get_order_book(token_down)
            if not liquid_dn:
                continue
            if abs(fair_down - book_mid_dn) > MAX_MODEL_MARKET_GAP:
                log.debug(f"    Skip hourly DOWN: model={fair_down:.2f} book={book_mid_dn:.2f}")
                continue
            limit    = round(max(bid_dn + 0.01, fair_down - EDGE_BUFFER, 0.02), 2)
            limit    = min(limit, ask_dn - 0.01)
            net_edge = fair_down * (1 - TAKER_FEE) - limit
            label    = f"{asset} DOWN {time_str} ET (hourly)"
            if net_edge >= MIN_EDGE and not om.has_open_order(token_down):
                size = kelly_buy(fair_down, limit, available)
                if size >= MIN_BET_USDC:
                    if place_order(client, om, token_down, limit, size, fair_down, label):
                        orders_placed += 1

    return orders_placed


# ══════════════════════════════════════════════════════════════════════════════
# P&L TRACKER
# ══════════════════════════════════════════════════════════════════════════════

class PnL:
    def __init__(self):
        self.start       = datetime.now(timezone.utc)
        self.last_report = time.time()

    def maybe_report(self, om: OrderManager, bankroll: float) -> None:
        if time.time() - self.last_report < PNL_REPORT_HOURS * 3600:
            return
        uptime = str(datetime.now(timezone.utc) - self.start).split(".")[0]
        log.info(f"[24h] uptime={uptime} {om.summary()} balance=${bankroll:.2f}")
        self.last_report = time.time()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def run_loop(client: ClobClient, wallet: str) -> None:
    """Inner loop — runs until KeyboardInterrupt or unrecoverable error."""
    om  = OrderManager(get_existing_positions(wallet))
    pnl = PnL()

    if MANUAL_BANKROLL > 0:
        bankroll = MANUAL_BANKROLL
        log.info(f"  Using manual bankroll: ${bankroll:.2f}")
    else:
        bankroll = get_usdc_balance(client, wallet) if not DRY_RUN else MAX_BET_USDC * 20
    last_reset = time.time()

    log.info(f"Loop started. Balance: ${bankroll:.2f}  Positions loaded: {len(om.held_token_ids)}")

    while True:
        cycle_start = time.time()

        # ── Periodic hard reset ────────────────────────────────────────────────
        if cycle_start - last_reset > TRADED_RESET_HOURS * 3600:
            log.info(f"[RESET] {TRADED_RESET_HOURS}h — cancelling all orders")
            om.cancel_all(client)
            _slug_cache.clear()
            if MANUAL_BANKROLL > 0:
                bankroll = MANUAL_BANKROLL
            elif not DRY_RUN:
                bankroll = get_usdc_balance(client, wallet)
            last_reset = cycle_start

        # ── Check for fills ────────────────────────────────────────────────────
        fills = om.refresh_from_api(client)
        for f in fills:
            tg(f"💰 <b>FILL</b>  {f['label']}\n"
               f"{f['size_matched']:.2f}sh @ {f['limit']:.3f} = ${f['filled_usdc']:.2f}\n"
               f"Fair: {f['fair']:.3f}  Edge: {f['edge_at_fill']:+.3f}")

        # ── Refresh balance every 5 min ────────────────────────────────────────
        if not DRY_RUN and MANUAL_BANKROLL == 0 and int(cycle_start) % 300 < POLL_INTERVAL:
            bankroll = get_usdc_balance(client, wallet)

        current_fairs: dict[str, float] = {}
        cycle_orders = 0

        for asset, cfg in ASSETS.items():
            spot = get_price(cfg["coingecko_id"], cfg["binance_symbol"])
            if not spot:
                log.warning(f"No price for {asset}, skipping")
                continue

            log.info(f"\n{'─'*24} {asset} ${spot:,.2f} {'─'*24}")
            drift   = get_live_drift(cfg["binance_symbol"], cfg["drift"])
            markets = collect_markets(asset, cfg)
            log.info(f"  {len(markets)} {asset} markets")

            results = []
            for m in markets:
                try:
                    r = process_market(client, om, m, spot, asset, cfg, drift, bankroll)
                except Exception as e:
                    log.debug(f"  process_market error: {e}")
                    r = None
                if r is not None:
                    results.append(r)
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

            if results:
                log.info(f"  {len(results)} priced · {new_orders} orders")
                log.info(f"  {'DIR':<5} {'STRIKE':>10}  {'FAIR':>5}  {'MID':>5}  {'GAP':>5}  {'SIDE':>4}  {'LIMIT':>6}  {'EDGE':>6}  {'T':>5}  ACTION")
                for r in sorted(results, key=lambda x: x["net_edge"], reverse=True)[:20]:
                    log.info(
                        f"  {r['direction'].upper():<5} ${r['strike']:>9,.0f}"
                        f"  {r['fair']:>5.3f}  {r['book_mid']:>5.3f}  {r['gap']:>5.3f}"
                        f"  {r['side']:>4}  {r['limit']:>6.3f}  {r['net_edge']:>+6.3f}"
                        f"  {r['T_days']:>4.0f}d  {r['action'] or '-'}{'  ← ORDER' if r['traded'] else ''}"
                    )
            else:
                log.info(f"  [{asset}] No priceable markets (drift={drift*100:.0f}%)")

        # ── Hourly up/down markets (momentum-based, no strike) ────────────────
        log.info(f"\n{'─'*24} HOURLY MARKETS {'─'*24}")
        try:
            hourly_placed = scan_hourly_markets(client, om, bankroll)
            cycle_orders += hourly_placed
            if hourly_placed == 0:
                log.info("  No hourly edge this cycle.")
            # Register hourly fairs so stale hourly orders get cancelled too
            for m, asset, bsym in _collect_hourly_markets():
                raw = m.get("clobTokenIds") or []
                if isinstance(raw, str):
                    try: raw = json.loads(raw)
                    except: raw = []
                if len(raw) >= 2:
                    fu, fd = compute_hourly_fair(bsym)
                    current_fairs[raw[0]] = fu
                    current_fairs[raw[1]] = fd
        except Exception as e:
            log.debug(f"  Hourly scan error: {e}")

        if current_fairs:
            om.cancel_stale(client, current_fairs)

        if cycle_orders == 0:
            log.info("No orders this cycle.")

        pnl.maybe_report(om, bankroll)

        elapsed   = time.time() - cycle_start
        sleep_for = max(1.0, POLL_INTERVAL - elapsed)
        log.info(f"Cycle {elapsed:.1f}s — next in {sleep_for:.0f}s  |  {om.summary()}\n{'='*72}")

        try:
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            raise


def run() -> None:
    mode = "DRY RUN" if DRY_RUN else "LIVE TRADING"
    log.info("=" * 72)
    log.info(f"Polymarket Ultimate Edge Bot  —  {mode}")
    log.info(f"edge={EDGE_BUFFER*100:.0f}%  min_edge={MIN_EDGE*100:.1f}%  "
             f"fee={TAKER_FEE*100:.0f}%  Kelly={KELLY_FRACTION}  max=${MAX_BET_USDC}  "
             f"model_gap_limit={MAX_MODEL_MARKET_GAP*100:.0f}%")
    log.info("=" * 72)

    pk     = os.getenv("PK") or os.getenv("POLYMARKET_PRIVATE_KEY", "")
    wallet = get_wallet_address(pk)
    if wallet:
        log.info(f"Wallet: {wallet}")

    restart_delay = 30
    while True:
        try:
            client = build_client()
            run_loop(client, wallet)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            tg("🛑 <b>Bot stopped</b> (KeyboardInterrupt)")
            break
        except Exception as e:
            log.error(f"Crash: {e} — restarting in {restart_delay}s…")
            time.sleep(restart_delay)
            restart_delay = min(restart_delay * 2, 300)  # cap at 5 min
        else:
            break


if __name__ == "__main__":
    run()
