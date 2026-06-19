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
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

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
POLL_INTERVAL         = 15      # seconds between scans
EDGE_BUFFER           = 0.03    # place limit this far below fair (3%)
MIN_EDGE              = 0.03    # minimum net edge after fee (raised 2%→3% to match hourly quality bar)
TRADE_DAILY_MARKETS   = False   # daily WR=45% (losing); paused until win-rate improves above 50%
TAKER_FEE             = 0.02    # Polymarket taker fee on winnings
KELLY_FRACTION        = 0.35    # 35% Kelly — more aggressive sizing
MAX_BET_USDC          = 8.0     # hard cap per order — raised to let high-edge trades get full Kelly size
TAKE_PROFIT           = 0.40    # exit when position up ≥40% from entry
STOP_LOSS             = 0.20    # exit when position down ≥20% — tighter to exit before near-expiry collapse
MIN_BET_USDC          = 0.20    # skip orders below this ($0.50 blocked all kelly bets on $30 bankroll)
MIN_VOLUME            = 15_000  # raised 5k→15k: thin markets have poor price discovery
TRADED_RESET_HOURS    = 6       # full reset every N hours
STALE_FAIR_DRIFT      = 0.12    # cancel order if fair drifted >12%
PNL_REPORT_HOURS      = 24
MAX_MODEL_MARKET_GAP  = 0.20    # tightened 30%→20%: market usually right when model disagrees >20¢
MIN_DAILY_CONVICTION  = 0.63    # daily markets need ≥63% fair — tightened given 33% observed win rate
MIN_BOOK_LIQUIDITY    = 0.01    # skip markets with spread wider than 99 cents
MAX_BOOK_SPREAD       = 0.25    # require a real two-sided book on regular markets too
POSITION_SYNC_MINS    = 10      # re-sync held positions from Data API every N minutes
MAX_POSITION_TOKENS   = 50      # max distinct token positions held at once
MAX_POSITIONS_PER_ASSET = 3    # max open positions in the same asset (correlated risk)
MANUAL_BANKROLL       = float(os.getenv("MANUAL_BANKROLL", "0"))  # override balance check if set

# ── Drawdown protection ────────────────────────────────────────────────────────
DRAWDOWN_WARN_PCT  = 0.20   # 20% from peak → halve Kelly + Telegram alert
DRAWDOWN_PAUSE_PCT = 0.30   # 30% from peak → halt trading entirely

# ── Rolling win-rate (dynamic Kelly) ──────────────────────────────────────────
WINRATE_WINDOW     = 30     # look back over the last N resolved positions
WINRATE_MIN_SAMPLE = 5      # need at least this many before adjusting Kelly

# ── Exit tuning ────────────────────────────────────────────────────────────────
TAKE_PROFIT_2       = 0.70  # second half of partial TP exits at +70%
TRAIL_STOP_TRIGGER  = 0.20  # start trailing once position is up ≥20% — protect gains sooner
TRAIL_STOP_DROP     = 0.10  # exit if bid drops 10% below its peak while trailing
LOSS_COOLDOWN_HOURS = 1.0   # block same asset+direction for 1h after a stop-loss (faster recovery)

# ── Order hygiene ──────────────────────────────────────────────────────────────
MAX_ORDER_AGE_HOURS = 3.0   # cancel unfilled GTC orders older than this

# ── Telegram ───────────────────────────────────────────────────────────────────
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Fear & Greed Index ─────────────────────────────────────────────────────────
# alternative.me/fng/ — free, no API key, updates daily.
# Score 0–100: 0=Extreme Fear, 50=Neutral, 100=Extreme Greed.
# Used as a market-wide crypto sentiment signal (applies to all assets equally).
_FNG_CACHE: list = [0.0, 0.0]   # [score_0_to_100, timestamp]
_FNG_TTL   = 3600                # refresh once per hour (index updates daily)


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


_tg_offset: int = 0

def _poll_tg_commands() -> list[str]:
    """Poll Telegram for incoming /commands from the configured chat. Returns list of command strings."""
    global _tg_offset
    if not TG_TOKEN or not TG_CHAT:
        return []
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
            params={"offset": _tg_offset, "limit": 20, "timeout": 0},
            timeout=8,
        ).json()
        commands: list[str] = []
        for update in resp.get("result", []):
            _tg_offset = update["update_id"] + 1
            msg  = update.get("message", {})
            text = msg.get("text", "").strip().lower()
            chat_id = str(msg.get("chat", {}).get("id", ""))
            if chat_id == TG_CHAT and text.startswith("/"):
                commands.append(text.split()[0])  # just the command, ignore args
        return commands
    except Exception:
        return []


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
    "XRP": {
        "vol":            0.75,
        "drift":          0.10,
        "binance_symbol": "XRP",
        "coingecko_id":   "ripple",
        "keywords":       ["xrp", "ripple"],
        "daily_names":    ["xrp", "ripple"],
        "slugs":          [],   # no long-term price target markets
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "HYPE": {
        "vol":            1.20,
        "drift":          0.30,
        "binance_symbol": "HYPE",
        "coingecko_id":   "hyperliquid",
        "keywords":       ["hyperliquid"],
        "daily_names":    ["hyperliquid", "hype"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "DOGE": {
        "vol":            0.90,
        "drift":          0.15,
        "binance_symbol": "DOGE",
        "coingecko_id":   "dogecoin",
        "keywords":       ["dogecoin", "doge"],
        "daily_names":    ["dogecoin", "doge"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "AVAX": {
        "vol":            0.80,
        "drift":          0.15,
        "binance_symbol": "AVAX",
        "coingecko_id":   "avalanche-2",
        "keywords":       ["avalanche", "avax"],
        "daily_names":    ["avalanche", "avax"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "LINK": {
        "vol":            0.85,
        "drift":          0.10,
        "binance_symbol": "LINK",
        "coingecko_id":   "chainlink",
        "keywords":       ["chainlink", "link"],
        "daily_names":    ["chainlink", "link"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "SUI": {
        "vol":            0.95,
        "drift":          0.30,
        "binance_symbol": "SUI",
        "coingecko_id":   "sui",
        "keywords":       ["sui"],
        "daily_names":    ["sui"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "PEPE": {
        "vol":            1.20,
        "drift":          0.20,
        "binance_symbol": "PEPE",
        "coingecko_id":   "pepe",
        "keywords":       ["pepe"],
        "daily_names":    ["pepe"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "ADA": {
        "vol":            0.70,
        "drift":          0.10,
        "binance_symbol": "ADA",
        "coingecko_id":   "cardano",
        "keywords":       ["cardano", "ada"],
        "daily_names":    ["cardano", "ada"],
        "slugs":          [],
        "end_date": datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
    },
    "TON": {
        "vol":            0.80,
        "drift":          0.15,
        "binance_symbol": "TON",
        "coingecko_id":   "the-open-network",
        "keywords":       ["toncoin", "ton"],
        "daily_names":    ["toncoin", "ton"],
        "slugs":          [],
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
BOOK_TTL  = 25    # longer than POLL_INTERVAL so cache survives across cycles
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
        # Polymarket sorts book arrays with the BEST price LAST (worst first)
        best_bid = max(float(b["price"]) for b in bids) if bids else 0.0
        best_ask = min(float(a["price"]) for a in asks) if asks else 1.0
        mid      = round((best_bid + best_ask) / 2, 4)
        # liquid = True when there is a real two-sided book (spread < 95 cents)
        # The old check used (1.0 - MIN_BOOK_LIQUIDITY) = 0.99, which accepted dead
        # 0.01/0.99 books (spread = 0.98 ≤ 0.99). Now we require spread < 0.95.
        liquid   = bool(bids and asks and best_bid > 0 and best_ask < 1
                        and (best_ask - best_bid) < 0.95)
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


def get_existing_positions(wallet: str) -> dict[str, float] | None:
    """
    Fetch held positions as {token_id: avg_entry_price}. Skips these on entry.
    Entry price from the API lets exit logic work across bot restarts.
    NOTE: wallet must be the PROXY address — positions live under it, not the EOA.
    Returns None on API failure (vs {} for a genuinely empty account).
    """
    if not wallet:
        return {}
    try:
        data = retry_get(
            f"{DATA_HOST}/positions",
            params={"user": wallet, "sizeThreshold": "0.01"},
            timeout=12,
        ).json()
        held: dict[str, float] = {}
        for p in (data if isinstance(data, list) else []):
            tid  = p.get("asset") or p.get("token_id") or p.get("assetId", "")
            size = float(p.get("size") or p.get("position") or 0)
            if tid and size > 0.01:
                held[tid] = float(p.get("avgPrice") or p.get("avg_price") or 0)
        log.info(f"  Existing positions loaded: {len(held)} token(s)")
        return held
    except Exception as e:
        log.warning(f"Could not load positions: {e}")
        return None


def get_recent_buys(wallet: str, hours: float = 2.0) -> dict[str, float]:
    """
    Net recently-bought tokens from the activity feed: {token_id: avg_buy_price}.
    The activity feed indexes near-instantly, unlike /positions which can lag
    minutes behind — without this, a restart right after a fill makes the bot
    forget the position and buy it again.
    """
    if not wallet:
        return {}
    try:
        data = retry_get(
            f"{DATA_HOST}/activity",
            params={"user": wallet, "limit": "60"},
            timeout=12,
        ).json()
        cutoff = time.time() - hours * 3600
        net:    dict[str, float] = {}   # token → net shares (buys − sells)
        bought: dict[str, float] = {}   # token → total shares bought
        cost:   dict[str, float] = {}   # token → total buy cost
        for a in (data if isinstance(data, list) else []):
            if a.get("type") != "TRADE" or float(a.get("timestamp") or 0) < cutoff:
                continue
            tid   = a.get("asset") or a.get("token_id", "")
            size  = float(a.get("size") or 0)
            price = float(a.get("price") or 0)
            if not tid or size <= 0:
                continue
            if a.get("side") == "BUY":
                net[tid]    = net.get(tid, 0) + size
                bought[tid] = bought.get(tid, 0) + size
                cost[tid]   = cost.get(tid, 0) + size * price
            else:
                net[tid]  = net.get(tid, 0) - size
        recent = {}
        for tid, shares in net.items():
            if shares > 0.01 and bought.get(tid, 0) > 0:
                avg_buy = cost[tid] / bought[tid]   # true avg buy price
                recent[tid] = round(min(0.99, avg_buy), 4)
        if recent:
            log.info(f"  Recent buys merged from activity feed: {len(recent)} token(s)")
        return recent
    except Exception as e:
        log.debug(f"get_recent_buys: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGER
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TrackedOrder:
    order_id:    str
    token_id:    str
    label:       str
    limit:       float
    size_usdc:   float
    fair:        float
    placed_at:   float = field(default_factory=time.time)
    market_end:  float = 0.0   # unix ts of market expiry (0 = unknown / non-expiring)


class OrderManager:
    def __init__(self, existing_positions: dict[str, float] = None):
        self.orders:          dict[str, TrackedOrder] = {}
        self.open_token_ids:  set[str] = set()
        # token_id → avg entry price (0.0 = unknown, exit logic skips those)
        self.held_positions:  dict[str, float] = dict(existing_positions or {})
        self.held_labels:     dict[str, str]   = {}   # token_id → market label for win-rate tracking
        self.held_market_end: dict[str, float] = {}   # token_id → expiry unix ts (hourly only)
        self.recent_fill_ts:  dict[str, float] = {}   # token_id → fill time
        self.peak_bid:        dict[str, float] = {}   # token_id → highest bid seen (trailing stop)
        self.partial_exit_done: set[str]       = set()  # tokens where first half already sold
        self.fills_usdc:      float = 0.0
        self.fill_count:      int   = 0
        self.order_count:     int   = 0

    def sync_positions(self, wallet: str) -> None:
        """
        Re-sync held positions from the Data API so memory matches reality —
        catches unfilled exit sells, manual trades, and fills missed across
        restarts. Fills from the last 15 min are protected from removal
        (the API may not have indexed them yet).
        """
        api = get_existing_positions(wallet)
        if api is None:
            return  # API failure — keep current state
        now = time.time()
        for tid, entry in api.items():
            if tid not in self.held_positions:
                # Position found in API but not in memory — could be from days ago
                # (restart/rediscovery). Do NOT stamp recent_fill_ts here; only
                # refresh_from_api stamps it for confirmed fresh fills. This avoids
                # blocking exit logic for 15 min on positions held for days.
                self.held_positions[tid] = entry
            elif self.held_positions[tid] <= 0:
                self.held_positions[tid] = entry
        for tid in list(self.held_positions):
            if tid not in api and now - self.recent_fill_ts.get(tid, 0) > 900:
                self.held_positions.pop(tid)
                log.info(f"  Position sync: {tid[:12]}… no longer held — removed")

    @property
    def held_token_ids(self) -> set[str]:
        return set(self.held_positions.keys())

    def record(self, order_id: str, token_id: str, label: str,
               limit: float, size_usdc: float, fair: float,
               market_end: float = 0.0) -> None:
        self.orders[order_id]  = TrackedOrder(order_id, token_id, label, limit, size_usdc, fair,
                                              market_end=market_end)
        self.open_token_ids.add(token_id)
        self.order_count      += 1

    def has_open_order(self, token_id: str) -> bool:
        return token_id in self.open_token_ids

    def already_holds(self, token_id: str) -> bool:
        return token_id in self.held_positions

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
                        # Use actual average fill price when available — limit price is
                        # only an approximation (partial fills may average lower)
                        fill_price = float(
                            detail.get("average_price") or detail.get("avg_price") or
                            detail.get("price") or tracked.limit
                        )
                        fill_price = max(0.01, min(0.99, fill_price))
                        filled_usdc = round(size_matched * fill_price, 2)
                        self.fills_usdc += filled_usdc
                        self.fill_count += 1
                        self.held_positions[tracked.token_id] = fill_price  # true entry price
                        self.held_labels[tracked.token_id]   = tracked.label
                        self.recent_fill_ts[tracked.token_id] = time.time()
                        if tracked.market_end > 0:
                            self.held_market_end[tracked.token_id] = tracked.market_end
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

    def cancel_aged(self, client: ClobClient) -> None:
        """Cancel unfilled GTC orders older than MAX_ORDER_AGE_HOURS."""
        if DRY_RUN:
            return
        cutoff = time.time() - MAX_ORDER_AGE_HOURS * 3600
        for oid, tracked in list(self.orders.items()):
            if tracked.placed_at < cutoff:
                age_h = (time.time() - tracked.placed_at) / 3600
                log.info(f"  Cancelling aged order {oid[:10]} ({age_h:.1f}h old): {tracked.label[:30]}")
                try:
                    client.cancel_order(OrderPayload(orderID=oid))
                    self.orders.pop(oid, None)
                    self.open_token_ids.discard(tracked.token_id)
                except Exception as e:
                    log.warning(f"  Cancel aged {oid[:10]} failed: {e}")

    def cancel_expiring_hourly(self, client: ClobClient, mins_before: float = 5.0) -> None:
        """Cancel open orders for hourly markets that expire within `mins_before` minutes.
        Prevents orphaned GTC orders that can never fill once the market closes."""
        if DRY_RUN:
            return
        cutoff = time.time() + mins_before * 60
        for oid, tracked in list(self.orders.items()):
            if tracked.market_end <= 0 or tracked.market_end > cutoff:
                continue
            mins_left = (tracked.market_end - time.time()) / 60
            log.info(f"  Cancelling expiring hourly order {oid[:10]} "
                     f"({mins_left:.0f}m left): {tracked.label[:30]}")
            try:
                client.cancel_order(OrderPayload(orderID=oid))
                self.orders.pop(oid, None)
                self.open_token_ids.discard(tracked.token_id)
            except Exception as e:
                log.warning(f"  Cancel expiring {oid[:10]} failed: {e}")

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
# EXIT LOGIC — take-profit and stop-loss on held positions
# ══════════════════════════════════════════════════════════════════════════════

_pos_size_cache: dict[str, tuple[float, float]] = {}  # token_id → (ts, shares)
_POS_SIZE_TTL = 60.0


def _get_position_size(wallet: str, token_id: str) -> float:
    """
    Query actual share count held for a token from the Data API.
    Falls back to the 5-share minimum if the API can't be reached.
    """
    if not wallet:
        return 5.0
    now = time.time()
    cached = _pos_size_cache.get(token_id)
    if cached and now - cached[0] < _POS_SIZE_TTL:
        return cached[1]
    try:
        data = retry_get(
            f"{DATA_HOST}/positions",
            params={"user": wallet, "asset": token_id, "sizeThreshold": "0.01"},
            timeout=8,
        ).json()
        for p in (data if isinstance(data, list) else []):
            tid  = p.get("asset") or p.get("token_id") or p.get("assetId", "")
            size = float(p.get("size") or p.get("position") or 0)
            if tid == token_id and size > 0:
                _pos_size_cache[token_id] = (now, size)
                log.debug(f"  Position size [{token_id[:12]}…]: {size:.2f} shares")
                return size
    except Exception as e:
        log.debug(f"  _get_position_size failed: {e}")
    return max(5.0, _pos_size_cache.get(token_id, (0, 5.0))[1])  # last known or minimum


def scan_exit_positions(client: ClobClient, om: OrderManager, wallet: str = "") -> None:
    """
    Exit logic with four tiers:
    - Trailing stop: if position ever gains ≥25%, sell when bid retreats 15% from peak
    - Partial TP:    sell half at +40% (hourly only, if ≥10 shares); let rest ride
    - Final TP:      sell remaining half at +70%
    - Stop-loss:     sell full position at -40% (-24% for second half)
    Records loss cooldowns so the same direction isn't re-entered for 1.5h.
    """
    if DRY_RUN:
        return
    for token_id, entry in list(om.held_positions.items()):
        if entry <= 0:
            continue
        if time.time() - om.recent_fill_ts.get(token_id, 0) < 900:
            continue  # held <15 min — entry price may not be settled
        try:
            bid, ask, _, liquid = get_order_book(token_id)
            if not liquid or bid < 0.05:
                continue
            if (ask - bid) > 0.30:
                continue

            # ── Track peak bid for trailing stop ──────────────────────────
            current_peak = max(bid, om.peak_bid.get(token_id, entry))
            om.peak_bid[token_id] = current_peak

            gain      = (bid - entry) / entry
            peak_gain = (current_peak - entry) / entry
            label     = om.held_labels.get(token_id, "")
            is_hourly = "hourly" in label.lower() or " et " in label.lower()
            is_partial = token_id in om.partial_exit_done

            # ── Hold-to-expiry zone (hourly markets only) ─────────────────
            # Within 10 min of expiry: selling costs taker fee + bid-ask spread
            # for the same expected value as holding. Settlement is exact (no
            # slippage) and fee-free — always better than selling early.
            market_end = om.held_market_end.get(token_id, 0.0)
            if is_hourly and market_end > 0 and (market_end - time.time()) < 600:
                log.debug(f"  Hold to expiry {token_id[:12]}…: <10m left, skip exit")
                continue

            reason   = None
            sell_all = True

            if not is_partial:
                # ── Trailing stop (kicks in after 25% gain, hourly only) ──
                if is_hourly and peak_gain >= TRAIL_STOP_TRIGGER:
                    if bid <= current_peak * (1 - TRAIL_STOP_DROP):
                        reason = (f"trailing-stop "
                                  f"(peak={current_peak:.3f} → bid={bid:.3f})")
                # ── Partial TP at +40% (hourly, ≥10 shares) ──────────────
                if reason is None and gain >= TAKE_PROFIT:
                    if is_hourly:
                        reason   = f"partial take-profit (+{gain*100:.0f}%)"
                        sell_all = False   # sell half, leave the rest
                    else:
                        reason = f"take-profit (+{gain*100:.0f}%)"
                # ── Standard stop-loss ────────────────────────────────────
                if reason is None and gain <= -STOP_LOSS:
                    reason = f"stop-loss ({gain*100:.0f}%)"
            else:
                # Second half: trailing stop or higher TP or tighter SL
                if is_hourly and peak_gain >= TRAIL_STOP_TRIGGER:
                    if bid <= current_peak * (1 - TRAIL_STOP_DROP):
                        reason = (f"final trailing-stop "
                                  f"(peak={current_peak:.3f} → bid={bid:.3f})")
                if reason is None and gain >= TAKE_PROFIT_2:
                    reason = f"final take-profit (+{gain*100:.0f}%)"
                if reason is None and gain <= -(STOP_LOSS * 0.60):
                    reason = f"final stop-loss ({gain*100:.0f}%)"

            if reason is None:
                continue

            # ── Determine shares to sell ───────────────────────────────────
            shares = round(_get_position_size(wallet, token_id), 2)
            if shares < 5.0:
                log.info(f"  Skip exit {token_id[:12]}…: too small "
                         f"({shares:.2f}sh < 5 CLOB minimum)")
                continue

            if not sell_all:
                half = math.floor(shares / 2)
                # If we can't leave ≥5 shares behind, just sell everything
                if half < 5 or (shares - half) < 5:
                    sell_all = True
                    reason   = reason.replace("partial ", "")
                else:
                    shares = float(half)

            sell_price = min(0.99, max(0.01, math.floor(bid * 100) / 100))
            log.info(f"  🔴 EXIT {token_id[:12]}… entry={entry:.3f} bid={bid:.3f} "
                     f"shares={shares:.2f} → {reason}")
            resp = client.create_and_post_order(
                order_args=OrderArgs(token_id=token_id, price=sell_price,
                                     side=Side.SELL, size=shares),
                options=PartialCreateOrderOptions(tick_size=TICK_SIZE),
                order_type=OrderType.GTC,
            )
            if resp and resp.get("success"):
                log.info(f"    ✓ Exit order placed: {resp.get('orderID','')[:20]}")
                pnl_usdc  = round((sell_price - entry) * shares, 2)
                pnl_emoji = "🟢" if pnl_usdc >= 0 else "🔴"
                tg(f"{pnl_emoji} <b>EXIT</b> {reason}\n"
                   f"{shares:.2f}sh  entry={entry:.3f} → sell@{sell_price:.3f}\n"
                   f"P&L: <b>{pnl_usdc:+.2f} USDC</b>  ({gain*100:+.1f}%)\n"
                   f"{label or token_id[:16]}")
                _wr_tracker.record_exit(label, won=(gain >= 0))
                if sell_all:
                    om.held_positions.pop(token_id, None)
                    om.held_labels.pop(token_id, None)
                    om.held_market_end.pop(token_id, None)
                    om.peak_bid.pop(token_id, None)
                    om.partial_exit_done.discard(token_id)
                    _pos_size_cache.pop(token_id, None)
                    if "stop-loss" in reason:
                        ck = _cooldown_key(label)
                        if ck:
                            _loss_cooldown[ck] = time.time()
                            log.info(f"  Loss cooldown set: {ck} for "
                                     f"{LOSS_COOLDOWN_HOURS}h")
                            tg(f"⏳ Loss cooldown: <b>{ck}</b> blocked for "
                               f"{LOSS_COOLDOWN_HOURS}h")
                else:
                    om.partial_exit_done.add(token_id)
                    _pos_size_cache.pop(token_id, None)
            else:
                log.warning(f"    ✗ Exit rejected: {resp}")
        except Exception as e:
            log.debug(f"  Exit scan {token_id[:12]}: {e}")


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
        raw = float(
            resp.get("balance") or resp.get("available") or
            resp.get("allowance") or resp.get("amount") or 0
        )
        # CLOB always returns balance in raw token units (6 decimals USDC)
        balance = raw / 1e6
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
    # Polymarket names daily markets by ET date — use ET, not UTC
    slugs, et = [], _et_now()
    for i in range(5):
        d    = et + timedelta(days=i)
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
# DRAWDOWN GUARD
# ══════════════════════════════════════════════════════════════════════════════

class DrawdownGuard:
    """Tracks peak bankroll and reduces / halts trading during drawdown."""

    def __init__(self):
        self.peak   = 0.0
        self._state = "ok"   # "ok" | "warn" | "pause"

    def update(self, bankroll: float) -> None:
        if bankroll > self.peak:
            if self._state != "ok":
                log.info(f"  ✅ Drawdown guard: recovered to ${bankroll:.2f} "
                         f"(was {self._state}, peak=${self.peak:.2f})")
                tg(f"✅ <b>Drawdown recovered</b>\nBankroll: ${bankroll:.2f}")
            self.peak   = bankroll
            self._state = "ok"

    def kelly_multiplier(self, bankroll: float) -> float:
        if self.peak <= 0:
            return 1.0
        dd = (self.peak - bankroll) / self.peak
        if dd >= DRAWDOWN_PAUSE_PCT:
            if self._state != "pause":
                self._state = "pause"
                log.warning(f"  🚨 DRAWDOWN PAUSE: {dd*100:.0f}% below peak "
                            f"${self.peak:.2f} → trading halted")
                tg(f"🚨 <b>DRAWDOWN PAUSE</b> ({dd*100:.0f}% from peak)\n"
                   f"Bankroll ${bankroll:.2f} vs peak ${self.peak:.2f}\n"
                   f"Trading halted until bankroll recovers above "
                   f"${self.peak * (1 - DRAWDOWN_WARN_PCT):.2f}")
            return 0.0
        if dd >= DRAWDOWN_WARN_PCT:
            if self._state == "ok":
                self._state = "warn"
                log.warning(f"  ⚠️ DRAWDOWN WARNING: {dd*100:.0f}% below peak "
                            f"${self.peak:.2f} → Kelly halved")
                tg(f"⚠️ <b>Drawdown warning</b> ({dd*100:.0f}% from peak)\n"
                   f"Bankroll ${bankroll:.2f} vs peak ${self.peak:.2f}\n"
                   f"Bet sizing halved until recovery")
            return 0.5
        return 1.0

    @property
    def is_paused(self) -> bool:
        return self._state == "pause"


# ══════════════════════════════════════════════════════════════════════════════
# WIN-RATE TRACKER  (per market type, rolling window)
# ══════════════════════════════════════════════════════════════════════════════

class WinRateTracker:
    """Records exit outcomes and adjusts Kelly fraction based on recent performance."""

    def __init__(self):
        self._history: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINRATE_WINDOW))

    @staticmethod
    def _market_type(label: str) -> str:
        lbl = (label or "").lower()
        if "hourly" in lbl or " et " in lbl:
            return "hourly"
        if "arb" in lbl:
            return "arb"
        return "daily"

    def record_exit(self, label: str, won: bool) -> None:
        mtype = self._market_type(label)
        self._history[mtype].append(won)
        self._history["all"].append(won)
        wr = self.win_rate("all")
        log.info(f"  📊 Win-rate update [{mtype}] {'WIN' if won else 'LOSS'} — "
                 f"all:{wr*100:.0f}% over {len(self._history['all'])} trades"
                 if wr is not None else f"  📊 Exit recorded [{mtype}]")
        _save_state()  # persist immediately so restarts don't lose this result

    def win_rate(self, mtype: str = "all") -> float | None:
        h = self._history[mtype]
        return sum(h) / len(h) if len(h) >= WINRATE_MIN_SAMPLE else None

    def kelly_multiplier(self, mtype: str = "all") -> float:
        wr = self.win_rate(mtype)
        if wr is None and mtype != "all":
            wr = self.win_rate("all")  # fall back to overall when type sample too small
        if wr is None:
            return 1.0   # not enough data — don't penalise yet
        if wr >= 0.60:
            return 1.0   # performing well — full Kelly
        if wr >= 0.50:
            return 0.75  # slight underperformance
        if wr >= 0.40:
            return 0.50  # clearly underperforming — half Kelly
        return 0.25      # poor run — near-minimum sizing

    def summary(self) -> str:
        parts = []
        for mtype in ("hourly", "daily", "arb"):
            h = self._history[mtype]
            if h:
                wr = sum(h) / len(h)
                parts.append(f"{mtype}:{wr*100:.0f}%({len(h)})")
        wr_all = self.win_rate()
        prefix = f"all:{wr_all*100:.0f}% | " if wr_all is not None else ""
        return prefix + " | ".join(parts) if (prefix or parts) else "no data yet"


_dd_guard   = DrawdownGuard()
_wr_tracker = WinRateTracker()
# Updated at the start of each cycle; used for logging and per-type sizing.
_effective_kelly = KELLY_FRACTION
_dd_mult: float  = 1.0   # cached drawdown multiplier — used by kelly_buy

# Loss cooldown: maps "BTC_UP" / "ETH_DOWN" etc. → timestamp of last stop-loss
_loss_cooldown: dict[str, float] = {}

# ── State persistence ──────────────────────────────────────────────────────────
# Survives process crashes and container restarts within the same Railway deploy.
# Lost on new deploys (code pushes) — add a Railway Volume for full persistence.
_STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")


def _save_state() -> None:
    """Write win-rate history, loss cooldowns and DD peak to disk."""
    try:
        now = time.time()
        data = {
            "winrate":  {k: list(v) for k, v in _wr_tracker._history.items()},
            "cooldown": {k: v for k, v in _loss_cooldown.items()
                         if now - v < LOSS_COOLDOWN_HOURS * 3600},
            "dd_peak":  _dd_guard.peak,
            "saved_at": now,
        }
        with open(_STATE_FILE, "w") as fh:
            json.dump(data, fh)
    except Exception as e:
        log.debug(f"State save failed: {e}")


def _load_state() -> None:
    """Restore win-rate history, loss cooldowns and DD peak from disk."""
    try:
        with open(_STATE_FILE) as fh:
            data = json.load(fh)
        # Win-rate history
        for mtype, outcomes in data.get("winrate", {}).items():
            for outcome in outcomes[-WINRATE_WINDOW:]:
                _wr_tracker._history[mtype].append(bool(outcome))
        # Loss cooldowns — skip any that have already expired
        now = time.time()
        for key, ts in data.get("cooldown", {}).items():
            if now - float(ts) < LOSS_COOLDOWN_HOURS * 3600:
                _loss_cooldown[key] = float(ts)
        # Drawdown peak
        peak = float(data.get("dd_peak", 0.0))
        if peak > 0:
            _dd_guard.peak = peak
        age_h = (now - float(data.get("saved_at", now))) / 3600
        log.info(f"  🔄 State restored (saved {age_h:.1f}h ago): "
                 f"win-rate={_wr_tracker.summary()}  "
                 f"cooldowns={len(_loss_cooldown)}  dd_peak=${_dd_guard.peak:.2f}")
    except FileNotFoundError:
        log.info("  No saved state — starting fresh")
    except Exception as e:
        log.warning(f"  State load failed: {e}")


def _cooldown_key(label: str) -> str | None:
    """Extract 'ASSET_DIR' key from a label like 'BTC UP 2pm ET (hourly)'."""
    u = label.upper()
    for asset in ("BTC", "ETH", "SOL", "XRP", "HYPE"):
        if asset in u:
            if " UP " in u or u.endswith("UP") or "(HOURLY)" in u and "UP" in u:
                return f"{asset}_UP"
            if " DOWN " in u or u.endswith("DOWN"):
                return f"{asset}_DOWN"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# KELLY SIZING
# ══════════════════════════════════════════════════════════════════════════════

def kelly_buy(fair: float, limit: float, bankroll: float,
              mtype: str = "all", max_bet: float = 0.0) -> float:
    if limit <= 0 or limit >= 1 or fair * (1 - TAKER_FEE) <= limit:
        return 0.0
    f_star = (fair * (1 - TAKER_FEE) - limit) / (1 - limit)
    # Per-type Kelly: each market type gets its own win-rate multiplier so a bad
    # run in daily markets doesn't unnecessarily shrink hourly bet sizes and vice versa.
    kelly = KELLY_FRACTION * _dd_mult * _wr_tracker.kelly_multiplier(mtype)
    cap = max_bet if max_bet > 0 else MAX_BET_USDC
    return round(min(cap, max(0.0, kelly * f_star * bankroll)), 2)


# ══════════════════════════════════════════════════════════════════════════════
# CLOB CLIENT
# ══════════════════════════════════════════════════════════════════════════════

def _get_proxy_wallet_address(eoa: str) -> str | None:
    """
    Try to discover the Polymarket proxy wallet address for this EOA.
    The proxy address is needed as 'funder' for POLY_PROXY / POLY_1271 orders.
    """
    # Method 1: Gamma API accounts endpoint
    _PROXY_KEYS = ("proxyWallet", "proxy_wallet", "proxy", "maker",
                   "depositAddress", "deposit_address", "address")

    def _extract(data) -> str | None:
        item = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
        for key in _PROXY_KEYS:
            val = item.get(key)
            if val and str(val).lower() not in (eoa.lower(), "", "0x0",
                                                "0x0000000000000000000000000000000000000000"):
                log.info(f"  Found proxy [{key}]: {val}")
                return val
        return None

    for label, url in [("Gamma", f"{GAMMA_HOST}/accounts"),
                       ("Data",  f"{DATA_HOST}/accounts")]:
        for attempt in range(3):
            try:
                resp = requests.get(url, params={"address": eoa}, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    log.info(f"  {label} accounts response: {data}")
                    result = _extract(data)
                    if result:
                        return result
                break  # non-200 but reachable → no point retrying
            except Exception as e:
                log.info(f"  {label} attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(1.5 ** attempt)

    return None


def build_client() -> ClobClient:
    pk = os.getenv("PK") or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not pk: raise EnvironmentError("PK not set in .env")

    from eth_account import Account as _Acct
    from py_clob_client_v2 import SignatureTypeV2
    wallet = _Acct.from_key(pk).address
    log.info(f"Wallet (EOA): {wallet}")

    # ── Step 1: Derive EOA creds (needed to authenticate balance scan) ────────
    log.info("Deriving EOA API credentials for balance scan…")
    eoa_tmp = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk,
                         signature_type=0)
    eoa_creds = eoa_tmp.create_or_derive_api_key()

    # ── Step 2: Detect which signature type holds the funds ──────────────────
    log.info("Scanning all signature types to find funded account…")
    detected_sig_type = None
    raw_bal_response = {}
    for st in [SignatureTypeV2.EOA, SignatureTypeV2.POLY_PROXY,
               SignatureTypeV2.POLY_GNOSIS_SAFE, SignatureTypeV2.POLY_1271]:
        try:
            probe = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk,
                               creds=eoa_creds, signature_type=int(st))
            bal = probe.get_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL,
                                              signature_type=int(st))
            )
            log.info(f"  Balance raw [{st.name}]: {bal}")   # log full dict
            usdc = float(bal.get("balance", 0)) / 1e6
            log.info(f"  Balance [{st.name}]: ${usdc:.2f}")
            if usdc > 0 and detected_sig_type is None:
                detected_sig_type = int(st)
                raw_bal_response = bal
                log.info(f"  ✓ Funds found with {st.name} — using this mode")
        except Exception as e:
            log.info(f"  Balance [{st.name}]: error: {e}")

    if detected_sig_type is None:
        log.warning("⚠️  CLOB shows $0 across all signature types.")
        detected_sig_type = 0

    # ── Step 3: Discover proxy wallet address (needed as 'funder') ───────────
    # For POLY_PROXY/POLY_1271: order.maker and order.signer must be the proxy
    # contract address, not the EOA. Without it orders get "maker not allowed".
    funder = None
    if detected_sig_type != 0:
        # Try to extract proxy addr from the balance response itself
        for key in ("proxyWallet", "proxy_wallet", "proxy", "maker", "address"):
            val = raw_bal_response.get(key)
            if val and str(val).lower() not in (wallet.lower(), "", "0x0",
                                                "0x0000000000000000000000000000000000000000"):
                funder = val
                log.info(f"  Proxy address from balance response [{key}]: {funder}")
                break

        if not funder:
            log.info("  Proxy not in balance response — querying Polymarket APIs…")
            funder = _get_proxy_wallet_address(wallet)

        if funder:
            log.info(f"  Using funder (proxy wallet): {funder}")
        else:
            log.warning("  ⚠️ Could not determine proxy wallet address. "
                        "Orders may fail — set POLY_FUNDER env var to your proxy address.")
            funder = os.getenv("POLY_FUNDER")  # manual override escape hatch

    # ── Step 4: Derive API key with the matching signature type ───────────────
    if detected_sig_type == 0:
        creds = eoa_creds
        log.info("Using EOA API credentials for trading.")
    else:
        log.info(f"Re-deriving API credentials for signature_type={detected_sig_type}…")
        typed_tmp = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk,
                               signature_type=detected_sig_type, funder=funder)
        creds = typed_tmp.create_or_derive_api_key()

    client = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk, creds=creds,
                        signature_type=detected_sig_type, funder=funder)

    log.info(f"Client ready — sig_type={detected_sig_type}, funder={funder or wallet}")

    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    except Exception:
        pass

    return client


# ══════════════════════════════════════════════════════════════════════════════
# ORDER PLACEMENT
# ══════════════════════════════════════════════════════════════════════════════

# Polymarket enforces 60 orders/minute per API key
_order_ts_window: list[float] = []
_RATE_LIMIT_PER_MIN = 55  # stay 5 under the hard cap


def _rate_limit_ok() -> bool:
    now = time.time()
    _order_ts_window[:] = [t for t in _order_ts_window if now - t < 60]
    if len(_order_ts_window) >= _RATE_LIMIT_PER_MIN:
        log.warning(f"  Rate limit: {len(_order_ts_window)} orders in last 60s — skipping")
        return False
    _order_ts_window.append(now)
    return True


def place_order(client: ClobClient, om: OrderManager,
                token_id: str, limit: float, size_usdc: float,
                fair: float, label: str, market_end: float = 0.0) -> bool:
    POLY_MIN_SHARES = 5                    # Polymarket CLOB rejects orders below 5 shares
    shares = round(size_usdc / limit, 4)
    if shares < POLY_MIN_SHARES:
        shares = float(POLY_MIN_SHARES)
        size_usdc = round(shares * limit, 4)  # bump USDC to afford minimum
    if size_usdc < 0.01:                  # dust guard
        return False

    log.info(f"{'[DRY] ' if DRY_RUN else ''}BUY LIMIT  {label}  {shares}sh @ {limit:.3f}  (${size_usdc:.2f})")

    if DRY_RUN:
        return True

    if not _rate_limit_ok():
        return False

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
                om.record(order_id, token_id, label, limit, size_usdc, fair,
                          market_end=market_end)
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

    # ── Per-asset concentration cap ────────────────────────────────────────────
    asset_positions = sum(1 for lbl in om.held_labels.values() if asset in lbl)
    if asset_positions >= MAX_POSITIONS_PER_ASSET:
        log.debug(f"  Skip {asset}: already holding {asset_positions} positions (cap={MAX_POSITIONS_PER_ASSET})")
        return None

    # ── Parse question ─────────────────────────────────────────────────────────
    end_date = parse_end_date(question, cfg["end_date"])
    sigs     = parse_question(question, spot, end_date)
    if not sigs:
        return None

    T = sigs[0]["T"]
    vol = get_live_vol(cfg["binance_symbol"], cfg["vol"], T)

    # ── Price model ───────────────────────────────────────────────────────────
    if len(sigs) == 2:
        # "Between $lo and $hi" market — price as P(lo < S_T < hi)
        lo_sig = next((s for s in sigs if s["direction"] == "down"), sigs[1])
        hi_sig = next((s for s in sigs if s["direction"] == "up"),   sigs[0])
        lo, hi = lo_sig["strike"], hi_sig["strike"]
        if not (0.15 * spot < lo < hi < 6.0 * spot):
            return None
        p_above_lo = european_prob(spot, lo, T, vol, drift, "up")
        p_above_hi = european_prob(spot, hi, T, vol, drift, "up")
        if p_above_lo is None or p_above_hi is None:
            return None
        fair      = round(max(0.03, min(0.97, p_above_lo - p_above_hi)), 4)
        direction = "up"
        strike    = (lo + hi) / 2  # midpoint for display
        model     = "between"
        label     = f"{asset} BETWEEN ${lo:,.0f}–${hi:,.0f} T={T*365:.0f}d"
    else:
        sig       = sigs[0]
        direction, strike = sig["direction"], sig["strike"]
        if not (0.15 * spot < strike < 6.0 * spot):
            return None
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
    if (ask - bid) > MAX_BOOK_SPREAD:
        # Empty-shell book (e.g. 0.01/0.99) — mid is meaningless and there's
        # no real counterparty; trading here is pure adverse selection
        log.debug(f"  Skip {label[:40]}: dead book bid={bid:.2f} ask={ask:.2f}")
        return None

    # Always compare YES vs YES — gap is the same whether measured from YES or NO side
    gap = abs(fair - book_mid)
    if gap > MAX_MODEL_MARKET_GAP:
        # Model and market strongly disagree — likely bad question parse, skip
        log.debug(f"  Skip {label[:40]}: model={fair:.2f} book_mid={book_mid:.2f} gap={gap:.2f}")
        return None

    # Never fade near-resolved markets — when the crowd prices ≥92% certainty,
    # buying the cheap side is a lottery ticket with negative expectancy
    if book_mid >= 0.92 or book_mid <= 0.08:
        log.debug(f"  Skip {label[:40]}: near-resolved (mid={book_mid:.2f})")
        return None

    # Shrink model toward market price — the book aggregates information our
    # model doesn't have. Weight increases with time horizon: the further out
    # the market, the less our option model knows vs. collective market wisdom.
    # Previous 70/30 split led to 40% daily win rate — market was systematically
    # more accurate. Now 55/45: model guides direction, market sets magnitude.
    T_days_local = T * 365
    if T_days_local > 30:
        # Long-horizon (monthly+): market has much more information — lean on it
        book_weight = 0.55
    elif T_days_local > 7:
        # Weekly: moderate trust split
        book_weight = 0.45
    else:
        # Short-horizon (≤7 days): model is more reliable, smaller book weight
        book_weight = 0.35
    fair = round((1 - book_weight) * fair + book_weight * book_mid, 4)

    # Daily conviction gate: skip near-coinflip positions.
    # Hourly markets require 55% conviction; daily/weekly need 60% — they're
    # longer duration with more uncertainty, so the bar should be higher.
    if abs(fair - 0.50) < (MIN_DAILY_CONVICTION - 0.50):
        log.debug(f"  Skip {label[:40]}: low conviction (fair={fair:.2f})")
        return None

    result = dict(question=question[:65], direction=direction, strike=strike,
                  fair=fair, book_mid=book_mid, gap=round(gap, 3), volume=volume,
                  T_days=T_days_local, model=model, action=None, traded=False,
                  limit=0.0, net_edge=0.0, side="?")

    # ── Free bankroll = balance minus already-committed USDC ──────────────────
    available = free_bankroll(bankroll, om)
    # Dynamic max bet: 5% of bankroll, capped at $10. Scales as the account grows.
    dyn_max = min(10.0, bankroll * 0.05)

    if fair >= 0.50:
        # BUY YES — limit below our fair, but at least 1 tick above best bid
        limit    = round(max(bid + 0.01, fair - EDGE_BUFFER, 0.02), 2)
        limit    = min(limit, ask - 0.01)  # never cross the spread
        net_edge = fair * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="YES")
        if net_edge >= MIN_EDGE and not om.has_open_order(token_yes):
            size = kelly_buy(fair, limit, available, mtype="daily", max_bet=dyn_max)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_yes, limit, size, fair, label):
                    result.update(action=f"BUY YES @{limit:.2f} ${size:.2f}", traded=True)
    else:
        # BUY NO — use NO token's book
        if token_no:
            bid_no, ask_no, _, liquid_no = get_order_book(token_no)
        else:
            bid_no, ask_no, liquid_no = 0.0, 1.0, False

        # Guard the NO-side book the same way the YES side is guarded above
        if not liquid_no or (ask_no - bid_no) > MAX_BOOK_SPREAD:
            bid_no, ask_no, liquid_no = 0.0, 1.0, False

        fair_no  = 1 - fair
        limit    = round(max(bid_no + 0.01, fair_no - EDGE_BUFFER, 0.02), 2)
        if liquid_no:
            limit = min(limit, ask_no - 0.01)
        net_edge = fair_no * (1 - TAKER_FEE) - limit
        result.update(limit=limit, net_edge=net_edge, side="NO")
        if net_edge >= MIN_EDGE and token_no and not om.has_open_order(token_no):
            size = kelly_buy(fair_no, limit, available, mtype="daily", max_bet=dyn_max)
            if size >= MIN_BET_USDC:
                if place_order(client, om, token_no, limit, size, fair_no, f"{label} NO"):
                    result.update(action=f"BUY NO  @{limit:.2f} ${size:.2f}", traded=True)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# HOURLY MARKET SCANNING  (up/down markets — momentum-based, no strike)
# ══════════════════════════════════════════════════════════════════════════════

# Each entry: slug_name used in Polymarket URLs, Binance base symbol
HOURLY_ASSETS = {
    "BTC":  {"slug_name": "bitcoin",      "binance": "BTC"},
    "ETH":  {"slug_name": "ethereum",     "binance": "ETH"},
    "SOL":  {"slug_name": "solana",       "binance": "SOL"},
    "XRP":  {"slug_name": "xrp",          "binance": "XRP"},
    "HYPE": {"slug_name": "hyperliquid",  "binance": "HYPE"},
    "DOGE": {"slug_name": "dogecoin",     "binance": "DOGE"},
    "AVAX": {"slug_name": "avalanche",    "binance": "AVAX"},
    "LINK": {"slug_name": "chainlink",    "binance": "LINK"},
    "SUI":  {"slug_name": "sui",          "binance": "SUI"},
    "PEPE": {"slug_name": "pepe",         "binance": "PEPE"},
    "ADA":  {"slug_name": "cardano",      "binance": "ADA"},
    "TON":  {"slug_name": "toncoin",      "binance": "TON"},
}


def _et_now() -> datetime:
    """Current time in US Eastern (EDT = UTC-4 Mar–Nov, EST = UTC-5 otherwise)."""
    utc = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 10 else -5
    return utc + timedelta(hours=offset)


def generate_hourly_slugs() -> list:
    """
    Return list of (slug, asset, binance_sym) for the current hour + next 3 hours in ET.
    Tries both slug formats Polymarket has used historically.
    """
    et     = _et_now()
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    results = []

    for hour_offset in range(4):
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


def _rsi(closes: list, period: int = 14) -> float:
    """
    Wilder RSI from the last `period` 1-min bars. Returns [0, 100].
    50 = neutral fallback when not enough data.
    """
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(-period, 0)]
    gains  = sum(max(0.0, d) for d in deltas) / period
    losses = sum(max(0.0, -d) for d in deltas) / period
    if losses == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _hourly_momentum(binance_sym: str) -> float:
    """
    Return momentum signal in [-0.15, +0.15] from 1-min Binance klines.
    Positive = bullish (favour UP token).

    RSI dampener: when the 14-period 1m RSI is extreme (>70 overbought /
    <30 oversold), the momentum signal is linearly faded toward zero because
    mean-reversion is more likely than continuation before the hour closes.
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
        closes  = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        if len(closes) < 12:
            return 0.0
        # Multi-timeframe momentum, normalised
        m10 = (closes[-1] - closes[-11]) / closes[-11]   # 10-bar
        m5  = (closes[-1] - closes[-6])  / closes[-6]    # 5-bar
        m2  = (closes[-1] - closes[-3])  / closes[-3]    # 2-bar
        raw = 0.50 * m10 + 0.30 * m5 + 0.20 * m2
        val = max(-0.15, min(0.15, raw * 8))             # scale ×8 then clamp

        # RSI mean-reversion dampener — fade momentum when the move is exhausted.
        # Linear fade: full signal at RSI=70/30, zero signal at RSI=100/0.
        rsi_val = _rsi(closes)
        if rsi_val > 70 and val > 0:
            val *= max(0.0, (100.0 - rsi_val) / 30.0)
        elif rsi_val < 30 and val < 0:
            val *= max(0.0, rsi_val / 30.0)

        # Volatility regime: amplify in trending markets, dampen in ranging ones.
        # Compare recent 10-bar vol vs 30-bar baseline to detect regime.
        if len(closes) >= 30:
            recent_vol   = (sum((closes[-i] - closes[-i - 1]) ** 2
                               for i in range(1, 11)) / 10) ** 0.5
            baseline_vol = (sum((closes[-i] - closes[-i - 1]) ** 2
                               for i in range(1, 30)) / 29) ** 0.5
            if baseline_vol > 1e-10:
                regime = recent_vol / baseline_vol
                if regime > 1.5:        # high vol / trending → strengthen signal
                    val = max(-0.15, min(0.15, val * 1.3))
                elif regime < 0.6:      # low vol / ranging → weaken signal
                    val *= 0.4

        # Volume confirmation: a price move on above-average volume is genuine;
        # the same move on thin volume may be noise. Scale [0.5×, 1.8×].
        if len(volumes) >= 10:
            avg_vol    = sum(volumes) / len(volumes)
            recent_avg = sum(volumes[-5:]) / 5
            if avg_vol > 0:
                vol_ratio = recent_avg / avg_vol
                val = max(-0.15, min(0.15, val * max(0.5, min(1.8, vol_ratio))))

        log.debug(f"  Momentum [{binance_sym}]: raw={raw*8:.3f} rsi={rsi_val:.0f} → {val:.3f}")
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


_funding_cache: dict[str, tuple[float, float]] = {}  # sym → (ts, rate)
_FUNDING_TTL = 300.0  # refresh every 5 min


def _funding_rate(binance_sym: str) -> float:
    """
    Return the latest Binance perpetual funding rate for a symbol.
    Positive = longs paying shorts = bullish market sentiment.
    Typical range: -0.003 to +0.003 (i.e. -0.3% to +0.3%).
    Cached for 5 minutes; returns 0.0 on any failure.
    """
    now = time.time()
    cached = _funding_cache.get(binance_sym)
    if cached and now - cached[0] < _FUNDING_TTL:
        return cached[1]
    try:
        data = retry_get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": binance_sym + "USDT", "limit": 1},
            timeout=8,
        ).json()
        rate = float(data[0]["fundingRate"]) if data else 0.0
        rate = max(-0.003, min(0.003, rate))
        _funding_cache[binance_sym] = (now, rate)
        log.debug(f"  Funding rate [{binance_sym}]: {rate*100:.4f}%")
        return rate
    except Exception:
        return _funding_cache.get(binance_sym, (0, 0.0))[1]


_pm_price_cache: dict[str, tuple[float, float]] = {}  # token_id → (momentum, ts)
_PM_PRICE_TTL   = 30.0   # refresh every 30s — fast enough to catch live moves

def _polymarket_price_momentum(token_id: str) -> float:
    """
    Fetch the last 20 minutes of Polymarket CLOB price history for a token
    (the same line chart visible on the Polymarket website) and return a
    momentum value in [-0.15, +0.15]:
      positive  = token price trending UP on Polymarket
      negative  = token price trending DOWN on Polymarket
      near-zero = price flat / insufficient data

    Used as a 5th consensus signal: if Polymarket participants are also
    pushing the YES price upward, that confirms our model's UP edge.
    Cached per token_id for _PM_PRICE_TTL seconds to avoid hammering the API.
    """
    now = time.time()
    cached = _pm_price_cache.get(token_id)
    if cached and now - cached[0] < _PM_PRICE_TTL:
        return cached[1]
    try:
        end_ts   = int(now)
        start_ts = end_ts - 1200  # last 20 minutes
        resp = retry_get(
            "https://clob.polymarket.com/prices-history",
            params={
                "market":   token_id,
                "fidelity": 1,       # 1-minute bars
                "startTs":  start_ts,
                "endTs":    end_ts,
            },
            timeout=8,
        ).json()
        history = resp.get("history", []) if isinstance(resp, dict) else []
        prices  = [float(pt["p"]) for pt in history if isinstance(pt, dict) and "p" in pt]
        if len(prices) < 4:
            _pm_price_cache[token_id] = (now, 0.0)
            return 0.0
        # Compare the last 5 prices vs the first half as baseline
        mid        = max(1, len(prices) // 2)
        recent_avg = sum(prices[-5:]) / min(5, len(prices))
        early_avg  = sum(prices[:mid]) / mid
        if early_avg <= 0:
            _pm_price_cache[token_id] = (now, 0.0)
            return 0.0
        mom = (recent_avg - early_avg) / early_avg
        mom = max(-0.15, min(0.15, mom))
        _pm_price_cache[token_id] = (now, mom)
        log.debug(f"  PM price momentum [{token_id[:12]}…]: {mom:+.4f} "
                  f"({len(prices)} bars, recent={recent_avg:.3f} early={early_avg:.3f})")
        return mom
    except Exception:
        return _pm_price_cache.get(token_id, (0, 0.0))[1]


_five_min_cache: dict[str, tuple[float, float]] = {}  # asset → (sentiment, ts)
_FIVE_MIN_TTL   = 45.0   # refresh every 45s

_FIVE_MIN_SLUGS: dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "XRP": "xrp",     "DOGE": "dogecoin", "AVAX": "avalanche",
    "LINK": "chainlink", "HYPE": "hyperliquid", "SUI": "sui",
    "PEPE": "pepe",   "ADA": "cardano",  "TON": "toncoin",
}

def _five_min_sentiment(asset: str) -> float:
    """
    Find the live Polymarket '5-minute up or down' market for this asset and
    return the YES mid-price as a short-term sentiment signal.
    Returns value in [-0.50, +0.50]:
      positive = Polymarket 5-min traders expect price UP (bullish now)
      negative = bearish now
      0.0      = no 5-minute market found (safe no-op)
    Cached 45s — these markets move fast but we don't need tick-level refresh.
    """
    now = time.time()
    cached = _five_min_cache.get(asset)
    if cached and now - cached[0] < _FIVE_MIN_TTL:
        return cached[1]
    name = _FIVE_MIN_SLUGS.get(asset, asset.lower())
    try:
        for kw in (f"{name} 5 minute", f"{name} 5-minute", f"will {name} be up in 5"):
            for m in search_gamma(kw, limit=10):
                q = m.get("question", "").lower()
                if "5 min" not in q and "5-min" not in q:
                    continue
                tokens = m.get("clobTokenIds") or []
                if not tokens:
                    continue
                bid, ask, book_mid, liquid = get_order_book(tokens[0])
                if liquid and 0.05 < book_mid < 0.95:
                    val = round(book_mid - 0.50, 4)   # positive = YES likely
                    _five_min_cache[asset] = (now, val)
                    log.debug(f"  5-min sentiment [{asset}]: {val:+.4f} "
                              f"(yes_mid={book_mid:.3f}  q={m.get('question','')[:40]})")
                    return val
    except Exception:
        pass
    _five_min_cache[asset] = (now, 0.0)
    return 0.0


_hour_open_cache: dict[str, tuple[float, float]] = {}  # sym → (ts, open_price)
_HOUR_OPEN_TTL = 120.0  # refresh every 2 min


def _get_hour_open(binance_sym: str) -> float | None:
    """Return the open price of the current 1h Binance candle (the hour reference price)."""
    now = time.time()
    cached = _hour_open_cache.get(binance_sym)
    if cached and now - cached[0] < _HOUR_OPEN_TTL:
        return cached[1]
    try:
        kline = retry_get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": binance_sym + "USDT", "interval": "1h", "limit": 1},
            timeout=8,
        ).json()
        open_price = float(kline[0][1])  # index 1 = open price
        _hour_open_cache[binance_sym] = (now, open_price)
        return open_price
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# NEWS SENTIMENT
# Primary signal:  Fear & Greed Index (alternative.me — free, no key needed)
# Secondary signal: CoinTelegraph + CoinDesk RSS keyword matching
# Both signals are blended: F&G gives market-wide direction, RSS adds
# asset-specific nuance. Combined score drives NEWS_FAIR_NUDGE on hourly fairs.
# ══════════════════════════════════════════════════════════════════════════════

_BULLISH_WORDS = frozenset([
    "surge", "rally", "pump", "breakout", "ath", "all-time high",
    "gain", "rise", "bull", "etf", "approval", "approved", "adopt",
    "upgrade", "halving", "record", "inflow", "launch", "partnership",
    "accumulate", "accumulation", "support",
])
_BEARISH_WORDS = frozenset([
    "crash", "hack", "hacked", "ban", "plunge", "drop", "dump", "bear",
    "fear", "warning", "collapse", "exploit", "breach", "sec", "lawsuit",
    "loss", "outflow", "scam", "fraud", "fine", "penalty",
    "liquidat", "bankrupt", "attack", "stolen", "vulnerab",
])
_ASSET_NEWS_KEYWORDS = {
    "BTC":  ["bitcoin"],
    "ETH":  ["ethereum"],
    "SOL":  ["solana"],
    "XRP":  ["xrp", "ripple"],
    "HYPE": ["hyperliquid"],
    "DOGE": ["dogecoin", "doge"],
    "AVAX": ["avalanche", "avax"],
    "LINK": ["chainlink", "link"],
}
_RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]


def _headline_score(text: str) -> float:
    hl = text.lower()
    score = 0.0
    for w in _BULLISH_WORDS:
        if w in hl:
            score += 0.3
    for w in _BEARISH_WORDS:
        if w in hl:
            score -= 0.3
    return max(-1.0, min(1.0, score))


def get_fear_greed() -> float:
    """
    Fetch the Crypto Fear & Greed Index (alternative.me). Returns a score
    in [-1.0, +1.0] normalised from the raw 0–100 value:
       0  (Extreme Fear)  → -1.0
       50 (Neutral)       →  0.0
       100 (Extreme Greed) → +1.0
    Cached for _FNG_TTL seconds; returns last known value on failure.
    """
    now = time.time()
    if _FNG_CACHE[1] and now - _FNG_CACHE[1] < _FNG_TTL:
        return _FNG_CACHE[0]
    try:
        data  = retry_get("https://api.alternative.me/fng/",
                          params={"limit": 1}, timeout=8).json()
        raw   = int(data["data"][0]["value"])
        label = data["data"][0]["value_classification"]
        score = round((raw - 50) / 50, 3)   # maps 0→-1, 50→0, 100→+1
        _FNG_CACHE[0] = score
        _FNG_CACHE[1] = now
        log.info(f"  Fear & Greed: {raw}/100 ({label}) → score={score:+.2f}")
        return score
    except Exception as e:
        log.debug(f"  Fear & Greed fetch failed: {e}")
        return _FNG_CACHE[0]   # last known value


class _NewsCache:
    """
    Blends two free sentiment signals:
      1. Fear & Greed Index (alternative.me) — market-wide, high quality,
         no API key. Applied equally to all assets with 60% weight.
      2. RSS keyword scoring (CoinTelegraph + CoinDesk) — asset-specific,
         rougher signal. Applied with 40% weight.
    """

    def __init__(self):
        self._ts:     float            = 0.0
        self._scores: dict[str, float] = {a: 0.0 for a in _ASSET_NEWS_KEYWORDS}

    def _parse_pub_date(self, raw: str) -> float:
        try:
            return parsedate_to_datetime(raw.strip()).timestamp()
        except Exception:
            return 0.0

    def _fetch_rss(self) -> dict[str, float]:
        """Asset-specific keyword scoring from CoinTelegraph + CoinDesk RSS."""
        cutoff = time.time() - _NEWS_WINDOW
        totals: dict[str, list[float]] = {k: [] for k in _ASSET_NEWS_KEYWORDS}
        for feed in _RSS_FEEDS:
            try:
                r = requests.get(feed, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code != 200:
                    continue
                items = re.findall(r"<item>(.*?)</item>", r.text, re.DOTALL)
                for item in items:
                    tm = re.search(
                        r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                        item, re.DOTALL,
                    )
                    dm = re.search(r"<pubDate>(.*?)</pubDate>", item)
                    if not tm:
                        continue
                    title  = tm.group(1).strip()
                    pub_ts = self._parse_pub_date(dm.group(1)) if dm else 0.0
                    if pub_ts and pub_ts < cutoff:
                        continue
                    score = _headline_score(title)
                    hl    = title.lower()
                    for asset, kws in _ASSET_NEWS_KEYWORDS.items():
                        if any(kw in hl for kw in kws):
                            totals[asset].append(score)
            except Exception as e:
                log.debug(f"  RSS {feed} failed: {e}")
        result: dict[str, float] = {}
        for asset, scores_list in totals.items():
            result[asset] = (round(max(-1.0, min(1.0,
                             sum(scores_list) / len(scores_list))), 3)
                             if scores_list else 0.0)
        return result

    def _fetch(self) -> dict[str, float]:
        fng      = get_fear_greed()          # market-wide score in [-1, +1]
        rss      = self._fetch_rss()         # asset-specific scores in [-1, +1]
        # 60% weight to F&G (reliable, aggregated) + 40% to RSS (asset-specific)
        result   = {}
        for asset in _ASSET_NEWS_KEYWORDS:
            blended = 0.60 * fng + 0.40 * rss.get(asset, 0.0)
            result[asset] = round(max(-1.0, min(1.0, blended)), 3)
        return result

    def get(self) -> dict[str, float]:
        if time.time() - self._ts >= _NEWS_TTL:
            self._scores = self._fetch()
            self._ts     = time.time()
            log.info(
                "  News sentiment (F&G+RSS): "
                + "  ".join(f"{a}={self._scores.get(a, 0.0):+.2f}"
                             for a in ("BTC", "ETH", "SOL", "XRP", "HYPE"))
            )
        return self._scores


_news_cache = _NewsCache()


def compute_hourly_fair(binance_sym: str, spot: float = 0.0, mins_left: float = 999.0) -> tuple:
    """
    Return (fair_up, fair_down) for an hourly up/down market.

    Blends two signals:
      1. Momentum + book-imbalance + funding + news (momentum-composite)
      2. Digital-option P(spot_T > hour_open) — the exact settlement question

    Option weight has two components:
      - Time component:        ramps from 0% at 90 min to 80% at ~8 min left
      - Displacement component: if spot has already moved ≥1% from hour-open,
                                add up to +40% weight (price has settled direction)
    Combined weight is capped at 85%. Clamped to [0.25, 0.75].
    """
    mom  = _hourly_momentum(binance_sym)
    imb  = _book_imbalance(binance_sym)
    fund = _funding_rate(binance_sym)
    news = _news_cache.get().get(binance_sym, 0.0) * NEWS_FAIR_NUDGE
    fair_mom = max(0.25, min(0.75,
               0.50 + 2.0 * mom + 0.08 * imb + 50.0 * fund + news))

    fair_opt   = None
    opt_weight = 0.0
    if spot > 0 and mins_left < 90:
        hour_open = _get_hour_open(binance_sym)
        if hour_open and hour_open > 0:
            T   = max(0.5 / 60 / 24 / 365, mins_left / 60 / 24 / 365)
            vol = get_live_vol(binance_sym, 0.70, T)
            p   = european_prob(spot, hour_open, T, vol, 0.0, "up")
            if p is not None:
                fair_opt = p
                # Time component: 0% at 90 min, 80% at ~8 min remaining
                time_w = max(0.0, min(0.80, (90 - mins_left) / 87.5))
                # Displacement component: each 1% move from hour-open adds 20% weight.
                # A 2%+ move → option model is highly informative regardless of time.
                disp_w = min(0.40, abs(spot - hour_open) / hour_open * 20)
                opt_weight = min(0.85, time_w + disp_w)
                log.debug(f"  Hourly opt [{binance_sym}]: S={spot:.2f} K={hour_open:.2f} "
                          f"T={mins_left:.0f}m vol={vol*100:.0f}% P(up)={p:.3f} "
                          f"w={opt_weight:.2f} (t={time_w:.2f}+d={disp_w:.2f})")

    if fair_opt is not None:
        fair_up = opt_weight * fair_opt + (1 - opt_weight) * fair_mom
        log.debug(f"  Hourly blend [{binance_sym}]: opt={fair_opt:.3f}×{opt_weight:.2f} "
                  f"+ mom={fair_mom:.3f}×{1-opt_weight:.2f} = {fair_up:.3f}")
    else:
        fair_up = fair_mom

    fair_up = round(max(0.25, min(0.75, fair_up)), 4)
    return fair_up, round(1 - fair_up, 4)


MIN_HOURLY_VOLUME    = 10     # hourly markets are new each hour — volume is always tiny early
MIN_HOURLY_MINS      = 15     # skip hourly markets with <15 min to expiry
HOURLY_EDGE_BUFFER   = 0.010  # tighter than regular 3% — hourly fairs cluster near 50¢
MAX_HOURLY_SPREAD    = 0.30   # require a real two-sided book (skip 0.01/0.99 empty shells)
MIN_HOURLY_BID       = 0.05   # require a real bid — avoids adverse selection in dead books
MIN_HOURLY_CONVICTION = 0.55  # require ≥55¢ fair before entering — no near-coinflip bets
MIN_HOURLY_EDGE      = 0.025  # consensus gate handles quality; lower bar finds more opportunities
MAX_HOURLY_BET_USDC  = 3.0    # cap hourly bets lower than regular $5 max
NEWS_FAIR_NUDGE  = 0.03   # max ±3¢ shift on hourly fair from news sentiment
NEWS_VETO_SCORE  = 0.60   # block entry when news strongly opposes direction (≥2 strong keywords)
_NEWS_TTL        = 600    # re-fetch RSS every 10 minutes
_NEWS_WINDOW     = 7200   # only count articles published in the last 2 hours


def _tod_bet_mult() -> float:
    """
    Time-of-day bet sizing multiplier.
    Asian hours (UTC 02–08): thin books, noisier signals → 40% sizing.
    US market hours (UTC 13–22): peak liquidity → full sizing.
    Overnight US / European hours: 75% sizing.
    """
    h = datetime.now(timezone.utc).hour
    if 2 <= h < 8:
        return 0.40   # Asian low-liquidity window
    if 13 <= h < 22:
        return 1.00   # US session peak
    return 0.75       # European / overnight


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
        kw_map = [("bitcoin up or down",     "BTC",  "BTC"),
                  ("ethereum up or down",    "ETH",  "ETH"),
                  ("solana up or down",      "SOL",  "SOL"),
                  ("xrp up or down",         "XRP",  "XRP"),
                  ("hyperliquid up or down", "HYPE", "HYPE"),
                  ("dogecoin up or down",    "DOGE", "DOGE"),
                  ("avalanche up or down",   "AVAX", "AVAX"),
                  ("chainlink up or down",   "LINK", "LINK"),
                  ("sui up or down",         "SUI",  "SUI"),
                  ("pepe up or down",        "PEPE", "PEPE"),
                  ("cardano up or down",     "ADA",  "ADA"),
                  ("toncoin up or down",     "TON",  "TON")]
        for kw, asset, bsym in kw_map:
            try:
                for m in search_gamma(kw, limit=20):
                    add(m, asset, bsym)
            except Exception:
                pass

    return results


def scan_hourly_markets(client: ClobClient, om: OrderManager, bankroll: float) -> int:
    """
    Scan current + next-3-hour up/down markets for BTC, ETH, SOL, XRP, HYPE, DOGE, AVAX, LINK.
    Returns number of orders placed.
    """
    orders_placed = 0
    seen_tokens: set = set()
    spot_map: dict[str, float] = {}   # cache spot per asset for this scan

    for m, asset, binance_sym in _collect_hourly_markets():
        question = m.get("question", "")

        # ── Volume gate (lower than weekly markets) ────────────────────────
        if float(m.get("volume") or 0) < MIN_HOURLY_VOLUME:
            continue

        # ── Skip markets about to expire (< MIN_HOURLY_MINS left) ─────────
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

        # ── Per-asset concentration cap (hourly counts toward the same limit) ─
        asset_positions = sum(1 for lbl in om.held_labels.values() if asset in lbl)
        if asset_positions >= MAX_POSITIONS_PER_ASSET:
            log.debug(f"    Skip {asset} hourly: {asset_positions} positions already held (cap={MAX_POSITIONS_PER_ASSET})")
            continue

        # ── Correlation limit: max 1 active hourly position per asset ──────
        if any(asset in lbl and ("hourly" in lbl.lower() or " et " in lbl.lower())
               for lbl in om.held_labels.values()):
            log.debug(f"    Skip {asset} hourly: already holding a {asset} hourly position")
            continue

        # ── Loss cooldown: skip direction for 1.5h after a stop-loss ───────
        now_ts    = time.time()
        up_on_cd  = now_ts - _loss_cooldown.get(f"{asset}_UP",   0) < LOSS_COOLDOWN_HOURS * 3600
        dn_on_cd  = now_ts - _loss_cooldown.get(f"{asset}_DOWN", 0) < LOSS_COOLDOWN_HOURS * 3600

        # ── Fetch spot (cached per asset) ─────────────────────────────────
        if asset not in spot_map:
            acfg = ASSETS.get(asset, {})
            s = get_price(acfg.get("coingecko_id", ""), binance_sym)
            spot_map[asset] = s or 0.0
        spot = spot_map[asset]

        fair_up, fair_down = compute_hourly_fair(binance_sym, spot, mins_left)

        # Cross-asset confirmation: count how many other assets' 1m momentum
        # agrees with the current direction. 2+ agreeing assets = risk-on/off
        # move, which is a real signal. Nudge fair by 2¢ per confirming asset.
        if fair_up >= MIN_HOURLY_CONVICTION or fair_down >= MIN_HOURLY_CONVICTION:
            direction_up = fair_up >= fair_down
            confirmations = sum(
                1 for oa, ocfg in HOURLY_ASSETS.items()
                if oa != asset
                and abs(_hourly_momentum(ocfg["binance"])) > 0.03
                and (_hourly_momentum(ocfg["binance"]) > 0) == direction_up
            )
            if confirmations >= 2:
                boost = 0.02 * confirmations
                if direction_up:
                    fair_up   = min(0.75, fair_up   + boost)
                    fair_down = round(1 - fair_up,  4)
                else:
                    fair_down = min(0.75, fair_down + boost)
                    fair_up   = round(1 - fair_down, 4)
                log.debug(f"    Cross-asset: {confirmations} assets agree → "
                          f"fair_up={fair_up:.3f}")

        news_score = _news_cache.get().get(asset, 0.0)
        tod_mult   = _tod_bet_mult()
        tm       = re.search(r"\d{1,2}(?::\d{2})?\s*(?:am|pm)", question, re.I)
        time_str = tm.group(0) if tm else "?"

        # ── Signal consensus gate ──────────────────────────────────────────
        # 5 signals scored +1/-1/0 based on agreement with dominant direction.
        # Range: [-5, +5]. Require consensus ≥ 1 to enter.
        # Size up: 1.2× at 3 agreeing, 1.4× at 4, 1.6× at all 5.
        direction_up = fair_up >= fair_down
        mom_s      = _hourly_momentum(binance_sym)
        imb_s      = _book_imbalance(binance_sym)
        fund_s     = _funding_rate(binance_sym)
        pm_mom_s   = _polymarket_price_momentum(token_up)   # live Polymarket chart trend
        five_min_s = _five_min_sentiment(asset)             # 5-min market YES price

        def _sig(val: float, threshold: float) -> int:
            if abs(val) < threshold: return 0
            return 1 if (val > 0) == direction_up else -1

        consensus = (
            _sig(mom_s,       0.02)   +   # Binance price momentum
            _sig(imb_s,       0.08)   +   # Binance order book imbalance
            _sig(fund_s,      0.0001) +   # Binance funding rate
            _sig(news_score,  0.10)   +   # Fear & Greed / news sentiment
            _sig(pm_mom_s,    0.03)   +   # Polymarket live chart trend (last 20 min)
            _sig(five_min_s,  0.05)       # Polymarket 5-min market crowd sentiment
        )
        # consensus_mult: 1.0× base, +0.20× per signal above 2 (max 1.8× at 6/6)
        consensus_mult = 1.0 + max(0, consensus - 2) * 0.20

        log.info(f"  ⏰ {asset} hourly [{time_str} ET | {mins_left:.0f}m left] "
                 f"fair_up={fair_up:.3f} fair_dn={fair_down:.3f} "
                 f"consensus={consensus}/6 pm_mom={pm_mom_s:+.3f} "
                 f"5min={five_min_s:+.3f} news={news_score:+.2f} tod={tod_mult:.2f}  "
                 f"{question[:40]}")

        if consensus < 2:
            log.info(f"    Skip {asset} hourly: weak consensus ({consensus}/6 < 2 required)")
            continue

        available  = free_bankroll(bankroll, om)
        market_end = time.time() + mins_left * 60   # unix ts of this market's expiry

        if fair_up >= MIN_HOURLY_CONVICTION:
            if news_score <= -NEWS_VETO_SCORE:
                log.info(f"    Skip {asset} UP: news bearish ({news_score:+.2f})")
            elif up_on_cd:
                log.info(f"    Skip {asset} UP: loss cooldown active")
            else:
                bid, ask, book_mid, liquid = get_order_book(token_up)
                if not liquid:
                    pass
                elif (ask - bid) > MAX_HOURLY_SPREAD or bid < MIN_HOURLY_BID:
                    log.debug(f"    Skip hourly UP: dead book bid={bid:.2f} ask={ask:.2f}")
                elif abs(fair_up - book_mid) > MAX_MODEL_MARKET_GAP:
                    log.debug(f"    Skip hourly UP: model={fair_up:.2f} book={book_mid:.2f}")
                else:
                    limit    = round(max(bid + 0.01, fair_up - HOURLY_EDGE_BUFFER, 0.02), 2)
                    limit    = min(limit, ask - 0.01)
                    net_edge = fair_up * (1 - TAKER_FEE) - limit
                    label    = f"{asset} UP {time_str} ET (hourly)"
                    if net_edge >= MIN_HOURLY_EDGE and not om.has_open_order(token_up):
                        dyn_max_h = min(6.0, bankroll * 0.05) * tod_mult * consensus_mult
                        size = kelly_buy(fair_up, limit, available,
                                         mtype="hourly", max_bet=dyn_max_h)
                        if size >= MIN_BET_USDC:
                            if place_order(client, om, token_up, limit, size, fair_up, label,
                                           market_end=market_end):
                                orders_placed += 1
        elif fair_down >= MIN_HOURLY_CONVICTION:
            if news_score >= NEWS_VETO_SCORE:
                log.info(f"    Skip {asset} DOWN: news bullish ({news_score:+.2f})")
            elif dn_on_cd:
                log.info(f"    Skip {asset} DOWN: loss cooldown active")
            else:
                bid_dn, ask_dn, book_mid_dn, liquid_dn = get_order_book(token_down)
                if not liquid_dn:
                    pass
                elif (ask_dn - bid_dn) > MAX_HOURLY_SPREAD or bid_dn < MIN_HOURLY_BID:
                    log.debug(f"    Skip hourly DOWN: dead book bid={bid_dn:.2f} ask={ask_dn:.2f}")
                elif abs(fair_down - book_mid_dn) > MAX_MODEL_MARKET_GAP:
                    log.debug(f"    Skip hourly DOWN: model={fair_down:.2f} book={book_mid_dn:.2f}")
                else:
                    limit    = round(max(bid_dn + 0.01, fair_down - HOURLY_EDGE_BUFFER, 0.02), 2)
                    limit    = min(limit, ask_dn - 0.01)
                    net_edge = fair_down * (1 - TAKER_FEE) - limit
                    label    = f"{asset} DOWN {time_str} ET (hourly)"
                    if net_edge >= MIN_HOURLY_EDGE and not om.has_open_order(token_down):
                        dyn_max_h = min(6.0, bankroll * 0.05) * tod_mult * consensus_mult
                        size = kelly_buy(fair_down, limit, available,
                                         mtype="hourly", max_bet=dyn_max_h)
                        if size >= MIN_BET_USDC:
                            if place_order(client, om, token_down, limit, size, fair_down, label,
                                           market_end=market_end):
                                orders_placed += 1
        else:
            log.debug(f"    Skip hourly {asset}: no conviction (fair_up={fair_up:.3f} fair_dn={fair_down:.3f})")

    return orders_placed


# ══════════════════════════════════════════════════════════════════════════════
# YES + NO STRUCTURAL ARBITRAGE
# ══════════════════════════════════════════════════════════════════════════════
# When YES ask + NO ask < (1 − fee) = 0.98, buying one share of each
# guarantees a risk-free profit regardless of outcome.

ARB_MIN_PROFIT  = 0.01   # require at least 1¢ profit after fee to enter
ARB_MAX_BET     = 5.0    # max USDC spent on each leg of the arb pair


def scan_yes_no_arb(client: ClobClient, om: OrderManager,
                    markets: list[dict], bankroll: float) -> int:
    """
    Scan a list of Gamma markets for YES+NO structural arbitrage.
    Places a BUY on both YES and NO tokens when combined ask price < 0.97.
    Returns number of arb pairs entered.
    """
    placed = 0
    for m in markets:
        raw = m.get("clobTokenIds") or []
        if isinstance(raw, str):
            try: raw = json.loads(raw)
            except: continue
        if len(raw) < 2:
            continue

        token_yes, token_no = raw[0], raw[1]
        if (om.already_holds(token_yes) or om.already_holds(token_no)
                or om.has_open_order(token_yes) or om.has_open_order(token_no)):
            continue
        if float(m.get("volume") or 0) < MIN_VOLUME:
            continue

        try:
            bid_y, ask_y, _, liq_y = get_order_book(token_yes)
            bid_n, ask_n, _, liq_n = get_order_book(token_no)
        except Exception:
            continue

        if not liq_y or not liq_n:
            continue
        if (ask_y - bid_y) > MAX_BOOK_SPREAD or (ask_n - bid_n) > MAX_BOOK_SPREAD:
            continue

        combined  = round(ask_y + ask_n, 4)
        payout    = 1.0 - TAKER_FEE   # guaranteed payout per share (one side wins)
        net_profit = round(payout - combined, 4)

        if net_profit < ARB_MIN_PROFIT:
            continue

        available = free_bankroll(bankroll, om)
        # Size: spend up to ARB_MAX_BET USDC on each leg, capped by available cash
        size_yes = min(ARB_MAX_BET, available / 2)
        size_no  = min(ARB_MAX_BET, available / 2)
        if size_yes < MIN_BET_USDC or size_no < MIN_BET_USDC:
            continue

        label_base = m.get("question", "")[:40]
        log.info(f"  ♻️  ARB: YES@{ask_y:.3f} + NO@{ask_n:.3f} = {combined:.3f} "
                 f"(profit/share={net_profit:.3f})  {label_base}")

        ok_y = place_order(client, om, token_yes, ask_y, size_yes, 1.0, f"ARB YES {label_base}")
        ok_n = place_order(client, om, token_no,  ask_n, size_no,  1.0, f"ARB NO  {label_base}")
        if ok_y and ok_n:
            tg(f"♻️ <b>ARB PAIR</b>\n"
               f"YES@{ask_y:.3f} + NO@{ask_n:.3f} = {combined:.3f}\n"
               f"Profit/share: {net_profit:.3f}  ({label_base})")
            placed += 1

    return placed


# ══════════════════════════════════════════════════════════════════════════════
# P&L TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def get_positions_value(wallet: str) -> float:
    """Total current market value of all held positions (Data API)."""
    if not wallet:
        return 0.0
    try:
        data = retry_get(
            f"{DATA_HOST}/positions",
            params={"user": wallet, "sizeThreshold": "0.01"},
            timeout=12,
        ).json()
        total = 0.0
        for p in (data if isinstance(data, list) else []):
            val = p.get("currentValue") or p.get("current_value")
            if val is not None:
                total += float(val)
            else:
                size  = float(p.get("size") or 0)
                price = float(p.get("curPrice") or p.get("price") or 0)
                total += size * price
        return round(total, 2)
    except Exception as e:
        log.debug(f"get_positions_value: {e}")
        return 0.0


class PnL:
    def __init__(self, wallet: str = ""):
        self.start          = datetime.now(timezone.utc)
        self.last_report    = 0.0  # 0 ensures the first 9am window always fires
        self.wallet         = wallet
        # Baselines snapshotted at startup / each report
        self.last_portfolio = 0.0
        self.last_fills     = 0
        self.last_orders    = 0

    def snapshot_baseline(self, bankroll: float) -> None:
        """Call once after startup so the first report has a comparison point."""
        self.last_portfolio = round(bankroll + get_positions_value(self.wallet), 2)
        log.info(f"  P&L baseline: ${self.last_portfolio:.2f} (cash + positions)")

    def maybe_report(self, om: OrderManager, bankroll: float,
                     force: bool = False) -> None:
        # Fire every 24h from the last report, or immediately if forced.
        # Previously tied to 9am UTC — that caused missed reports on restarts.
        if not force and time.time() - self.last_report < PNL_REPORT_HOURS * 3600:
            return

        uptime    = str(datetime.now(timezone.utc) - self.start).split(".")[0]
        pos_value = get_positions_value(self.wallet)
        portfolio = round(bankroll + pos_value, 2)
        change    = portfolio - self.last_portfolio if self.last_portfolio > 0 else 0.0
        pct       = (change / self.last_portfolio * 100) if self.last_portfolio > 0 else 0.0
        fills     = om.fill_count  - self.last_fills
        orders    = om.order_count - self.last_orders
        emoji     = "📈" if change >= 0 else "📉"

        log.info(f"[24h] uptime={uptime} {om.summary()} "
                 f"portfolio=${portfolio:.2f} ({change:+.2f})")

        # Cash drawdown: compare current cash vs peak cash — what the guard uses.
        # Positive = above peak (good), negative = drawdown (guard may intervene).
        cash_vs_peak = ((bankroll - _dd_guard.peak) / _dd_guard.peak * 100) if _dd_guard.peak > 0 else 0.0
        dd_label = f"⚠️ {abs(cash_vs_peak):.1f}% drawdown" if cash_vs_peak < -DRAWDOWN_WARN_PCT * 100 else f"{cash_vs_peak:+.1f}% vs peak cash"
        tg(f"{emoji} <b>DAILY P&L REPORT</b>\n"
           f"━━━━━━━━━━━━━━━━━\n"
           f"Portfolio: <b>${portfolio:.2f}</b>  ({change:+.2f} / {pct:+.1f}%)\n"
           f"Cash: ${bankroll:.2f}  ·  Positions: ${pos_value:.2f}\n"
           f"Peak cash: ${_dd_guard.peak:.2f}  ({dd_label})\n"
           f"━━━━━━━━━━━━━━━━━\n"
           f"24h orders: {orders}  ·  24h fills: {fills}\n"
           f"Open positions: {len(om.held_positions)}\n"
           f"Committed to open orders: ${om.committed_usdc():.2f}\n"
           f"Win-rate: {_wr_tracker.summary()}\n"
           f"Kelly: {_effective_kelly:.4f} (base {KELLY_FRACTION})\n"
           f"Uptime: {uptime}")

        self.last_portfolio = portfolio
        self.last_fills     = om.fill_count
        self.last_orders    = om.order_count
        self.last_report    = time.time()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def run_loop(client: ClobClient, wallet: str) -> None:
    """Inner loop — runs until KeyboardInterrupt or unrecoverable error."""
    om  = OrderManager(get_existing_positions(wallet))
    # Merge fills from the last 2h that /positions may not have indexed yet —
    # prevents double-buying the same market right after a restart
    for tid, entry in get_recent_buys(wallet).items():
        if tid not in om.held_positions:
            om.held_positions[tid] = entry
            om.recent_fill_ts[tid] = time.time()
    pnl = PnL(wallet)

    if MANUAL_BANKROLL > 0:
        bankroll = MANUAL_BANKROLL
        log.info(f"  Using manual bankroll: ${bankroll:.2f}")
    else:
        bankroll = get_usdc_balance(client, wallet) if not DRY_RUN else MAX_BET_USDC * 20
    last_reset = time.time()

    log.info(f"Loop started. Balance: ${bankroll:.2f}  Positions loaded: {len(om.held_token_ids)}")
    _load_state()   # restore win-rate, cooldowns and DD peak from previous session
    pnl.snapshot_baseline(bankroll)
    # Drain any stale Telegram updates so old /report commands don't double-fire
    _poll_tg_commands()
    # Send a startup report so any missed daily reports are covered immediately
    pnl.maybe_report(om, bankroll, force=True)
    last_pos_sync = time.time()

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

        # ── Drawdown guard + dynamic Kelly ────────────────────────────────────
        global _effective_kelly, _dd_mult
        _dd_guard.update(bankroll)
        _dd_mult = _dd_guard.kelly_multiplier(bankroll)   # cached for kelly_buy per-type calc
        dd_mult  = _dd_mult
        wr_mult  = _wr_tracker.kelly_multiplier()         # "all" — for display only
        _effective_kelly = round(KELLY_FRACTION * dd_mult * wr_mult, 4)
        if _dd_guard.is_paused:
            log.warning(f"  🚨 Trading paused (drawdown). "
                        f"Bankroll ${bankroll:.2f} / peak ${_dd_guard.peak:.2f}. "
                        f"Win-rate: {_wr_tracker.summary()}")
            time.sleep(POLL_INTERVAL)
            continue
        if _effective_kelly < KELLY_FRACTION:
            log.info(f"  Kelly reduced: {_effective_kelly:.4f} "
                     f"(dd×{dd_mult:.2f} wr×{wr_mult:.2f})")

        # ── Check for fills ────────────────────────────────────────────────────
        fills = om.refresh_from_api(client)
        for f in fills:
            tg(f"💰 <b>FILL</b>  {f['label']}\n"
               f"{f['size_matched']:.2f}sh @ {f['limit']:.3f} = ${f['filled_usdc']:.2f}\n"
               f"Fair: {f['fair']:.3f}  Edge: {f['edge_at_fill']:+.3f}")

        # ── Cancel GTC orders older than MAX_ORDER_AGE_HOURS ─────────────────
        om.cancel_aged(client)
        # ── Cancel hourly orders whose market expires in <5 min ──────────────
        om.cancel_expiring_hourly(client, mins_before=5.0)

        # ── Re-sync positions from API so memory matches reality ───────────────
        if cycle_start - last_pos_sync > POSITION_SYNC_MINS * 60:
            om.sync_positions(wallet)
            last_pos_sync = cycle_start

        # ── Exit held positions at take-profit / stop-loss ─────────────────────
        scan_exit_positions(client, om, wallet)

        # ── Refresh balance every 5 min ────────────────────────────────────────
        if not DRY_RUN and MANUAL_BANKROLL == 0 and int(cycle_start) % 300 < POLL_INTERVAL:
            bankroll = get_usdc_balance(client, wallet)

        current_fairs: dict[str, float] = {}
        cycle_orders  = 0
        all_markets:  list[dict] = []   # accumulated for arb scan (no re-fetch)

        if not TRADE_DAILY_MARKETS:
            log.info("Daily markets paused (TRADE_DAILY_MARKETS=False — daily WR 45%)")

        for asset, cfg in ASSETS.items():
            if not TRADE_DAILY_MARKETS:
                break  # skip the entire daily scan

            spot = get_price(cfg["coingecko_id"], cfg["binance_symbol"])
            if not spot:
                log.warning(f"No price for {asset}, skipping")
                continue

            log.info(f"\n{'─'*24} {asset} ${spot:,.2f} {'─'*24}")
            drift   = get_live_drift(cfg["binance_symbol"], cfg["drift"])
            markets = collect_markets(asset, cfg)
            all_markets.extend(markets)
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

        # ── YES+NO structural arbitrage scan ─────────────────────────────────
        log.info(f"\n{'─'*24} ARB SCAN {'─'*24}")
        try:
            arb_placed = scan_yes_no_arb(client, om, all_markets, bankroll)
            cycle_orders += arb_placed
            if arb_placed == 0:
                log.info("  No arb opportunities this cycle.")
        except Exception as e:
            log.debug(f"  Arb scan error: {e}")

        # ── Hourly up/down markets (momentum-based, no strike) ────────────────
        log.info(f"\n{'─'*24} HOURLY MARKETS {'─'*24}")
        try:
            hourly_placed = scan_hourly_markets(client, om, bankroll)
            cycle_orders += hourly_placed
            if hourly_placed == 0:
                log.info("  No hourly edge this cycle.")
            # Register hourly fairs so stale hourly orders get cancelled too.
            # Only fill in tokens NOT already written by scan_hourly_markets —
            # that call uses the correct blended (option-weighted) fair; calling
            # compute_hourly_fair() without spot/mins_left here would overwrite
            # with a momentum-only fair and incorrectly cancel near-expiry orders.
            for m, asset, bsym in _collect_hourly_markets():
                raw = m.get("clobTokenIds") or []
                if isinstance(raw, str):
                    try: raw = json.loads(raw)
                    except: raw = []
                if len(raw) >= 2 and raw[0] not in current_fairs:
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

        # ── Telegram command listener (/report sends an instant P&L) ──────────
        for cmd in _poll_tg_commands():
            if cmd in ("/report", "/status", "/pnl"):
                log.info(f"Telegram command received: {cmd} — sending instant report")
                pnl.maybe_report(om, bankroll, force=True)

        # ── Adaptive sleep: 10s if any hourly market is within 30 min of expiry ──
        try:
            near_expiry = any(
                _hourly_mins_remaining(m) < 30
                for m, _, _ in _collect_hourly_markets()
            )
        except Exception:
            near_expiry = False
        effective_interval = 10 if near_expiry else POLL_INTERVAL

        elapsed   = time.time() - cycle_start
        sleep_for = max(1.0, effective_interval - elapsed)
        log.info(f"Cycle {elapsed:.1f}s — next in {sleep_for:.0f}s"
                 f"{' ⚡fast' if near_expiry else ''}  |  {om.summary()}\n{'='*72}")

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

    # Positions/balances live under the PROXY wallet, not the EOA —
    # all Data API queries must use it or they return empty.
    data_wallet = os.getenv("POLY_FUNDER") or _get_proxy_wallet_address(wallet) or wallet
    if data_wallet != wallet:
        log.info(f"Data wallet (proxy): {data_wallet}")

    restart_delay = 30
    while True:
        try:
            client = build_client()
            run_loop(client, data_wallet)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            tg("🛑 <b>Bot stopped</b> (KeyboardInterrupt)")
            break
        except Exception as e:
            log.error(f"Crash: {e} — restarting in {restart_delay}s…")
            tg(f"⚠️ <b>Bot crashed — restarting in {restart_delay}s</b>\n"
               f"<code>{str(e)[:300]}</code>")
            time.sleep(restart_delay)
            restart_delay = min(restart_delay * 2, 300)  # cap at 5 min
        else:
            break


if __name__ == "__main__":
    run()
