
Polymarket edge bot

Prices Polymarket prediction markets against an independently modelled fair value and places limit orders only where the modelled edge clears fees and a minimum threshold.

Currently runs in paper mode (DRY_RUN = True).

What it does

Markets covered

Hourly up/down crypto markets, scored on a 6-signal consensus
Daily price-target markets across 18 crypto and commodity assets (BTC, ETH, SOL, XRP, DOGE, AVAX, LINK, SUI, PEPE, ADA, TON, BNB, LTC, WIF, NEAR, DOT, TRX, XAU)
YES+NO structural arbitrage where both legs are mispriced against each other
Politics and event markets on a separate 3-signal model, sized more conservatively

Pricing

Fair value is built from a European digital option model with barrier/touch handling, using a volatility term structure fitted across 7d/14d/30d windows. Market inputs come from spot, short-horizon momentum and top-of-book imbalance, sourced independently of Polymarket so the model isn't anchored to the price it's trying to evaluate.

Execution

GTC limit orders placed relative to the book rather than at market, cross-checked against live order book state, with stale orders cancelled when fair value drifts more than 12% from the price the order was placed at.

Sizing

Fractional Kelly (0.45) against available bankroll net of committed capital, with a hard per-order cap and a per-market exposure ceiling of the lesser of a fixed limit or 12% of bankroll. Politics markets are sized at 0.55× the crypto fraction.

Safety

Position awareness on startup so a restart doesn't double up, exponential-backoff retries on API failures, a crash-proof main loop with auto-restart, and Telegram alerts limited to real fills and bot stops rather than every scan.

Thresholds, and why they are where they are

The tuning constants at the top of polymarket_btc_bot.py are annotated with the observation that moved each one. A few examples:

Constant	Value	Reason
MIN_EDGE	0.03	Raised from 2% to match the quality bar the hourly markets actually cleared
STOP_LOSS	0.30	Reverted from 0.20 — the tighter stop turned a 61% win rate into 27% by exiting on normal noise
MIN_VOLUME	15,000	Raised from 5k; thin markets have poor price discovery and the fills weren't real
MAX_MODEL_MARKET_GAP	0.20	Tightened from 30% — when the model disagrees with the market by more than 20¢, the market is usually right
MIN_DAILY_CONVICTION	0.63	Tightened after an observed 33% win rate on daily markets

Shadow prediction tracking records what the model would have done on markets it skipped, so the paper run produces a measurable answer rather than an impression.

Files
File	Purpose
polymarket_btc_bot.py	Main bot — pricing, scanning, sizing, execution
polymarket_btc_hourly_bot.py	Earlier, simpler hourly-only version
requirements.txt	Dependencies
railway.toml	Deployment config
Setup
bash
pip install -r requirements.txt
cp .env.example .env   # add your Polymarket API credentials
python polymarket_btc_bot.py

Credentials are read from the environment and are never committed. DRY_RUN = True is the default — set it to False only deliberately.

Status and known work
Running in paper mode; MAX_BET_USDC and the exposure ceilings are sized for a small live bankroll, not a production one.
polymarket_btc_bot.py has grown into a single large module and needs decomposing into pricing, scanning, sizing and execution units.
Polymarket removed the hourly crypto markets during development; the daily scanner is currently the only active crypto path.
