# Context for all research agents (read fully before starting)

## The system
A paper-trading bot ("HL Whale Terminal") watches ~hundreds of whale wallets on Hyperliquid (HL) perpetuals.
When a whale CLOSES part of a big position with a taker order, the bot reacts on **Binance USDT-M futures**
(since 13.09 all prices/exec are Binance; before 13.09 "live" prices were HL book snapshots that were ~2 s stale).
- `ratio` = whale position USD / HL order-book depth within 1% of mid (the "depth_usd"). ratio 5 = position is 5x the 1% depth.
- `tx_pct` = share of the whale's position closed by one transaction. Bot gate: tx >= 5% of position, >= $5k, ratio >= 2.
- F-strategies (follow): trade in the whale's trade direction (whale sells to close long -> we SHORT). Exit on 60 s of whale silence
  (F1), 2 min (F2), 3 min (F3), or on the whale's full close. R-strategies (reverse/fade) after the whale's close episode.
  T-strategies: follow HL TWAP orders (T1 = TWAP opening, T2 = TWAP reducing a position), exit after 60 min.
- Official metric `net_official_pct` (settlements.csv) = min(live, tape) where tape = Binance aggTrades, entry/exit at the WORST
  price within 3 s after the decision second; minus costs_pct (~0.146% per round trip) and funding. Very conservative.
- Detection latency from whale fill to bot decision: WS median ~1.2-1.8 s (q90 ~3.3 s); "sweep" source ~60 s; "scan" ~5 min.

## Data (all read-only; paths absolute)
SP = /tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/
- Bot data: SP/hl_whale_export_20260930/02_bot_data/  (README: SP/hl_whale_export_20260930/00_README.md)
  follow_trades.csv (+ .legacy-*.csv from 30.08, algo 2.10/2.15, HL-book prices), rev_trades.csv, rev_signals.csv, rev_outcomes.csv,
  follow_outcomes.csv, twap_trades.csv, twap_signals.csv (6399 TWAPs >= $100k seen 09.09-30.09, incl. 'ineligible' long ones),
  twap_curves.csv, settlements.csv (17.5k rows, key = trade_id | sig_id|strategy | twap_id|strategy), sim_trades.csv (bot's
  own simulator incl. whale_pnl_pct, liq_dist), fc_trades.csv, prio_fetch.csv (whale position fetches: notional, depth, result),
  wallet_profiles.json, events-YYYYMMDD.jsonl (primary log 12.09-30.09: close_batch = every batch of whale close txs with raw
  px/pf/pl/sz/startPosition; follow_exit; rev_decision). Last CSV column '^' is a sentinel: rows without it are truncated.
- Prior research datasets: SP/hl_whale_export_20260930/03_research_datasets/ (exitk/, speed/, tx2/ each with README.txt)
- Prior reports: SP/hl_whale_export_20260930/04_reports/ (INDEX.md) and the main findings SP/hl_whale_export_20260930/01_FINDINGS_sections/B_stats_wallets.md
- NEW (built for this study):
  - SP/data/whale_txs.parquet: 458k UNIQUE whale close transactions from close_batch 12.09-30.09 (addr, coin, wside = whale
    position side, ts ms, px vwap, pf first fill px, pl last fill px, sz, sp = startPosition before tx, dir, liq (1 = system fill:
    TWAP slice/liquidation/ADL), nf fills, bot_ts (when bot saw it), detect_src, batch_ratio, depth_usd, tx_usd, tx_pct, sweep_pct).
  - SP/data/close_batches.parquet: 68.9k batches (old_size/new_size/old_val/ratio/full_close/detect_src/n_passive).
  - SP/data/tx_matrix.parquet (txs with tx_usd >= $1000, ~87k): Binance returns in the WHALE TRADE direction (wdir = +1 whale buys,
    i.e. Close Short; -1 whale sells). px_base = last Binance price in the second BEFORE the tx. r{o} = % move at offset o seconds
    (o in -300,-60,-10,-3,-1,0,1,2,3,5,10,20,30,60,120,300,600,1800,3600), wentry1/wentry2 = % paid vs px_base when entering
    in whale dir at worst-in-3s after 1 s / 2 s. qv_pre60/qv_post60 Binance quote volume, tbuy_* taker-buy share. R{h}h = returns
    at 2,4,8,24,48,72 h from 1m klines; BTC{h}h, BTCr{o} = BTC return over the same windows (same sign convention).
    IMPORTANT: r-columns are measured from BEFORE the tx; a tradable entry is only possible at ts + latency (>= 1 s).
  - SP/data/bars1s/{SYMBOL}/{YYYY-MM-DD}.npz: sparse 1-second Binance futures bars from aggTrades for every (symbol, day) with events
    (2096 symbol-days, 25.08-29.09). SP/data/k1m/{SYMBOL}/{day}.npz: 1-minute klines for all 150+ symbols 20.08-29.09.
    Binance has no 30.09 file yet -> Binance-priced analyses end 29.09 23:59 UTC.
  - SP/data/hl/{COIN}_{1m,5m,15m}.json: HL candles (latest 5000 per interval: 1m ~ last 3.5 days, 5m ~ 17 days, 15m ~ 52 days);
    {COIN}_funding.json HL funding history.
  - SP/infra/symmap.json HL coin -> Binance symbol.
  - Library: SP/lib/hl.py (read its docstring): bars(), worst_px(), last_px(), kl(), px_at_min(), ret_min(), boot_ci(),
    summary(), honesty(). Its worst_px on 1s bars reproduces settlement tape prices exactly in ~60-70% of rows and otherwise is
    slightly MORE conservative (never kinder).
- Readiness: bars1s and tx_matrix.parquet are being built at launch time. Before using them check that the file SP/data/READY
  exists; if not, wait for it in ONE Bash call: `until [ -f SP/data/READY ]; do sleep 20; done` (timeout 900000 ms).
- Environment: 4 CPU cores and 15 GB RAM shared with one other agent. Keep memory < 5 GB, avoid loading all bars at once,
  vectorize. Network: only data.binance.vision and api.hyperliquid.xyz (read-only public endpoints) if truly needed.
  Write ONLY inside your own work dir SP/work/<your-lens>/ (scripts + outputs). Python has pandas, numpy, scipy,
  statsmodels, sklearn, pyarrow.

## What prior research (30.08-22.09, ~45 000 variants, 25 workflows) already established — do NOT redo, only extend
DEAD (robustly negative after costs): all follow F1/F2/F3/F6/F8/F10 (gross ~0, costs 0.146 => net ~ -0.15..-0.2);
exit tuning (fixed minute 1..60, silence timers, SL, TP as profit source, trailing); delayed entry, limit entry, entry on 2nd tx;
wallet selection by history ("good wallets", profile fields, pair rules) - regression to mean; coin exclusion; ML on entry features
(AUC 0.62-0.69 not enough); BTC veto, hour, weekend; reverse R1-R8 on Binance prices; T1; mirror/fade of F events.
Structural facts: price impact +0.10% happens within 1 s BEFORE any possible entry, +0.20% within 60 s; direction of the whale is
worth ~+0.1..0.3 pp, less than costs; HL moves ~2x Binance in dumps (|Binance move|/|HL move| ~ 0.54-0.56, HL move is largely the
whale's own footprint in a thin book); exit at whale's full close is +0.18 vs silence timer -0.19 but not tradable (not predictable);
LONG (following short-closers) looked better than SHORT, attributed to alt up-drift + coin mix; 78% of wallets trade one coin.
REGISTERED (to be checked only on NEW data): T2 TWAP-reduce follow (decision ~01.10); F4 paired vs F1; F1-PROF (prof ok & tx<20% &
lag<2 s); H1/H2 TP stack (tx<20% & lag 0-2 s [& wallet exclusion], TP +1.0 / SL -1.5 / cap 30 min, from 20.09); tx2 rules
(ratio2>=5 & gap<=40 s; ratio2>=10 any gap, from 22.09); pair 0x523852be/CHIP and 0x0871deb3/VVV; "one trade per pair per day" brake;
REV after big fast dump (move3 >= 1.5%, TP +3%/60 min; in-sample to 13.09 mean +1.03, decaying by thirds).

## Honesty standard (mandatory for every number you call a result)
- Unit of analysis = independent EVENT/EPISODE, not rows (bursts of txs by one whale in one coin are ONE event; prior work found
  x3 duplication). Say how you deduplicated.
- No look-ahead: a rule may only use information available at decision time (tx seen at ts + detection latency; use >= 1.5 s for
  WS-detected txs, 60 s for sweep, the batch's bot_ts is when the bot actually knew). Entry/exit prices: worst-in-3s rule on 1s bars
  unless you explicitly model maker orders. Costs: 0.146% per round trip taker (official); report sensitivity for 0.10 and maker.
- Time split: DISCOVERY = up to 21.09 inclusive, TEST = 22.09-29.09 (nobody has looked at it). Choose rules on discovery, evaluate
  test ONCE. Also report walk-forward if you tune anything.
- Cluster bootstrap CI90 for mean and median by wallet, by day, by coin (lib.boot_ci / honesty). Both halves. Sum without top 10%.
  Max share of positive sum by one wallet/day/coin. Count K = number of variants you looked at and report what chance alone would
  produce (permutation/placebo with the SAME clustering: e.g. random times in the same coin and day, or mirrored direction).
- Market drift control: report returns net of BTC (and/or an equal-weight alt index) over the same window; LONG vs SHORT split.
- Be skeptical of your own positives. A result carried by <= 3 wallets, 1 coin or 1 day is not a result.
- Write every prose field in UKRAINIAN (numbers as numbers). Be concrete and short.

## TIME ZONE TRAP (verified)
All human-readable date strings in the bot CSVs (date, date_open, date_close, date_entry, start, end, settled_at ...) are LOCAL
time CEST = UTC+2. All *_ms / *_ts columns and jsonl `ts` are UTC epoch. Example: follow date_open '2026-09-12 21:36:20' ==
open_ts_ms 1789241780197 == 19:36:20 UTC. Convert strings with pd.Timestamp(s) - 2h before comparing to ms timestamps.

## PHASE 2 additions (read this too)
New data: SP/data/k1m/ now also covers 2026-03-01..2026-08-19 (so ~7 months of 1m klines, 150+ symbols; some symbols listed later);
SP/data/metrics/{SYMBOL}/{day}.csv = Binance 5-min open interest (sum_open_interest, sum_open_interest_value), top trader and
all-account long/short ratios, taker long/short vol ratio, 2026-03-01..2026-09-29. Wait for SP/data/READY_HIST before using them
(`until [ -f SP/data/READY_HIST ]; do sleep 20; done`, timeout 900000).
Live recording (30.09 20:04-22:04 UTC, 35 coins): SP/work/live/hl_trades.parquet (every HL trade with buyer/seller address, px, sz,
time ms, r = local receive ms), hl_trades_fair.parquet (+ Binance fair, deviation), hl_book.parquet (HL l2Book 20 levels, json in
'levels'), bn_book.parquet (Binance bookTicker, T ms), positions.parquet (clearinghouseState of 120 whales every ~5 min).
Facts from live data: HL public WS delivers trades ~312 ms (median) after HL block time; dislocations >= 1% below fair happen
~3 coin-minutes per 2 h over 35 coins, 87-100% of such prints have a tracked whale as taker; after a big sweep fast bots
(some are in our whale list) buy the dislocation back within 0.4-1.5 s.
PHASE 1 RESULTS (7 lenses + 7 skeptics, full JSON in SP/work/results/): every registered hypothesis failed out of sample; all
follow/fade taker strategies dead (80-90% of the move happens before any entry; round-trip friction 0.33-0.45%); TWAP: move
happens on the first slice; latency value only below 150 ms. Survivors (all 'weak'): A2 = post-only bid on HL 1% below Binance fair
for 60 s after a tracked whale sweep with tx_usd/depth >= 0.3 (fills come from the SAME whale's continuation sweeps; first hits
lose): skeptic-adjusted +0.15..+0.22% per fill, ~4 fills/day, 93% LONG; S_DISTRESS-SHORT (short whale in loss closes -> short coin
24 h hedged; NOTE liq_dist in sim_trades is a FRACTION, <5 means liquidation within 500%, i.e. a leverage proxy): +0.2..+0.8 with
CI lower bound ~0, carried by NIL and 28.09; generic 'fade a fast Binance dump >= 1.5%' where the whale adds nothing over the
no-whale control (+0.10).
