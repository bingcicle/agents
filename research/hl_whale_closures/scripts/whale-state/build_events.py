# Build the whale-state event table.
# Event = first bot-gate tx (liq==0, tx_usd>=5k, tx_pct>=5, batch_ratio>=2, Binance symbol) of an episode
# (per addr x coin x whale-trade-dir; a new episode starts after > 30 min without a gate tx).
# State features use ONLY information known at decision time dec_ms = max(ts+1500, bot_ts*1000)
# (other wallets' txs count only if their bot_ts <= dec_ms).
import sys, json, numpy as np, pandas as pd, time
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
sys.path.insert(0, SP + 'lib')
from hl import SYM
W = SP + 'work/whale-state/'
t00 = time.time()
tx = pd.read_parquet(SP + 'data/whale_txs.parquet')
tx['wdir'] = np.where(tx.dir.isin(['Close Short', 'Short > Long', 'Open Long']), 1, -1)
tx['symbol'] = tx.coin.map(SYM)
tx['bot_ms'] = (tx.bot_ts * 1000).astype(np.int64)
tx = tx.sort_values('ts').reset_index(drop=True)

# ---- tracked-since per addr (first close_batch the bot logged for that wallet)
cb = pd.read_parquet(SP + 'data/close_batches.parquet')
tracked = (cb.groupby('addr').bot_ts.min() * 1000).astype(np.int64)
tx['tracked_ms'] = tx.addr.map(tracked)

# ---- universe U: tx at which the wallet's rolling-60s close in this coin/dir reaches >=5% of the position AND >=$5k,
#      position ratio (batch_ratio) >= 1, liq==0 (see count_universe.py). Episodes: new one after >30 min without such a tx.
R = pd.read_parquet(W + 'txs_roll.parquet', columns=['h', 'addr', 'coin', 'r_usd', 'r_pct'])
tx = tx.merge(R, on=['h', 'addr', 'coin'], how='left')
g = tx[(tx.liq == 0) & (tx.r_usd >= 5000) & (tx.r_pct >= 5) & (tx.batch_ratio >= 1) & tx.symbol.notna()].copy()
g = g.sort_values(['addr', 'coin', 'wdir', 'ts'])
gap = g.groupby(['addr', 'coin', 'wdir']).ts.diff()
g['new_ep'] = gap.isna() | (gap > 30 * 60000)
g['ep_id'] = g.new_ep.cumsum()
# episode aggregates known only AFTER the fact (for description only, never used in rules)
epagg = g.groupby('ep_id').agg(ep_ngate=('ts', 'size'), ep_usd=('tx_usd', 'sum'), ep_end=('ts', 'max'))
ev = g[g.new_ep].copy().join(epagg, on='ep_id')
ev['dec_ms'] = np.maximum(ev.ts + 1500, ev.bot_ms)
ev['dec_sec'] = np.ceil(ev.dec_ms / 1000).astype(np.int64)
ev['lat_s'] = (ev.dec_ms - ev.ts) / 1000
ev['day'] = pd.to_datetime(ev.ts, unit='ms').dt.strftime('%Y-%m-%d')
ev['td'] = ev.tx_usd / ev.depth_usd
ev['rtd'] = ev.r_usd / ev.depth_usd
print('gate txs', len(g), 'episodes/events', len(ev), round(time.time() - t00), 's')

# ---- herding + stage features (per coin arrays)
feat = {}
for coin, e in ev.groupby('coin'):
    a = tx[tx.coin == coin]
    ts = a.ts.values; bm = a.bot_ms.values; ad = a.addr.values; wd = a.wdir.values; usd = a.tx_usd.values; lq = a.liq.values
    for idx, r in e.iterrows():
        out = {}
        for Wm in (5, 30, 120):
            i0 = np.searchsorted(ts, r.ts - Wm * 60000, 'left'); i1 = np.searchsorted(ts, r.ts, 'left')  # strictly before the event tx
            known = bm[i0:i1] <= r.dec_ms
            other = ad[i0:i1] != r.addr
            same = wd[i0:i1] == r.wdir
            m_s = known & other & same; m_o = known & other & ~same; m_self = known & ~other & same
            u = usd[i0:i1]
            if m_s.any():
                s = pd.Series(u[m_s]).groupby(ad[i0:i1][m_s]).sum()
                out[f'nw_same_{Wm}'] = int((s >= 1000).sum()); out[f'usd_same_{Wm}'] = float(s.sum())
            else:
                out[f'nw_same_{Wm}'] = 0; out[f'usd_same_{Wm}'] = 0.0
            if m_o.any():
                s = pd.Series(u[m_o]).groupby(ad[i0:i1][m_o]).sum()
                out[f'nw_opp_{Wm}'] = int((s >= 1000).sum()); out[f'usd_opp_{Wm}'] = float(s.sum())
            else:
                out[f'nw_opp_{Wm}'] = 0; out[f'usd_opp_{Wm}'] = 0.0
            out[f'usd_self_{Wm}'] = float(u[m_self].sum())
            out[f'nliq_{Wm}'] = int((lq[i0:i1][known] == 1).sum())
        # stage: time since previous tx (>= $1k, any direction) of the same wallet in this coin, known at decision
        i1 = np.searchsorted(ts, r.ts, 'left')
        mm = (ad[:i1] == r.addr) & (usd[:i1] >= 1000) & (bm[:i1] <= r.dec_ms)
        prev = ts[:i1][mm]
        out['gap_prev_h'] = (r.ts - prev[-1]) / 3.6e6 if len(prev) else np.nan
        out['tracked_age_h'] = (r.ts - r.tracked_ms) / 3.6e6
        feat[idx] = out
F = pd.DataFrame.from_dict(feat, orient='index')
ev = ev.join(F)
print('features', round(time.time() - t00), 's')

# fresh flags: previous activity gap >= X h, or no previous tx while the wallet was already tracked >= X h
for X in (6, 24):
    ev[f'fresh{X}'] = np.where(ev.gap_prev_h.notna(), ev.gap_prev_h >= X, ev.tracked_age_h >= X).astype(int)
    ev[f'fresh{X}_known'] = (ev.gap_prev_h.notna() | (ev.tracked_age_h >= X)).astype(int)
ev['near_full'] = (ev.tx_pct >= 80).astype(int)
ev['flip'] = ev.dir.isin(['Short > Long', 'Long > Short']).astype(int)

# ---- wallet type (past-only): distinct coins traded (>= $1k txs) before the event, tx count per tracked day; vault flag
txk = tx[tx.tx_usd >= 1000][['addr', 'coin', 'ts']]
ncoins = []; act = []
by_addr = {a: d for a, d in txk.groupby('addr')}
for idx, r in ev.iterrows():
    d = by_addr.get(r.addr)
    p = d[d.ts < r.ts] if d is not None else None
    ncoins.append(p.coin.nunique() if p is not None else 0)
    age_d = max(r.tracked_age_h / 24, 0.5)
    act.append((len(p) if p is not None else 0) / age_d)
ev['ncoins_past'] = ncoins; ev['tx_per_day_past'] = act
BOT = SP + 'hl_whale_export_20260930/02_bot_data/'
fv = pd.concat([pd.read_csv(BOT + 'follow_trades.csv', usecols=['whale_addr', 'vault']),
                pd.read_csv(BOT + 'follow_trades.csv.legacy-1789241806.csv', usecols=['whale_addr', 'vault'])])
vault = fv.groupby('whale_addr').vault.max()
ev['vault'] = ev.addr.map(vault).fillna(0).astype(int)

# ---- whale PnL / liq distance from sim_trades (bot simulator) : same addr+coin, sim open within [ts, ts+180 s]
s = pd.read_csv(BOT + 'sim_trades.csv')
s['t_ms'] = ((pd.to_datetime(s.date_open) - pd.Timedelta(hours=2)) - pd.Timestamp('1970-01-01')) // pd.Timedelta('1ms')
s = s.sort_values('t_ms')
wp = []
for idx, r in ev.iterrows():
    c = s[(s.whale_addr == r.addr) & (s.coin == r.coin) & (s.t_ms >= r.ts - 5000) & (s.t_ms <= r.ts + 180000)]
    if len(c):
        c = c.iloc[0]
        wp.append((idx, c.whale_pnl_pct, c.liq_dist, c.whale_ratio, c.unload_time_s, (c.t_ms - r.ts) / 1000, c.our_side))
wp = pd.DataFrame(wp, columns=['idx', 'whale_pnl', 'liq_dist', 'sim_ratio', 'sim_unload_s', 'sim_dt_s', 'sim_side']).set_index('idx')
ev = ev.join(wp)
print('sim match', ev.whale_pnl.notna().sum(), 'of', len(ev))
ev.to_parquet(W + 'events_state.parquet')
print('saved', len(ev), round(time.time() - t00), 's')
