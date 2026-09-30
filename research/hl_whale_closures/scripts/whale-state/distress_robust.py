# Robustness of the only S-grid cell that passed TEST: S_DISTRESS L24 fad
# (sim trigger with whale PnL < 0 AND liq distance < 5%; we trade AGAINST the whale's closing trade; 24 h; market-neutral vs EW alt idx).
# No new selection: same events, same dedupe; only sensitivity views. Output: out_distress_robust.txt
import sys, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
import ws
from ws import *
P_ = np.load(W + 'pxmat.npz'); mins = P_['mins']; C = P_['C']; syms = list(P_['syms']); A = P_['altidx']; t0 = mins[0]
si = {s: i for i, s in enumerate(syms)}; ib = si['BTCUSDT']
S = pd.read_parquet(W + 'sim_events_out.parquet'); S['cd'] = S.coin + '|' + S.day
ws.STATES['S_DISTRESS'] = lambda d: ((d.wpnl < 0) & (d.liq_dist < 5), np.ones(len(d), bool))
out = open(W + 'out_distress_robust.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n'); out.flush()

def fwd(x, delay_min, h, kind='mn'):
    """our-side (fade) return from entry at first full minute after dec + delay, h hours; kind mn (vs alt idx), raw, btc."""
    j0 = ((x.dec_ms.values - t0) // 60000).astype(int) + 1 + delay_min
    jh = j0 + int(60 * h)
    k = x.symbol.map(si).values
    ok = (jh < C.shape[1]) & ~pd.isna(k)
    kk = np.where(pd.isna(k), 0, k).astype(int); j0c = np.clip(j0, 0, C.shape[1] - 1); jhc = np.clip(jh, 0, C.shape[1] - 1)
    r = C[kk, jhc] / C[kk, j0c] - 1
    a = A[np.clip(jhc + 1, 0, len(A) - 1)] / A[np.clip(j0c + 1, 0, len(A) - 1)] - 1
    b = C[ib, jhc] / C[ib, j0c] - 1
    side = -x.wdir.values   # fade
    v = {'mn': r - a, 'raw': r, 'btc': r - b}[kind]
    return np.where(ok, 100 * side * v, np.nan)

def show(lab, x, y):
    x = x.assign(y=y); x = x[x.y.notna()]
    if len(x) < 3:
        P(f'  {lab:38s} n={len(x)}'); return
    o = battery(x)
    P(f'  {lab:38s} n={o["n"]:4d} mean={o["mean"]:+.3f} med={o["median"]:+.3f} win={o["win"]:.2f} ciC={o["ci_c"]} ciD={o["ci_d"]} ciW={o["ci_w"]} '
      f'h1/h2={o["half1"]:+.2f}/{o["half2"]:+.2f} s_wo10={o["sum_wo_top10"]:+.1f} maxsh c/w/d={o["maxsh_coin"]:.2f}/{o["maxsh_addr"]:.2f}/{o["maxsh_day"]:.2f} '
      f'L {o["n_long"]}:{o["mean_long"]:+.2f} S {o["n_short"]}:{o["mean_short"]:+.2f}')

for per, d in (('DISC 27.08-21.09', S[S.day <= DISC_END]), ('TEST 22.09-29.09', S[(S.day >= TEST_START) & (S.day <= TEST_END)])):
    d = d.reset_index(drop=True)
    x = cell_frame(d, 'S_DISTRESS', 'L24', 'fad')      # declared cell (y = mn net incl. funding, cost 0.146)
    P(f'\n==== {per}: declared cell S_DISTRESS L24 fad')
    show('declared (mn, -0.146, -funding)', x, x.y.values)
    fund = (-1) * x.wdir.values * x.fund24.fillna(0).values
    show('+ hedge leg cost 0.10', x, x.y.values - 0.10)
    show('costs 0.10 instead of 0.146', x, x.y.values + 0.046)
    show('raw (no alt hedge) net', x, fwd(x, 0, 24, 'raw') - 0.146 - fund)
    show('BTC-neutral net', x, fwd(x, 0, 24, 'btc') - 0.146 - fund)
    for dl in (15, 60, 180):
        show(f'entry delayed +{dl} min (mn net)', x, fwd(x, dl, 24, 'mn') - 0.146 - fund)
    for h in (4, 8, 48, 72):
        show(f'horizon {h}h (mn gross-0.146)', x, fwd(x, 0, h, 'mn') - 0.146)
    P('  per coin (n, mean):', x.groupby('coin').y.agg(['size', 'mean']).round(2).sort_values('size', ascending=False).head(12).to_dict('index'))
    top = x.groupby('coin').y.sum().sort_values(ascending=False)
    show('without best coin', x[x.coin != top.index[0]], x[x.coin != top.index[0]].y.values)
    show('without best 3 coins', x[~x.coin.isin(top.index[:3])], x[~x.coin.isin(top.index[:3])].y.values)
    P(f'  state at trigger: median wpnl {x.wpnl.median():.1f}%  median liq_dist {x.liq_dist.median():.2f}%  whale side buys(short-cover) share {(x.wdir==1).mean():.2f}')
    P(f'  past mn 24h (whale dir) median {x.past_mn_24h.median():+.2f}  72h {x.past_mn_72h.median():+.2f}  7d {x.past_mn_7d.median():+.2f}')
    # placebo 1: same coin & same fade side, random decision times in the same period (1000 draws), same dedupe, mn net
    rng = np.random.default_rng(7)
    lo, hi = d.dec_ms.min(), d.dec_ms.max()
    pm = []
    for _ in range(300):
        z = x.copy(); z['dec_ms'] = rng.integers(lo, hi, len(z))
        yv = fwd(z, 0, 24, 'mn') - 0.146 - fund
        pm.append(np.nanmean(yv))
    pm = np.array(pm)
    P(f'  placebo same coin+side, random time in period: mean {np.nanmean(pm):+.3f}  sd {np.nanstd(pm):.3f}  P(placebo>=obs {x.y.mean():+.3f}) = {(pm >= x.y.mean()).mean():.3f}')
    # placebo 2: all sim events with liq_dist >= 5 or pnl >= 0 (non-distress) in the same coins, fade, L24 mn
    nd = d[~(((d.wpnl < 0) & (d.liq_dist < 5)).values) & d.coin.isin(x.coin.unique())].copy()
    nd['side_key'] = -nd.wdir; nd['y'] = metric(nd, 'L24', 'fad'); nd = dedupe(nd[nd.y.notna()], 'L24')
    show('non-distress events, same coins, fade', nd, nd.y.values)
    # portfolio
    x = x.sort_values('dec_ms')
    daily = x.groupby('day').y.sum()
    ends = x.dec_ms.values + 24 * 3600e3; conc = max(((x.dec_ms.values[:, None] >= x.dec_ms.values[None, :]) & (x.dec_ms.values[:, None] < ends[None, :])).sum(1))
    cum = daily.cumsum(); dd = (cum - cum.cummax()).min()
    P(f'  portfolio (equal notional per trade, trade-% summed by entry day): total {x.y.sum():+.1f}, positive days {(daily>0).mean():.2f} of {len(daily)}, maxDD {dd:+.1f}, max concurrent {conc}')
