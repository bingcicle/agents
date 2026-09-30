# whale-state evaluation library: states, cell metrics, dedupe, honesty battery, clustered state-label null.
import sys, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/whale-state/'
sys.path.insert(0, SP + 'lib')
from hl import boot_ci
COST = 0.146
DISC_END, TEST_START, TEST_END = '2026-09-21', '2026-09-22', '2026-09-29'

def load():
    ev = pd.read_parquet(W + 'events_out.parquet')
    try:
        p = pd.read_json(W + 'pnl_api.jsonl', lines=True)
        p = p[['h', 'addr', 'n', 'pnl_pct', 'entry', 't_fill']].rename(columns={'n': 'api_n', 'pnl_pct': 'wpnl'})
        ev = ev.merge(p, on=['h', 'addr'], how='left')
    except Exception:
        ev['wpnl'] = np.nan; ev['api_n'] = np.nan
    ev['hs30'] = ev.usd_same_30 / ev.depth_usd
    ev['ho30'] = ev.usd_opp_30 / ev.depth_usd
    ev['cd'] = ev.coin + '|' + ev.day
    return ev

# ---- states (boolean masks; NaN-aware states return (mask, known) )
STATES = {
    'HERD30_0':    lambda d: (d.nw_same_30 == 0, np.ones(len(d), bool)),
    'HERD30_1':    lambda d: (d.nw_same_30 == 1, np.ones(len(d), bool)),
    'HERD30_2P':   lambda d: (d.nw_same_30 >= 2, np.ones(len(d), bool)),
    'HERD120_3P':  lambda d: (d.nw_same_120 >= 3, np.ones(len(d), bool)),
    'HERDUSD30':   lambda d: (d.hs30 > 0.5, np.ones(len(d), bool)),
    'OPP30':       lambda d: (d.ho30 > 0.1, np.ones(len(d), bool)),
    'FRESH6':      lambda d: (d.fresh6 == 1, (d.fresh6_known == 1).values),
    'NEARFULL':    lambda d: (d.r_pct >= 50, np.ones(len(d), bool)),
    'MULTICOIN':   lambda d: (d.ncoins_past >= 3, np.ones(len(d), bool)),
    'WLOSS':       lambda d: (d.wpnl < 0, d.wpnl.notna().values),
    'WPROFIT':     lambda d: (d.wpnl > 3, d.wpnl.notna().values),
    'WDISTRESS':   lambda d: (d.wpnl < -3, d.wpnl.notna().values),
}
HOR = {'S300': 300, 'S1800': 1800, 'L8': 8 * 3600, 'L24': 24 * 3600}

def metric(d, hor, direction, mn=True, extra_cost=0.0):
    """net % for our side; direction 'fol' (whale dir) or 'fad'. mn: subtract EW-alt index over same window."""
    s = 1 if direction == 'fol' else -1
    if hor in ('S300', 'S1800'):
        H = HOR[hor]
        g = d[f'fol_{H}'] if s > 0 else d[f'fad_{H}']
        if mn:
            g = g - s * d[f'alt_s{H}'].fillna(0)
        return g - COST - extra_cost
    h = int(hor[1:])
    raw = s * d[f'L{h}']
    if mn:
        raw = raw - s * d[f'alt{h}']
    fund = s * d.wdir * d[f'fund{h}'].fillna(0)   # long pays positive funding
    return raw - fund - COST - extra_cost

def dedupe(d, hor):
    """one trade per coin & our side at a time: drop an event if a previous KEPT event of the same coin & side is still open."""
    H = HOR[hor] * 1000
    keep = np.zeros(len(d), bool)
    d = d.sort_values('dec_ms')
    last = {}
    for i, (k, t) in enumerate(zip((d.coin + d.side_key.astype(str)).values, d.dec_ms.values)):
        if k not in last or t >= last[k] + H:
            keep[i] = True; last[k] = t
    return d[keep]

def cell_frame(d, state, hor, direction, mask=None):
    if mask is None:
        m, known = STATES[state](d)
        m = np.asarray(m) & known
    else:
        m = mask
    x = d[m].copy()
    x['side_key'] = x.wdir * (1 if direction == 'fol' else -1)
    x['y'] = metric(x, hor, direction)
    x = x[x.y.notna()]
    return dedupe(x, hor)

def battery(x, col='y'):
    v = x[col].values
    if len(v) < 3:
        return {'n': len(v)}
    o = {'n': len(v), 'per_day': len(v) / max(x.day.nunique(), 1), 'mean': v.mean(), 'median': np.median(v), 'win': (v > 0).mean()}
    for c, lab in (('addr', 'w'), ('day', 'd'), ('coin', 'c')):
        lo, hi, p = boot_ci(v, x[c].values, np.mean, n=1000)
        o[f'ci_{lab}'] = (round(lo, 3), round(hi, 3))
    lo, hi, _ = boot_ci(v, x['coin'].values, np.median, n=1000); o['ci_med_c'] = (round(lo, 3), round(hi, 3))
    days = sorted(x.day.unique()); h = len(days) // 2
    o['half1'] = x[x.day.isin(days[:h])][col].mean(); o['half2'] = x[x.day.isin(days[h:])][col].mean()
    xs = np.sort(v)[::-1]; k = max(1, int(round(len(v) * 0.1)))
    o['sum'] = v.sum(); o['sum_wo_top10'] = xs[k:].sum()
    for c in ('addr', 'day', 'coin'):
        pos = x[x[col] > 0].groupby(c)[col].sum()
        o[f'maxsh_{c}'] = pos.max() / pos.sum() if pos.sum() > 0 else np.nan
    o['n_long'] = int((x.side_key > 0).sum()); o['mean_long'] = x[x.side_key > 0][col].mean()
    o['n_short'] = int((x.side_key < 0).sum()); o['mean_short'] = x[x.side_key < 0][col].mean()
    o['wallets'] = x.addr.nunique(); o['coins'] = x.coin.nunique()
    return o

def null_p(d, state, hor, direction, obs_mean, nperm=500, seed=1):
    """shuffle the state label within coin x day among events whose state is known; recompute the cell mean."""
    rng = np.random.default_rng(seed)
    m, known = STATES[state](d)
    m = np.asarray(m); known = np.asarray(known)
    dd = d[known].reset_index(drop=True); mm = m[known]
    grp = dd.cd.values
    order = np.argsort(grp, kind='stable'); gs = grp[order]
    bounds = np.flatnonzero(np.r_[True, gs[1:] != gs[:-1], True])
    res = []
    for _ in range(nperm):
        perm = mm[order].copy()
        for a, b in zip(bounds[:-1], bounds[1:]):
            if b - a > 1:
                perm[a:b] = perm[a:b][rng.permutation(b - a)]
        newm = np.empty_like(mm); newm[order] = perm
        x = cell_frame(dd, state, hor, direction, mask=newm)
        res.append(x.y.mean() if len(x) else np.nan)
    res = np.array(res)
    return float(np.nanmean(res >= obs_mean)), float(np.nanmean(res)), res
