import sys, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
from hl import boot_ci, honesty, summary, SP
OUT = SP + 'work/impulse-latency/'
DISC_END = '2026-09-21'

def load():
    X = pd.read_parquet(OUT + 'txm.parquet')
    X = X[X.p0.notna()].copy()
    P = pd.read_parquet(OUT + 'placebo.parquet')
    X['qv24m'] = X.qv24 / 1440.0
    X['rel60'] = X.tx_usd / X.qv60b.replace(0, np.nan)
    X['rel24'] = X.tx_usd / X.qv24m
    X['td'] = X.tx_usd / X.depth_usd
    X['per'] = np.where(X.day <= DISC_END, 'D', 'T')
    X = X.sort_values(['addr', 'coin', 'wdir', 'ts']).reset_index(drop=True)
    # episodes: same wallet+coin+direction, chain gap <= 10 s
    for gap, nm in ((10_000, 'ep'), (60_000, 'ep60')):
        new = (X.addr != X.addr.shift()) | (X.coin != X.coin.shift()) | (X.wdir != X.wdir.shift()) | \
              (X.ts - X.ts.shift() > gap)
        X[nm] = new.cumsum()
    g = X.groupby('ep')
    X['ep_usd'] = g.tx_usd.transform('sum'); X['ep_n'] = g.tx_usd.transform('size')
    X['ep_max'] = g.tx_usd.transform('max'); X['ep_dur'] = (g.ts.transform('max') - g.ts.transform('min')) / 1000
    X['ep_first'] = ~X.ep.duplicated()
    X['ep_pct'] = g.tx_pct.transform('sum')
    X['ep_liq'] = g.liq.transform('max')
    g60 = X.groupby('ep60')
    X['ep60_first'] = ~X.ep60.duplicated()
    X['ep60_usd'] = g60.tx_usd.transform('sum')
    # usd known within first 1 s of episode (incl. anchor)
    X['t_ep0'] = g.ts.transform('min')
    X['cum1s'] = X.tx_usd.where(X.ts - X.t_ep0 <= 1000, 0).groupby(X.ep).transform('sum')
    # multi-wallet bursts: same coin + direction, chain gap <= 30 s across wallets (on all >=1k txs)
    Y = X.sort_values(['coin', 'wdir', 'ts'])
    nb = (Y.coin != Y.coin.shift()) | (Y.wdir != Y.wdir.shift()) | (Y.ts - Y.ts.shift() > 30_000)
    X.loc[Y.index, 'burst'] = nb.cumsum().values
    gb = X.groupby('burst')
    X['b_nadr'] = gb.addr.transform('nunique'); X['b_usd'] = gb.tx_usd.transform('sum')
    X['b_t0'] = gb.ts.transform('min')
    # number of distinct wallets that already hit this coin/dir within this burst BEFORE (and incl.) this tx
    X = X.sort_values(['burst', 'ts'])
    X['b_rank_addr'] = (~X.duplicated(['burst', 'addr'])).groupby(X.burst).cumsum()
    X['b_cum_usd'] = X.groupby('burst').tx_usd.cumsum()
    X = X.sort_values(['addr', 'coin', 'wdir', 'ts']).reset_index(drop=True)
    P = P[P.rep == 0].set_index('row')
    return X, P

def clus_ci(v, cl, stat=np.mean, n=1000):
    lo, hi, p = boot_ci(v, cl, stat, n=n)
    return lo, hi

def bucket_table(D, bycol, bins, cols, P=None, labels=None):
    D = D.copy(); D['b'] = pd.cut(D[bycol], bins, labels=labels)
    rows = []
    for b, d in D.groupby('b', observed=True):
        r = {'bucket': str(b), 'n': len(d), 'n_wal': d.addr.nunique()}
        for c in cols:
            r[c] = d[c].mean()
        if P is not None:
            pp = P.loc[P.index.intersection(d.row)]
            for c in cols:
                if c in pp:
                    r['P_' + c] = pp[c].mean()
        rows.append(r)
    return pd.DataFrame(rows)
