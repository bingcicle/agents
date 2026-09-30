# Q2 Strategy A, WHALE-CONDITIONAL part: resting limit order on HL at X% beyond Binance fair (Binance last px of the previous
# second x (1+basis_1h)), on the side a closing whale hits. Filled iff the whale tx's LAST fill px is strictly beyond our level.
# One fill per coin per 60 s (order consumed, re-armed after 60 s). Exits:
#   P_dt  : passive HL exit at Binance fair after dt (optimistic: assumes our ask/bid at fair gets filled), fees 0.015+0.015
#   T_dt  : taker HL exit after dt at Binance fair minus the residual HL discount measured from HL prints (same-side prints = the
#           side we would hit), fees 0.015+0.045
#   H_lat : hedge on Binance with taker, worst px in [t+lat, t+lat+2] s, later unwind both legs at convergence (assumed exact),
#           fees 0.015 (HL maker) + 0.05 + 0.05 (Binance taker in/out) + 0.045 (HL taker unwind) = 0.16
from common import *
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)
def P(*a):
    print(*a)

w = load_tx()
w = w[w.ts < END].copy()
w['ep'] = episodes(w)

# ---------------- prior knowledge of big positions (close_batches + prio_fetch), known >= 60 s before the tx
cb = pd.read_parquet(SP + 'data/close_batches.parquet')
kn = cb[['bot_ts', 'addr', 'coin', 'ratio']].rename(columns={'bot_ts': 't'}); kn['t'] = (kn.t * 1000).astype('int64')
pf = pd.read_csv(SP + 'hl_whale_export_20260930/02_bot_data/prio_fetch.csv')
pf = pf[pf.result.isin(['known', 'added', 'small_ratio'])]
pf['t'] = ((pd.to_datetime(pf.date) - pd.Timedelta(hours=2)).astype('int64') // 10**6)
pf['ratio'] = pf.notional_usd / pf.depth_usd
kn = pd.concat([kn, pf[['t', 'addr', 'trigger_coin', 'ratio']].rename(columns={'trigger_coin': 'coin'})]).dropna()
kn = kn.sort_values('t')
w = w.sort_values('ts')
w['tq'] = w.ts - 60000
m = pd.merge_asof(w[['tq', 'addr', 'coin']].reset_index(), kn.rename(columns={'t': 'tq'}), on='tq', by=['addr', 'coin'],
                  direction='backward', tolerance=48 * 3600 * 1000)
w['known_ratio'] = m.set_index('index').ratio.reindex(w.index).fillna(0)
# coin level: max known ratio of ANY wallet in this coin during the previous 24 h (before ts-60s)
w['coin_known'] = 0.0
for coin, g in w.groupby('coin'):
    k = kn[kn.coin == coin]
    if not len(k):
        continue
    kt = k.t.values; kr = k.ratio.values
    lo = np.searchsorted(kt, g.tq.values - 24 * 3600 * 1000); hi = np.searchsorted(kt, g.tq.values)
    w.loc[g.index, 'coin_known'] = [kr[a:b].max() if b > a else 0 for a, b in zip(lo, hi)]
P('txs', len(w), '| known_ratio>=2 share', (w.known_ratio >= 2).mean().round(3), '| coin_known>=5 share', (w.coin_known >= 5).mean().round(3))

# ---------------- residual HL discount from prints (same side as the whale = where a taker exit would execute)
R = pd.read_parquet(W + 'q1_rev_prints.parquet')
resid_same = R[R.same_side].groupby(['ep', 'bk']).exc.median().groupby('bk').median()
P('residual HL discount at the side we would hit (median of per-event medians, %):', resid_same.round(3).to_dict())
def resid(dt):
    ks = [k for k in resid_same.index if k <= dt]
    return float(resid_same.loc[max(ks)]) if ks else float(resid_same.iloc[0])

XS = [0.3, 0.5, 1.0, 1.5, 2.0, 3.0]
DTS = [5, 30, 60, 300]
def fills(df, X, cool_s=60):
    f = df[df.exc_pl > X].sort_values('ts')
    keep = []; last = {}
    for i, c, t in zip(f.index, f.coin.values, f.ts.values):
        if c in last and t - last[c] < cool_s * 1000:
            continue
        last[c] = t; keep.append(i)
    f = f.loc[keep].copy()
    # exact P&L vs our level L = F*(1 + wdir*X)
    Lr = (1 + f.wdir * X / 100)          # L / F
    for dt in DTS:
        fair = f[f'bn{dt}'] / f['bn-1']    # F_dt / F
        gross = 100 * (-f.wdir) * (fair / Lr - 1)
        f[f'P{dt}'] = gross - 0.03
        f[f'T{dt}'] = gross - resid(dt) - 0.06
    for lat in (0, 1):
        hed = f[f'hedge_w{lat}'] / f['bn-1']
        f[f'H{lat}'] = 100 * (-f.wdir) * (hed / Lr - 1) - 0.16
    # capacity proxy: notional of the whale tx executed beyond our level, linear walk pf->pl
    fr = ((f.exc_pl - X) / (f.exc_pl - f.exc_pf).clip(lower=1e-9)).clip(0, 1)
    f['cap_usd'] = f.tx_usd * fr
    return f

