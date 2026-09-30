"""Skeptic's own per-TWAP price table from raw twap_signals.csv + Binance 1s bars / k1m."""
import sys, time; sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-twap-flow')
from px import *
t0 = time.time()
d = load_twaps()
print('id-start check (should be 0):', d.id_minus_start.describe()[['min', 'max']].tolist())
d = d[d.sym.notna()].reset_index(drop=True)
d['seen_s'] = np.where(d.eligible == 1, np.minimum(d.date_s, d.start_s + 90), d.date_s)   # lens assumption
ceilm = lambda x: (np.ceil(np.asarray(x, float) / 60) * 60).astype(np.int64)
floorm = lambda x: (np.floor(np.asarray(x, float) / 60) * 60).astype(np.int64)
P = {  # last-price points (1s where available)
    'pre60': d.start_s - 3600, 'pre15': d.start_s - 900, 'pre': d.start_s - 1, 's0': d.start_s, 's3': d.start_s + 3, 's30': d.start_s + 30,
    's60': d.start_s + 60, 's120': d.start_s + 120, 's300': d.start_s + 300, 's900': d.start_s + 900,
    'seen': d.seen_s, 'e1': d.seen_s + 60, 'e1_15': d.seen_s + 60 + 900, 'e1_60': d.seen_s + 60 + 3600, 'e1_240': d.seen_s + 60 + 14400,
    'end': d.end_s, 'end_15': d.end_s + 900, 'end_60': d.end_s + 3600, 'end_120': d.end_s + 7200,
    'date': d.date_s, 'dec': d.date_s + 2, 'dec_60': d.date_s + 2 + 3600,
}
K = {  # k1m minute-boundary points (lens-style)
    'Kstart': floorm(d.start_s), 'Ke1': ceilm(d.seen_s + 60), 'Ke1_15': ceilm(d.seen_s + 60) + 900, 'Ke1_60': ceilm(d.seen_s + 60) + 3600,
    'Kdec': ceilm(d.date_s), 'Kdec_60': ceilm(d.date_s) + 3600, 'Kend': ceilm(d.end_s), 'Kend_60': ceilm(d.end_s) + 3600,
}
WP = ['e1', 'e1_15', 'e1_60', 'e1_240', 'dec', 'dec_60', 'end', 'end_60', 's3', 'seen']  # worst-in-3s points (both sides)
res = {('p_' + k): np.full(len(d), np.nan) for k in list(P) + list(K)}
for k in WP:
    res['wb_' + k] = np.full(len(d), np.nan); res['ws_' + k] = np.full(len(d), np.nan)
res['vol24'] = np.full(len(d), np.nan)
for s, idx in d.groupby('sym').groups.items():
    S = Sym1s(s)
    idx = np.asarray(idx)
    for k, v in P.items():
        vv = np.asarray(v)
        res['p_' + k][idx] = [S.last(vv[i]) for i in idx]
    for k, v in K.items():
        vv = np.asarray(v)
        res['p_' + k][idx] = [S.klast(vv[i]) for i in idx]
    for k in WP:
        vv = np.asarray(P[k])
        res['wb_' + k][idx] = [S.worst(vv[i], +1) for i in idx]
        res['ws_' + k][idx] = [S.worst(vv[i], -1) for i in idx]
    res['vol24'][idx] = [S.vol24(d.start_s.values[i]) for i in idx]
    del S
for k, v in res.items():
    d[k] = v
for k, v in list(P.items()) + list(K.items()):
    a, b = mkt_at(np.asarray(v))
    d['a_' + k] = a; d['b_' + k] = b
d['flow_pm'] = d.usd / d.dur_min
d['intensity'] = d.flow_pm / d.vol24
d.to_parquet(W + 'tw.parquet')
print('done', round(time.time() - t0), d.shape, 'p_pre ok', d.p_pre.notna().sum(), 'p_end ok', d.p_end.notna().sum(), 'wb_e1 ok', d.wb_e1.notna().sum())
