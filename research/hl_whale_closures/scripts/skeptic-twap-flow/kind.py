import sys; sys.path.insert(0, '.')
from px import *
d = pd.read_parquet(W + 'tw.parquet')
pf = pd.read_csv(BOT + 'prio_fetch.csv'); pf = pf[pf.eol == '^'].rename(columns={'trigger_coin': 'coin'})
pf['t'] = (pd.to_datetime(pf.date) - pd.Timedelta(hours=2)).astype('datetime64[s]').astype('int64')
print('pf date range', pf.date.min(), pf.date.max(), 'results', pf.result.value_counts().to_dict())
w = pd.read_parquet(DATA + 'whale_txs.parquet', columns=['addr', 'coin', 'wside', 'bot_ts', 'ts', 'sp', 'sz'])
w['t'] = w.bot_ts.astype('int64')
w['side'] = np.where((w.sp - w.sz).abs() <= 1e-9 * w.sp.abs().clip(lower=1), 'NONE', w.wside)
def infer(d, pf_mode, use_tx=True, use_pf=True, tcol='seen_s', win=86400):
    parts = []
    if use_pf:
        p = pf[pf.pos_side.isin(['LONG', 'SHORT']) | (pf.result == 'no_pos')].copy()
        if pf_mode == 'inv': p['side'] = np.where(p.result == 'no_pos', 'NONE', p.pos_side.map({'LONG': 'SHORT', 'SHORT': 'LONG'}))
        else: p['side'] = np.where(p.result == 'no_pos', 'NONE', p.pos_side)
        parts.append(p[['addr', 'coin', 't', 'side']].assign(src='pf'))
    if use_tx: parts.append(w[['addr', 'coin', 't', 'side']].assign(src='tx'))
    o = pd.concat(parts).sort_values('t')
    q = d[['twap_id', 'addr', 'coin', tcol, 'twap_side']].rename(columns={tcol: 't'}).sort_values('t')
    q['t'] = q.t.astype('int64')
    m = pd.merge_asof(q, o, on='t', by=['addr', 'coin'], direction='backward', tolerance=win, allow_exact_matches=False)
    k = np.where(m.side.isna(), None, np.where(m.side == 'NONE', 'open',
                 np.where((m.side == 'LONG') == (m.twap_side == 'buy'), 'increase', 'reduce')))
    return pd.Series(k, index=m.twap_id.values), pd.Series(m.src.values, index=m.twap_id.values)
for mode in ('raw', 'inv'):
    for lab, ut, up in (('pf only', False, True), ('tx only', True, False), ('both', True, True)):
        k, src = infer(d, mode, ut, up)
        x = d.set_index('twap_id').assign(ki=k)
        x = x[x.kind.notna() & x.ki.notna()]
        ct = pd.crosstab(x.kind, x.ki)
        acc = (x.kind == x.ki).mean()
        print(f'--- pf={mode} {lab}: n={len(x)} acc={acc:.2f}'); print(ct)
k, src = infer(d, 'inv')
d['kind_inf'] = d.twap_id.map(k); d['kind_inf_src'] = d.twap_id.map(src)
k2, _ = infer(d, 'inv', True, False); d['kind_inf_tx'] = d.twap_id.map(k2)
k3, _ = infer(d, 'raw'); d['kind_inf_raw'] = d.twap_id.map(k3)
k4, _ = infer(d, 'inv', tcol='date_s'); d['kind_inf_date'] = d.twap_id.map(k4)
d['kind2'] = d.kind.fillna(d.kind_inf).fillna('unknown')
print(d.kind2.value_counts(), d.kind_inf.value_counts(dropna=False))
print('inferred increase by source', d[d.kind.isna() & (d.kind_inf == 'increase')].kind_inf_src.value_counts())
d[['twap_id', 'kind_inf', 'kind_inf_src', 'kind_inf_tx', 'kind_inf_raw', 'kind_inf_date', 'kind2']].to_parquet(W + 'kind.parquet')
