"""Infer TWAP kind (reduce/increase/open) for TWAPs without slice-based kind, from the whale's last observed position
side in the same coin BEFORE the decision (first-seen) time. Sources: prio_fetch.csv (pos_side at fetch; no_pos), whale_txs
(wside of close txs, known at bot_ts). Window: 24 h before seen."""
import sys; sys.path.insert(0, '.')
from common import *
d = pd.read_parquet('twaps_px2.parquet')
pf = pd.read_csv(BOT + 'prio_fetch.csv'); pf = pf[pf.eol == '^']
pf['t'] = (pd.to_datetime(pf.date) - pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
pf = pf.rename(columns={'trigger_coin': 'coin'})
pf['side'] = np.where(pf.result == 'no_pos', 'NONE', pf.pos_side.map({'LONG': 'SHORT', 'SHORT': 'LONG'}))  # pos_side in prio_fetch = side of the whale's CLOSING trade (verified vs slice startPosition)
pf = pf[pf.side.isin(['LONG', 'SHORT', 'NONE'])][['addr', 'coin', 't', 'side']].assign(src='pf')
w = pd.read_parquet(DATA + 'whale_txs.parquet', columns=['addr', 'coin', 'wside', 'bot_ts', 'sp', 'sz'])
w['t'] = (w.bot_ts * 1000).astype('int64'); w['side'] = w.wside
# after a tx that closes the whole position the side is NONE
w.loc[(w.sp - w.sz).abs() < 1e-9 * w.sp.abs().clip(lower=1), 'side'] = 'NONE'
obs = pd.concat([pf, w[['addr', 'coin', 't', 'side']].assign(src='tx')]).sort_values('t')
print('observations', len(obs), obs.src.value_counts().to_dict())
g = {k: v for k, v in obs.groupby(['addr', 'coin'])}
res = []
for r in d.itertuples():
    o = g.get((r.addr, r.coin))
    ks, src, age = None, None, None
    if o is not None:
        oo = o[(o.t <= r.seen_ms) & (o.t >= r.seen_ms - 24 * 3600 * 1000)]
        if len(oo):
            last = oo.iloc[-1]; age = (r.seen_ms - last.t) / 60000; src = last.src
            if last.side == 'NONE':
                ks = 'open'
            elif (last.side == 'LONG') == (r.twap_side == 'buy'):
                ks = 'increase'
            else:
                ks = 'reduce'
    res.append((ks, src, age))
d['kind_inf'], d['kind_inf_src'], d['kind_inf_age_min'] = zip(*res)
print('inferred coverage', d.kind_inf.notna().mean(), d.kind_inf.value_counts(dropna=False).to_dict())
print('validation vs slice-based kind (where both):')
print(pd.crosstab(d.kind.fillna('na'), d.kind_inf.fillna('none')))
d['kind2'] = d.kind.fillna(d.kind_inf).fillna('unknown')
print(d.kind2.value_counts())
d[['twap_id', 'kind_inf', 'kind_inf_src', 'kind_inf_age_min', 'kind2']].to_parquet('kind_inf.parquet')
