import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet').merge(pd.read_parquet(W + 'kind.parquet'), on='twap_id')
d = d[d.per != 'none'].copy()
d['frac'] = d.lag_s / d.dur_s
d['mvK'] = 100 * d.tdir * (d.p_Kdec / d.p_Kstart - 1)          # lens: Binance k1m floor(start)->ceil(date)
d['mv1s'] = 100 * d.tdir * (d.p_date / d.p_pre - 1)            # 1 s: 1 s before start -> date second
d['mvHL'] = d.move_pct                                           # bot's own HL move p0->p1 (known at decision)
E = d[(d.eligible == 1) & (d.frac > 0.5) & (d.frac < 1.1)].copy()
print('eligible near-end pool', len(E), E.per.value_counts().to_dict(), 'results', E.result.value_counts().to_dict())
print('corr mvK vs mvHL', E[['mvK', 'mv1s', 'mvHL']].corr().round(2).to_string())
def dedup(x, te, tx):
    x = x.assign(_te=te, _tx=tx).sort_values('_te'); keep = []; busy = {}
    for i, c, a, b in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > a: continue
        keep.append(i); busy[c] = b
    return x.loc[keep]
def fadeK(x):
    x = x[np.isfinite(x.p_Kdec) & np.isfinite(x.p_Kdec_60)]
    x = dedup(x, x.date_s, x.date_s + 3600)
    our = -x.tdir; x['our'] = np.where(our > 0, 'LONG', 'SHORT')
    x['k_net'] = 100 * our * (x.p_Kdec_60 / x.p_Kdec - 1) - COST
    en = np.where(our > 0, x.wb_dec, x.ws_dec); ex = np.where(our > 0, x.ws_dec_60, x.wb_dec_60)
    x['t_net'] = 100 * our * (ex / en - 1) - COST
    x['alt'] = 100 * our * (x.a_dec_60 / x.a_dec - 1)
    x['t_net_xalt'] = x.t_net - x.alt
    return x
for per in ('disc', 'test'):
    s = E[E.per == per]
    print(f'######## {per}')
    for lab, f in (('E lens: mvK>=1 & int>=0.5', (s.mvK >= 1) & (s.intensity >= 0.5)), ('E0 mvK>=1', s.mvK >= 1),
                   ('E 1s: mv1s>=1 & int>=0.5', (s.mv1s >= 1) & (s.intensity >= 0.5)),
                   ('E HL: mvHL>=1 & int>=0.5', (s.mvHL >= 1) & (s.intensity >= 0.5)), ('E HL0: mvHL>=1', s.mvHL >= 1)):
        x = fadeK(s[f])
        print(bat(x, 'k_net', f'{lab:28s} k1m'))
        print(bat(x, 't_net', f'{lab:28s} tape@date+2s'))
        print(f'      xalt tape={x.t_net_xalt.mean():+.3f}  LONG {(x.our=="LONG").sum()}/{x[x.our=="LONG"].t_net.mean():+.3f} SHORT {(x.our=="SHORT").sum()}/{x[x.our=="SHORT"].t_net.mean():+.3f}  wallets={x.addr.nunique()} coins={x.coin.nunique()}')
print('\n### threshold sensitivity (tape@date+2s net mean / n), mv1s and intensity grid')
rows = []
for per in ('disc', 'test'):
    s = E[E.per == per]
    for mv in (0.5, 0.75, 1.0, 1.5):
        for it in (0, 0.2, 0.35, 0.5, 0.75, 1.0, 2.0):
            x = fadeK(s[(s.mvK >= mv) & (s.intensity >= it)])
            rows.append(dict(per=per, mv=mv, it=it, n=len(x), mean=x.t_net.mean(), med=x.t_net.median()))
g = pd.DataFrame(rows)
print(g.pivot_table(index=['mv', 'it'], columns='per', values=['n', 'mean', 'med']).round(3).to_string())
