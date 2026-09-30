import sys; sys.path.insert(0, '.')
from px import *
from scipy.stats import spearmanr
st = pd.read_csv(BOT + 'settlements.csv', low_memory=False); st = st[(st.eol == '^') & (st.family == 'twap')].copy()
print('rows', len(st), 'unique keys', st.key.nunique())
# are duplicates identical?
g = st.groupby('key').net_official_pct.agg(['size', 'nunique'])
print('dup keys with differing net', (g['nunique'] > 1).sum())
st = st.drop_duplicates('key', keep='last')
st['twap_id'] = st.key.str.split('|').str[0]
d = pd.read_parquet(W + 'tw.parquet')
x = st.merge(d[['twap_id', 'intensity', 'coin', 'addr', 'tdir', 'move_pct']], on='twap_id', how='left', suffixes=('', '_t'))
x['t'] = pd.to_datetime(x.entry_ts_ms, unit='ms')
x['win'] = np.where(x.t < '2026-09-17 08:30', 'eval', 'after')
for strat in ('T2_твап_скорочення', 'T1_твап_відкриття'):
    y = x[x.strategy == strat]
    print('\n==', strat, len(y))
    print(y.groupby('win').agg(n=('net_official_pct', 'size'), mean=('net_official_pct', 'mean'), med=('net_official_pct', 'median'), int_med=('intensity', 'median')).round(3))
    ok = y.intensity.notna()
    print('spearman all', spearmanr(y[ok].net_official_pct, y[ok].intensity).statistic.round(3), 'n', ok.sum())
    for w_ in ('eval', 'after'):
        yy = y[ok & (y.win == w_)]; print('  spearman', w_, spearmanr(yy.net_official_pct, yy.intensity).statistic.round(3), len(yy))
    print(y.assign(ib=pd.cut(y.intensity, [0, 0.5, 1e9])).groupby(['ib', 'win']).net_official_pct.agg(['size', 'mean', 'median']).round(3))
    y2 = y[y.t >= '2026-09-22']
    print('  TEST-window (>=22.09) n', len(y2), 'mean', round(y2.net_official_pct.mean(), 3), 'median', round(y2.net_official_pct.median(), 3))
