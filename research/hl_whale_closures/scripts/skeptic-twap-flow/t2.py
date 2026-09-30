import sys; sys.path.insert(0, '.')
from px import *
from scipy.stats import spearmanr
st = pd.read_csv(BOT + 'settlements.csv', low_memory=False); st = st[st.eol == '^']
st = st[st.family == 'twap'].copy()
st['twap_id'] = st.key.str.split('|').str[0]
print(st.strategy.value_counts())
d = pd.read_parquet(W + 'tw.parquet')
x = st.merge(d[['twap_id', 'intensity', 'usd', 'dur_s', 'coin', 'addr', 'tdir', 'move_pct', 'p_Kdec', 'p_Kdec_60', 'dday']], on='twap_id', how='left', suffixes=('', '_t'))
x['ent_day'] = pd.to_datetime(x.entry_ts_ms, unit='ms').dt.strftime('%Y-%m-%d %H:%M')
for strat in ('T2_твап_скорочення', 'T1_твап_відкриття'):
    y = x[x.strategy == strat].copy()
    y['win'] = np.where(y.ent_day < '2026-09-17 10:30', 'eval<=17.09', 'after')
    print('\n==', strat, 'n', len(y), 'with intensity', y.intensity.notna().sum())
    print(y.groupby('win').agg(n=('net_official_pct', 'size'), mean=('net_official_pct', 'mean'), med=('net_official_pct', 'median'), int_med=('intensity', 'median')).round(3))
    ok = y.intensity.notna() & y.net_official_pct.notna()
    print('spearman(net_official, intensity) all', spearmanr(y[ok].net_official_pct, y[ok].intensity))
    y['ib'] = pd.cut(y.intensity, [0, 0.1, 0.5, 2, 1e9])
    print(y.groupby(['ib', 'win']).net_official_pct.agg(['size', 'mean', 'median']).round(3))
    # our k1m reconstruction of fade 60 from ceil(date)
    rec = 100 * (-y.tdir) * (y.p_Kdec_60 / y.p_Kdec - 1) - 0.146
    print('corr(reconstruction, net_official)', np.corrcoef(rec[rec.notna() & y.net_official_pct.notna()], y.net_official_pct[rec.notna() & y.net_official_pct.notna()])[0, 1].round(3))
