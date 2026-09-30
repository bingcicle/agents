import numpy as np, pandas as pd
OUT = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-hl-dislocation/'
w = pd.read_parquet(OUT + 's_tx.parquet')
w = w[(w.liq == 0) & w.dir.isin(['Close Long', 'Close Short'])].copy()
w['bas'] = w.bas15
w['F'] = w['b-1'] * (1 + w.bas)
w['exc'] = 100 * w.wd * (w.pl / w.F - 1)
w['exc_raw'] = 100 * w.wd * (w.pl / w['b-1'] - 1)
w['td'] = w.tx_usd / w.depth_usd
w['tdb'] = pd.cut(w.td, [0, 0.03, 0.1, 0.3, 1, 1000])
g = w.groupby('tdb', observed=True)
print(pd.DataFrame({'n': g.size(), 'exc_med': g.exc.median(), 'exc_raw_med': g.exc_raw.median(), 'P>1': g.exc.apply(lambda x: (x > 1).mean())}).round(3))
# Binance confirmation share
for k in [0, 10, 60, 300]:
    w[f'mv{k}'] = 100 * w.wd * (w[f'b{k}'] / w['b-1'] - 1)
s = w[(w.exc > 0.3) & (w.td >= 0.01)]
print('n exc>0.3 td>=.01', len(s))
for k in [0, 10, 60, 300]:
    print(k, 'median(mv/exc)=%.2f  meanmv/meanexc=%.2f' % (np.median(s[f'mv{k}'] / s.exc), s[f'mv{k}'].mean() / s.exc.mean()))
# dedup by episode (addr,coin, gap 60s): deepest tx per episode
s = s.sort_values(['addr', 'coin', 'ts'])
ep = ((s.addr != s.addr.shift()) | (s.coin != s.coin.shift()) | (s.ts.diff() > 60000)).cumsum()
s['ep'] = ep
e = s.sort_values('exc', ascending=False).drop_duplicates('ep')
print('episodes', len(e))
for k in [0, 10, 60, 300]:
    print(k, 'EP median(mv/exc)=%.2f  meanmv/meanexc=%.2f' % (np.median(e[f'mv{k}'] / e.exc), e[f'mv{k}'].mean() / e.exc.mean()))
# log-log fit
b2 = w[(w.td >= 0.1) & (w.exc > 0)]
X = np.c_[np.ones(len(b2)), np.log(b2.td)]
beta = np.linalg.lstsq(X, np.log(b2.exc), rcond=None)[0]; print('exc ~ %.2f * td^%.2f  n=%d (NB: conditions on exc>0, drops %d rows)' % (np.exp(beta[0]), beta[1], len(b2), ((w.td >= 0.1) & (w.exc <= 0)).sum()))
