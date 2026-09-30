# Calibrate the candle 'sure fill' criterion with the exactly-known whale fills (tx level) inside the HL 1m window (27.09-29.09).
from strat import *
out = open(W + 'q2_calib_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
F1 = pd.read_parquet(W + 'q2_uncond_fills_1m.parquet')
t0 = F1.t.min()
for X in (1.0, 1.5, 2.0):
    f = fills(w[(w.ts >= t0) & (w.ts < END)], X)
    f['bar'] = (f.ts // 60000) * 60000
    side = np.where(f.wdir < 0, 1, -1)
    sure = F1[F1.X == X][['coin', 't', 'side']].assign(sure=True)
    m = f.assign(side=side).merge(sure, left_on=['coin', 'bar', 'side'], right_on=['coin', 't', 'side'], how='left')
    m['sure'] = m.sure.fillna(False).astype(bool)
    P(f'X={X}: exact whale fills in 1m window n={len(m)}; classified as candle sure-fill: {m.sure.mean():.2f}; '
      f'P60 mean sure={m[m.sure].P60.mean():+.3f} (n={m.sure.sum()}) vs not-sure={m[~m.sure].P60.mean():+.3f} (n={(~m.sure).sum()}); '
      f'Binance mv60 median sure={m[m.sure].mv60.median():+.3f} not-sure={m[~m.sure].mv60.median():+.3f}')
out.close()
