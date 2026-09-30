# Mechanism check: all-day resting fills (Strategy A, X=1 and 1.5) split by whether a big sweep (td>=0.3) in the same coin & direction
# happened 1.5-61.5 s BEFORE the fill (i.e. the fill is a continuation of a burst) or not (first shock).
from strat import *
out = open(W + 'q2_split_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
big = w[w.td >= 0.3]
for X in (1.0, 1.5):
    for part in (False, True):
        f = fills(w[w.test == part], X)
        pre = []
        for r in f.itertuples():
            b = big[(big.coin == r.coin) & (big.wdir == r.wdir) & (big.ts <= r.ts - 1500) & (big.ts >= r.ts - 61500)]
            pre.append(len(b) > 0)
        f['preceded'] = pre
        g = f.groupby('preceded')
        t = pd.DataFrame({'n': g.size(), 'P5': g.P5.mean(), 'P60': g.P60.mean(), 'P60_md': g.P60.median(), 'P300': g.P300.mean(), 'H1': g.H1.mean(),
                          'exc_med': g.exc_pl.median(), 'mv0_med': g.mv0.median(), 'mv60_med': g.mv60.median()})
        P(f'X={X} {"TEST" if part else "DISC"}'); P(t.round(3).to_string())
# daily P&L of A2 base
a2 = pd.read_parquet(W + 'q2_a2_fills.parquet')
d = a2.groupby('day').agg(n=('P60', 'size'), P60_sum=('P60', 'sum'), P60_mean=('P60', 'mean'))
P('\nA2 daily (sum of P60 in % units = $ per $100 per fill):'); P(d.round(3).to_string())
P('days with fills: %d, positive days: %d' % (len(d), (d.P60_sum > 0).sum()))
out.close()
