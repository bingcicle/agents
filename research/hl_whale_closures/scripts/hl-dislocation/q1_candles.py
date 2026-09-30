# Q1(b): minute-scale reversion of the HL-vs-Binance gap after big whale dislocations, from HL candles (1m: 27.09-29.09,
# 5m: 13.09-29.09) vs Binance 1m klines at the same bar ends. gap_k = whale-signed (HL close / (Binance close x (1+basis)) - 1).
from common import *
import json, os
out = open(W + 'q1_candles_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
w = load_tx(); w = w[w.ts < END]
w['ep'] = episodes(w)
E = w[(w.exc_pl > 0.5) & (w.td >= 0.03)].sort_values('exc_pl', ascending=False).drop_duplicates('ep')
P('events (deepest tx per episode, exc_pl>0.5):', len(E))
for iv, mins, K in (('1m', 1, [-1, 0, 1, 2, 3, 5, 10, 15, 30]), ('5m', 5, [-1, 0, 1, 2, 3, 6, 12])):
    rows = []
    for coin, ge in E.groupby('coin'):
        fn = f'{SP}data/hl/{coin}_{iv}.json'
        if not os.path.exists(fn) or not SYM.get(coin):
            continue
        d = json.load(open(fn))
        if not d:
            continue
        h = pd.DataFrame(d); h['c'] = h.c.astype(float); h['v'] = h.v.astype(float)
        h = h[h.v > 0].set_index('t')
        k = kl(SYM[coin])
        if k is None:
            continue
        for e in ge.itertuples():
            bar = (e.ts // (mins * 60000)) * (mins * 60000)
            r = dict(coin=coin, test=e.test, exc=e.exc_pl, whale=e.addr, day=e.day)
            for kk in K:
                t = bar + kk * mins * 60000
                if t not in h.index:
                    continue
                bc = k.c.get(t + (mins - 1) * 60000, np.nan)      # Binance close at the same bar end
                r[f'g{kk}'] = 100 * e.wdir * (h.c.loc[t] / (bc * (1 + e.basis)) - 1)
            rows.append(r)
    G = pd.DataFrame(rows)
    cols = [f'g{kk}' for kk in K if f'g{kk}' in G]
    P(f'\n{iv}: events with candles {len(G)}; median / mean whale-signed HL-vs-Binance gap at bar ends (bar 0 = bar containing the tx), %')
    P(pd.DataFrame({'median': G[cols].median(), 'mean': G[cols].mean(), 'n': G[cols].count()}).T.round(3).to_string())
    G['eb'] = pd.cut(G.exc, [0.5, 1, 2, 100])
    P('by dislocation size (median):'); P(G.groupby('eb', observed=True)[cols].median().round(3).to_string())
out.close()
