from strat import *
out = open(W + 'q2_whale_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
cols = [f'P{d}' for d in DTS] + [f'T{d}' for d in DTS] + ['H0', 'H1']
for cond_name, cond in [('ALL tracked-whale taker closes', w.index == w.index),
                        ('wallet known_ratio>=2', w.known_ratio >= 2),
                        ('wallet known_ratio>=5', w.known_ratio >= 5),
                        ('coin_known>=5 (any wallet)', w.coin_known >= 5)]:
    P(f'\n==== condition: {cond_name}')
    for part, sel in [('DISC<=21.09', ~w.test), ('TEST 22-29.09', w.test)]:
        d = w[cond & sel]
        nd = d.day.nunique()
        P(f'-- {part}: txs={len(d)} days={nd}')
        rows = []
        for X in XS:
            f = fills(d, X)
            if len(f) == 0:
                continue
            r = dict(X=X, fills=len(f), per_day=len(f) / nd, n_coin=f.coin.nunique(), n_wal=f.addr.nunique(), exc_med=f.exc_pl.median(),
                     cap_med_k=f.cap_usd.median() / 1000)
            for c in cols:
                r[c + '_md'] = f[c].median(); r[c + '_mn'] = f[c].mean()
            rows.append(r)
        t = pd.DataFrame(rows).set_index('X')
        P(t[['fills', 'per_day', 'n_coin', 'n_wal', 'exc_med', 'cap_med_k']].round(2).to_string())
        P(t[[c + s for c in cols for s in ('_md', '_mn')]].round(3).to_string())
out.close()
