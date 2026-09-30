# Strategy A2 "event-armed deep liquidity": only AFTER a tracked whale's big sweep is detected (t_det = ts + 1.5 s, or the bot's
# real bot_ts), rest an order X% beyond Binance fair on HL for W seconds in that coin, on the side the whale hits.
# Filled by a LATER tracked-whale tx (any wallet, same direction) whose last fill is beyond X. One fill per trigger episode.
from strat import *
out = open(W + 'q2_a2_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)

w = w.sort_values('ts').reset_index(drop=True)
def run(trig_mask, X, Wsec, det='ws15'):
    trig = w[trig_mask].copy()
    trig['tdet'] = trig.ts + 1500 if det == 'ws15' else np.maximum(trig.ts + 1500, (trig.bot_ts * 1000).astype('int64'))
    rows = []; busy_until = {}
    by_coin = {c: g for c, g in w.groupby('coin')}
    for _, t in trig.iterrows():
        key = (t.coin, t.wdir)
        if busy_until.get(key, 0) > t.tdet:          # already armed for this coin/direction
            continue
        g = by_coin[t.coin]
        tsv = g.ts.values
        lo = np.searchsorted(tsv, t.tdet, side='right'); hi = np.searchsorted(tsv, t.tdet + Wsec * 1000, side='right')
        c = g.iloc[lo:hi]
        c = c[(c.wdir == t.wdir) & (c.exc_pl > X)]
        busy_until[key] = t.tdet + Wsec * 1000
        if len(c) == 0:
            rows.append(dict(trig_ts=t.ts, coin=t.coin, filled=False))
            continue
        f = c.iloc[0]
        busy_until[key] = f.ts + 60000
        Lr = 1 + f.wdir * X / 100
        r = dict(trig_ts=t.ts, coin=t.coin, filled=True, fill_ts=f.ts, wait_s=(f.ts - t.tdet) / 1000, same_wallet=f.addr == t.addr,
                 whale=f.addr, day=f.day, test=f.test, exc_pl=f.exc_pl, td=f.td, wdir=f.wdir, cap=f.tx_usd)
        for dt in (5, 60, 300):
            r[f'P{dt}'] = 100 * (-f.wdir) * ((f[f'bn{dt}'] / f['bn-1']) / Lr - 1) - 0.03
        r['H1'] = 100 * (-f.wdir) * ((f['hedge_w1'] / f['bn-1']) / Lr - 1) - 0.16
        rows.append(r)
    return pd.DataFrame(rows)

res = []
for tname, tm in [('td>=0.3', w.td >= 0.3), ('exc_pl>=1', w.exc_pl >= 1.0), ('td>=1', w.td >= 1.0)]:
    for X in (0.5, 1.0, 1.5, 2.0):
        for Wsec in (60, 300):
            R = run(tm, X, Wsec)
            for part in (False, True):
                trig_n = int(((R.trig_ts >= TEST0) == part).sum())
                f = R[R.filled & (R.test == part)] if 'test' in R else R.iloc[:0]
                nd = w[w.test == part].day.nunique()
                r = dict(trigger=tname, X=X, W=Wsec, part='TEST' if part else 'DISC', triggers=trig_n, fills=len(f), per_day=len(f) / nd,
                         fill_rate=len(f) / max(trig_n, 1), same_wallet=f.same_wallet.mean() if len(f) else np.nan)
                for ex in ('P5', 'P60', 'P300', 'H1'):
                    r[ex + '_md'] = f[ex].median() if len(f) else np.nan; r[ex + '_mn'] = f[ex].mean() if len(f) else np.nan
                if len(f) >= 5:
                    h = honesty(f, 'P60', wallet='whale', day='day', coin='coin')
                    r['P60_ci_wallet'] = h.get('mean_ci90_whale'); r['P60_ci_day'] = h.get('mean_ci90_day'); r['P60_ci_coin'] = h.get('mean_ci90_coin')
                    r['maxsh_coin'] = h.get('max_share_coin'); r['sum_wo_top10'] = h.get('sum_wo_top10')
                res.append(r)
            print(tname, X, Wsec, flush=True)
S = pd.DataFrame(res)
S.to_csv(W + 'q2_a2_table.csv', index=False)
P(S.round(3).to_string())
piv = S.pivot_table(index=['trigger', 'X', 'W'], columns='part', values='P60_mn')
P('\nK=%d variants; DISC P60 mean>0: %d; of those TEST>0: %d' % (len(piv), (piv.DISC > 0).sum(), ((piv.DISC > 0) & (piv.TEST > 0)).sum()))
out.close()
