# Q1: HL-vs-Binance dislocation at whale txs, how it scales, how much of it is permanent (Binance follows) vs transient.
from common import *
from scipy.stats import spearmanr
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)
out = open(W + 'q1_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')

w = load_tx()
P('taker close txs with Binance 1s + basis:', len(w), 'coins', w.coin.nunique(), 'wallets', w.addr.nunique(), 'days', w.day.nunique())
w['ep'] = episodes(w)
w['tdb'] = pd.cut(w.td, [0, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 100])

P('\n== 1. Dislocation at the tx (exc_* in %, signed in whale direction, vs Binance last px of previous second x (1+basis_1h))')
g = w.groupby('tdb', observed=True)
t = pd.DataFrame({'n_tx': g.size(), 'n_ep': g.ep.nunique(), 'usd_med': g.tx_usd.median().round(0), 'sweep_med': g.sweep.median(), 'sweep_p90': g.sweep.quantile(.9),
                  'exc_pf_med': g.exc_pf.median(), 'exc_pl_med': g.exc_pl.median(), 'exc_pl_mean': g.exc_pl.mean(), 'exc_pl_p90': g.exc_pl.quantile(.9),
                  'P(>0.3)': g.exc_pl.apply(lambda x: (x > .3).mean()), 'P(>0.5)': g.exc_pl.apply(lambda x: (x > .5).mean()),
                  'P(>1)': g.exc_pl.apply(lambda x: (x > 1).mean()), 'P(>2)': g.exc_pl.apply(lambda x: (x > 2).mean())})
P(t.round(3).to_string())
big = w[w.td >= 0.03]
P('\nSpearman (td>=0.03, n=%d): exc_pl~td %.3f | exc_pl~batch_ratio %.3f | exc_pl~tx_usd %.3f | sweep~td %.3f | exc_pf~td %.3f' % (
    len(big), spearmanr(big.exc_pl, big.td)[0], spearmanr(big.exc_pl, big.batch_ratio)[0], spearmanr(big.exc_pl, big.tx_usd)[0],
    spearmanr(big.sweep, big.td)[0], spearmanr(big.exc_pf, big.td)[0]))
# OLS log-log on td>=0.1
b2 = w[(w.td >= 0.1) & (w.exc_pl > 0)]
X = np.c_[np.ones(len(b2)), np.log(b2.td), np.log(b2.batch_ratio.clip(0.1))]
beta = np.linalg.lstsq(X, np.log(b2.exc_pl), rcond=None)[0]
P('log(exc_pl) = %.3f + %.3f log(td) + %.3f log(ratio)   (td>=0.1 & exc>0, n=%d)  -> exc ~ %.2f%% * td^%.2f' % (beta[0], beta[1], beta[2], len(b2), np.exp(beta[0]), beta[1]))
w['rb'] = pd.cut(w.batch_ratio, [0, 2, 5, 10, 20, 1000])
g = big.assign(rb=pd.cut(big.batch_ratio, [0, 2, 5, 10, 20, 1000])).groupby(['tdb', 'rb'], observed=True).exc_pl
P('\nexc_pl median by td bin x batch_ratio bin (td>=0.03):'); P(g.median().unstack().round(2).to_string()); P(g.size().unstack().to_string())
P('\nBy coin (td>=0.1): n, exc_pl median, P(>0.5), Binance mv60 median')
gc = w[w.td >= 0.1].groupby('coin')
tc = pd.DataFrame({'n': gc.size(), 'n_ep': gc.ep.nunique(), 'exc_pl': gc.exc_pl.median(), 'P05': gc.exc_pl.apply(lambda x: (x > .5).mean()),
                   'mv60': gc.mv60.median(), 'depth_med_k': gc.depth_usd.median() / 1000}).sort_values('n', ascending=False)
P(tc.head(25).round(2).to_string())

P('\n== 2. Permanent vs transient: Binance move in whale dir after the tx (mv_k, %, from Binance px of previous second) by HL dislocation bin')
w['eb'] = pd.cut(w.exc_pl, [-100, 0.1, 0.3, 0.5, 1, 2, 100])
ev = w[w.td >= 0.01]
g = ev.groupby('eb', observed=True)
cols = ['mv-60', 'mv-5', 'mv0', 'mv1', 'mv2', 'mv5', 'mv10', 'mv30', 'mv60', 'mv300', 'mv1800', 'hmv0', 'hmv1']
t = g[cols].median(); t.insert(0, 'exc_med', g.exc_pl.median()); t.insert(0, 'n', g.size()); t.insert(1, 'n_ep', g.ep.nunique())
P('medians:'); P(t.round(3).to_string())
t = g[cols].mean(); t.insert(0, 'exc_mean', g.exc_pl.mean())
P('means:'); P(t.round(3).to_string())
P('share of HL dislocation that Binance "confirms" (median mv/exc):')
for k in ['mv0', 'mv10', 'mv60', 'mv300', 'mv1800']:
    s = ev[ev.exc_pl > 0.3]
    P(f'  {k}: median(mv/exc) = {np.median(s[k] / s.exc_pl):.2f}  mean ratio of means = {s[k].mean() / s.exc_pl.mean():.2f}  (n={s[k].notna().sum()})')

P('\n== 3. HL reversion from later HL prints (any tracked wallet, same coin), excess vs Binance at their own second, whale-signed')
E = w[(w.exc_pl > 0.3) & (w.td >= 0.03)].copy()
E = E.sort_values('exc_pl', ascending=False).drop_duplicates('ep').sort_values('ts')   # one (deepest) tx per episode
P('events (deepest tx per episode, exc_pl>0.3 & td>=0.03):', len(E))
allp = load_tx(all_rows=True)
allp = allp[allp['bn-1'].notna()]
BK = [0, 1, 2, 5, 10, 30, 60, 120, 300, 600]
rec = []
for coin, ge in E.groupby('coin'):
    pc = allp[allp.coin == coin].sort_values('ts')
    pt = pc.ts.values; ppx = pc.px.values; pbn = pc['bn-1'].values; pside = pc.wdir.values; paddr = pc.addr.values; ph = pc.h.values
    for _, e in ge.iterrows():
        lo = np.searchsorted(pt, e.ts, side='right'); hi = np.searchsorted(pt, e.ts + 600000, side='right')
        if hi <= lo:
            continue
        dts = (pt[lo:hi] - e.ts) / 1000
        exc = 100 * e.wdir * (ppx[lo:hi] / (pbn[lo:hi] * (1 + e.basis)) - 1)
        same_side = pside[lo:hi] == e.wdir
        same_w = paddr[lo:hi] == e.addr
        b = np.digitize(dts, BK[1:], right=True)
        for i in range(len(dts)):
            if ph[lo + i] == e.h:
                continue
            rec.append((e.ep, coin, e.addr, e.day, e.exc_pl, BK[b[i]] if b[i] < len(BK) else 600, exc[i], same_side[i], same_w[i]))
R = pd.DataFrame(rec, columns=['ep', 'coin', 'addr', 'day', 'exc0', 'bk', 'exc', 'same_side', 'same_w'])
R.to_parquet(W + 'q1_rev_prints.parquet')
# per event per bucket medians by side, then across events
pe = R.groupby(['ep', 'bk', 'same_side']).exc.median().unstack()
pe.columns = ['opp', 'same']
t = pe.groupby('bk').agg(['median', 'count'])
P('bucket = start of dt window (s). "same" = prints in the whale direction (e.g. sells hitting the bid after a whale sell), "opp" = opposite side')
P(t.round(3).to_string())
both = pe.dropna()
P('mid proxy (events with both sides in bucket): median of (same+opp)/2:')
P(((both.same + both.opp) / 2).groupby('bk').agg(['median', 'mean', 'count']).round(3).to_string())
ow = R[~R.same_w].groupby(['ep', 'bk']).exc.median().groupby('bk').agg(['median', 'count'])
P('other wallets only (any side), median of per-event medians:'); P(ow.round(3).to_string())

out.close()
