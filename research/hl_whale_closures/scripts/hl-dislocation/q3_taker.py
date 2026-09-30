# Q3 Strategy B "taker arb at bot latency": detect the whale sweep at ts+1.5 s, buy HL at the ask / sell Binance (after a whale
# sell), unwind on convergence. Without HL book history the only observable HL price after +1.5 s is later HL prints of tracked
# wallets. Opposite-side prints (e.g. BUYS after a whale sell) = the price a taker-arb actually paid at that moment.
from common import *
out = open(W + 'q3_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
w = load_tx(); w = w[w.ts < END]
w['ep'] = episodes(w)
allp = load_tx(all_rows=True); allp = allp[allp['bn-1'].notna()]
FEES = 0.19   # HL taker 0.045 + Binance taker 0.05 in, same out
rows = []
for thr in (0.5, 1.0, 1.5, 2.0, 3.0):
    E = w[w.exc_pl >= thr].sort_values('ts').drop_duplicates('ep')      # first tx of the episode that crosses thr
    for coin, ge in E.groupby('coin'):
        pc = allp[allp.coin == coin].sort_values('ts')
        pt = pc.ts.values
        for e in ge.itertuples():
            lo = np.searchsorted(pt, e.ts + 1500); hi = np.searchsorted(pt, e.ts + 5000, side='right')
            seg = pc.iloc[lo:hi]
            opp = seg[seg.wdir == -e.wdir]
            r = dict(thr=thr, ep=e.ep, coin=coin, day=e.day, test=e.test, whale=e.addr, exc0=e.exc_pl, has_opp=len(opp) > 0,
                     n_same=int((seg.wdir == e.wdir).sum()), mv1=e.mv1, mv5=e.mv5)
            if len(opp):
                o = opp.iloc[0]
                gap = 100 * e.wdir * (o.px / (o['bn-1'] * (1 + e.basis)) - 1)     # >0: HL still dislocated at the ask
                r.update(gap_opp=gap, t_opp=(o.ts - e.ts) / 1000, arb_net=gap - FEES)
            rows.append(r)
R = pd.DataFrame(rows)
R.to_parquet(W + 'q3_events.parquet')
g = R.groupby(['thr', 'test'])
t = pd.DataFrame({'episodes': g.size(), 'share_with_opp_print_1.5-5s': g.has_opp.mean(), 'gap_opp_med': g.gap_opp.median(),
                  'gap_opp_mean': g.gap_opp.mean(), 'arb_net_med': g.arb_net.median(), 'arb_net_mean': g.arb_net.mean(),
                  'share_arb_net>0': g.arb_net.apply(lambda x: (x.dropna() > 0).mean()), 'bn_mv1_med': g.mv1.median()})
P('Q3: HL price available to a taker 1.5-5 s after the sweep (first opposite-side print of any tracked wallet), whale-signed excess vs Binance fair, %')
P(t.round(3).to_string())
for thr in (1.0, 2.0):
    d = R[(R.thr == thr) & R.has_opp]
    if len(d) > 5:
        P(f'thr {thr}: arb_net ' + fmt_h(honesty(d, 'arb_net', wallet='whale', day='day', coin='coin')))
out.close()
