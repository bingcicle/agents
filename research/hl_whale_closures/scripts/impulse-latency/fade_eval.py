"""Focused evaluation of the FADE-after-squeeze family found in grid_fade (reversal after a big whale close that came
after a strong pre-move and continued after the close). Selection strictly on DISCOVERY, TEST once.
Rule template (all info known at decision time t0+300 s):
  anchor = first >=$1k tx of a 10 s same-wallet/coin/dir chain, SIZE filter F, pre60 = move in whale dir over the 60 s
  before the tx >= a, m120 = move in whale dir from tx to +120 s >= b  ->  enter AGAINST whale at t0+300 s (worst-in-3s),
  exit at t0+H (worst-in-3s). One open position per coin (later signals in the same coin while open are skipped).
"""
from common import *
import itertools
X, P = load()
E = X[X.ep_first].copy()
E['pre60'] = -E['m-60']
COST = 0.146
SIZE = {}
for t in (0.1, 0.2, 0.3, 0.5): SIZE[f'td>={t}'] = E.td >= t
for t in (5, 10, 20): SIZE[f'pct>={t}'] = E.tx_pct >= t
for t in (3e4, 1e5): SIZE[f'usd>={int(t/1e3)}k'] = E.tx_usd >= t
for t in (0.5, 1, 2): SIZE[f'rel24>={t}'] = E.rel24 >= t
SIZE['none'] = E.tx_usd > 0

def trades(mask, K, H, dedupe=True):
    d = E[mask].copy()
    d['g'] = d[f'fw{K}'] - d[f'xr{H}']
    d['g_alt'] = d.g + d[f'alt{H}']
    d['g_btc'] = d.g + d[f'btc{H}']
    d = d[d.g.notna()].sort_values('ts')
    if dedupe:
        keep, busy = [], {}
        for r in d.itertuples():
            if busy.get(r.coin, 0) > r.ts:
                continue
            keep.append(r.Index); busy[r.coin] = r.ts + H * 1000
        d = d.loc[keep]
    return d

def fam(K=300, Hs=(1800, 3600)):
    rows = []
    for (sn, sm), a, b, H in itertools.product(SIZE.items(), (0.2, 0.5, 1.0), (None, 0.3, 0.6), Hs):
        m = sm & (E.pre60 >= a)
        if b is not None: m &= E.m120 >= b
        d = trades(m, K, H)
        for per in ('D', 'T'):
            x = d[d.per == per]
            rows.append(dict(size=sn, pre=a, imp=b, H=H, per=per, n=len(x), nw=x.addr.nunique(), mean=x.g.mean(), med=x.g.median(),
                             net=x.g.mean() - COST, alt=x.g_alt.mean() - COST))
    R = pd.DataFrame(rows)
    W = R.pivot_table(index=['size', 'pre', 'imp', 'H'], columns='per', values=['n', 'nw', 'mean', 'med', 'net', 'alt'], dropna=False).reset_index()
    W.columns = [a if not b else f'{a}_{b}' for a, b in W.columns]
    return W

def battery(d, label, col='g'):
    x = d.copy(); x['net'] = x[col] - COST
    h = honesty(x, 'net', wallet='addr', day='day', coin='coin')
    out = [f'--- {label}: n={h.get("n")} days={x.day.nunique()} per_day={h.get("n",0)/max(1,x.day.nunique()):.1f} wallets={x.addr.nunique()} coins={x.coin.nunique()}']
    if h.get('n', 0) == 0:
        return '\n'.join(out)
    out.append(f'   net mean={h["mean"]:+.3f} median={h["median"]:+.3f} win={h["win"]:.2f} | gross mean={x[col].mean():+.3f}')
    for c in ('addr', 'day', 'coin'):
        out.append(f'   CI90 mean by {c}: {h[f"mean_ci90_{c}"]}  P(mean<=0)={h[f"P(mean<=0)_{c}"]}  median CI90 {h[f"med_ci90_{c}"]}  max_share_pos={h[f"max_share_{c}"]:.2f}')
    out.append(f'   halves(by day) mean: {h["half1_mean"]:+.3f} / {h["half2_mean"]:+.3f}; sum={h["sum"]:+.2f} sum_wo_top10={h["sum_wo_top10"]:+.2f}')
    for nm, s in (('whale Close Short -> we SHORT', x.wdir > 0), ('whale Close Long -> we LONG', x.wdir < 0)):
        out.append(f'   {nm}: n={s.sum()} net mean={x.net[s].mean():+.3f} med={x.net[s].median():+.3f}')
    out.append(f'   alt-neutral net mean={(x.g_alt - COST).mean():+.3f} med={(x.g_alt - COST).median():+.3f}; BTC-neutral net mean={(x.g_btc - COST).mean():+.3f}')
    for fee in (0.10, 0.04):
        out.append(f'   fee {fee}: net mean={(x[col] - fee).mean():+.3f}')
    top_c = x.groupby('coin').net.agg(['size', 'sum']).sort_values('sum', ascending=False).head(5)
    out.append('   top coins by net sum: ' + ', '.join(f'{c}:{int(r["size"])}/{r["sum"]:+.2f}' for c, r in top_c.iterrows()))
    return '\n'.join(out)

if __name__ == '__main__':
    pd.set_option('display.width', 250)
    W = fam()
    W.to_csv(OUT + 'fade_family.csv', index=False)
    ok = W[(W.n_D >= 25)]
    print('family variants', len(W), 'eligible(n_D>=25)', len(ok), 'share net_D>0', round((ok.net_D > 0).mean(), 3),
          'share net_T>0', round((ok.net_T > 0).mean(), 3), 'corr', round(ok[['mean_D', 'mean_T']].corr().iloc[0, 1], 3))
    cols = ['size', 'pre', 'imp', 'H', 'n_D', 'nw_D', 'mean_D', 'med_D', 'net_D', 'alt_D', 'n_T', 'nw_T', 'mean_T', 'med_T', 'net_T', 'alt_T']
    print(ok.sort_values('net_D', ascending=False)[cols].round(3).head(30).to_string())
    print('--- no size filter rows:')
    print(W[W['size'] == 'none'][cols].round(3).to_string())
