# Size/tail view of candle 'sure fills': HL bar notional (upper bound for what could have traded at our level) and loss tails.
from common import *
out = open(W + 'q2_uncond_size_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
for lab, k in (('1m', 5), ('5m', 3)):
    F = pd.read_parquet(W + f'q2_uncond_fills_{lab}.parquet')
    F['bar_usd'] = F.v * F.c
    P(f'\n{lab} candles: sure fills by X (both periods), bar notional $ (ALL prices in the bar = upper bound of our fill), tails of pes{k} (%):')
    g = F.groupby(['X', 'whale'])
    t = pd.DataFrame({'n': g.size(), 'bar_usd_p25': g.bar_usd.quantile(.25), 'bar_usd_med': g.bar_usd.median(), 'bar_usd_p75': g.bar_usd.quantile(.75),
                      f'pes{k}_mean': g[f'pes{k}'].mean(), f'pes{k}_p5': g[f'pes{k}'].quantile(.05), f'share_pes{k}<-1': g[f'pes{k}'].apply(lambda x: (x < -1).mean()),
                      f'opt{k}_mean': g[f'opt{k}'].mean()})
    P(t.round(3).to_string())
    # usd-weighted by bar notional capped at $5k (crude): what a $5k resting order could at most have earned
    for X in (1.0, 1.5, 2.0):
        d = F[F.X == X]
        cap = d.bar_usd.clip(upper=5000)
        nd = pd.to_datetime(d.t, unit='ms').dt.date.nunique()
        P(f'  X={X}: upper-bound $/day with $5k order (bar notional cap): pes ${(cap*d[f"pes{k}"]/100).sum()/nd:,.0f}  opt ${(cap*d[f"opt{k}"]/100).sum()/nd:,.0f}  over {nd} days')
out.close()
