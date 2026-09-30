from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
X = pd.read_parquet(OUT + 's_a2_exit.parquet')
Ah = A.set_index(['coin', 'ts'])
# capacity: whale volume beyond our level (linear walk pf->pl), and a concave version (volume share ~ sqrt of price share)
caps = []; exitvol = []
for r in X.itertuples():
    g = BYC[r.coin]; row = g[(g.ts == r.ts) & (g.addr == r.whale)].iloc[0]
    F = row['b-1'] * (1 + row.bas)
    e_pl = 100 * r.wd * (row.pl / F - 1); e_pf = 100 * r.wd * (row.pf / F - 1)
    fr = np.clip((e_pl - 1.0) / max(e_pl - e_pf, 1e-9), 0, 1)
    caps.append((row.tx_usd * fr, row.tx_usd * fr ** 2))
    # tracked opposite-side volume crossing our exit ask (fair-0.1%) within (ts+1s, ts+60s]
    tsv = g.ts.values; lo = np.searchsorted(tsv, r.ts + 1000, side='right'); hi = np.searchsorted(tsv, r.ts + 60000, side='right')
    seg = g.iloc[lo:hi]; seg = seg[seg.wd == -r.wd]
    Aask = seg['b-1'] * (1 + seg.bas) * (1 + r.wd * 0.1 / 100)
    exitvol.append(seg.tx_usd[((-r.wd) * (seg.pl - Aask) > 0).values].sum())
X['cap_lin'] = [a for a, b in caps]; X['cap_cvx'] = [b for a, b in caps]; X['exit_vol'] = exitvol
print('capacity beyond L (whale volume at prices beyond our level): linear p25/p50/p75', X.cap_lin.quantile([.25, .5, .75]).round(0).tolist(),
      '| if volume concentrated near the top of the sweep (fr^2):', X.cap_cvx.quantile([.25, .5, .75]).round(0).tolist())
print('tracked opposite volume through our exit ask within 60 s: share of fills with >= $1k', (X.exit_vol >= 1000).mean().round(2), '>= $5k', (X.exit_vol >= 5000).mean().round(2),
      '>= $20k', (X.exit_vol >= 20000).mean().round(2))
# per-day P&L at size S with realistic exit (maker at fair-0.1 lat 1s, fallback BN-0.25) and with HL-close exit
for S in (5000, 20000):
    for col in ('R1000_0.1_px_fb_bn', 'fb_hl', 'P60'):
        sz = np.minimum(S, X.cap_lin)
        X['usd'] = sz * X[col] / 100
        for part in (False, True):
            g = X[X.test == part]; nd = 10 if not part else 8
            daily = g.groupby('day').usd.sum().reindex([d for d in sorted(A[A.test == part].day.unique())], fill_value=0)
            print(f'S=${S} {col:20s} {"TEST" if part else "DISC"}: $/day mean {daily.mean():7.1f}  median {daily.median():7.1f}  worst day {daily.min():8.1f}  days>0 {(daily>0).sum()}/{len(daily)}  days=0 {(daily==0).sum()}')
