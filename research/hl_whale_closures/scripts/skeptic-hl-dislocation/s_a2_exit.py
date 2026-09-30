# Maker-exit realism for A2 fills: exit ask at F_t*(1 - wd... ) i.e. d% on the whale side of fair, re-pegged; filled if a later
# OPPOSITE-direction print (any tracked wallet, any dir, liq) in (tf+lat, tf+60] crosses it. Fallback: HL-own-price exit (5m close) or
# Binance fair60 - 0.25 - 0.045.
from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
b = pd.read_parquet(OUT + 's_a2_base_hl.parquet')
rows = []
for r in b.itertuples():
    g = BYC[r.coin]; tsv = g.ts.values
    res = {}
    for lat in (0, 1000, 3000):
        lo = np.searchsorted(tsv, r.ts + lat, side='right'); hi = np.searchsorted(tsv, r.ts + 60000, side='right')
        seg = g.iloc[lo:hi]; seg = seg[seg.wd == -r.wd]
        Ft = seg['b-1'] * (1 + seg.bas)
        for d in (0.0, 0.1, 0.2):
            Aask = Ft * (1 + r.wd * d / 100)
            for pxcol in ('px', 'pl'):
                hit = ((-r.wd) * (seg[pxcol] - Aask) > 0).values
                k = f'{lat}_{d}_{pxcol}'
                if hit.any():
                    j = np.argmax(hit)
                    res['E' + k] = 100 * (-r.wd) * (Aask.iloc[j] / r.L - 1) - 0.03
                    res['f' + k] = True
                    if lat == 1000 and d == 0.1 and pxcol == 'px':
                        res['who_same'] = seg.addr.iloc[j] == r.whale; res['who'] = seg.addr.iloc[j][:8]; res['tfill'] = (seg.ts.iloc[j] - r.ts) / 1000
                        res['who_usd'] = seg.tx_usd.iloc[j]
                else:
                    res['E' + k] = np.nan; res['f' + k] = False
    rows.append(res)
X = pd.concat([b.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
X['fb_bn'] = X.P60 + 0.03 - 0.25 - 0.06          # fallback: taker at 60 s, HL bid = fair - 0.25
X['fb_hl'] = X.HLx_5m                             # fallback: HL own close 1-6 min later
for k in ['0_0.1_px', '1000_0.1_px', '1000_0.1_pl', '3000_0.1_px', '1000_0.0_px', '1000_0.2_px']:
    for fb in ('fb_bn', 'fb_hl'):
        X[f'R{k}_{fb}'] = X['E' + k].where(X['f' + k], X[fb])
    print(f'exit {k}: maker-filled share DISC {X[~X.test]["f"+k].mean():.2f} TEST {X[X.test]["f"+k].mean():.2f}')
    rep(X, f'R{k}_fb_bn', f'  +fallback BN-0.25')
    rep(X, f'R{k}_fb_hl', f'  +fallback HL close')
print('maker-exit counterparty (lat1 d0.1): same tracked wallet as the whale?', X.who_same.mean(), '| top counterparties', X.who.value_counts().head(5).to_dict())
print('counterparty tx_usd median', X.who_usd.median(), 'time to exit fill median', X.tfill.median())
rep(X, 'fb_bn', 'all-taker BN-0.25'); rep(X, 'fb_hl', 'all HL close')
X.to_parquet(OUT + 's_a2_exit.parquet')
