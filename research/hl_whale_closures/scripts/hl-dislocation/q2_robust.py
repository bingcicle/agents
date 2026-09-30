# Robustness of the whale-conditional deep-liquidity result (family: ALL tracked-whale taker closes, X in {1.5, 2}).
from strat import *
import strat
out = open(W + 'q2_robust_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')

def fills_ref(df, X, ref='bn-1', basis_col='basis', cool_s=60):
    """Same fill logic as strat.fills but with a different Binance reference second / basis estimator."""
    d = df.copy()
    F = d[ref] * (1 + d[basis_col].fillna(0))
    d['exc_pl'] = 100 * d.wdir * (d.pl / F - 1)
    d = d[d.exc_pl > X].sort_values('ts')
    keep = []; last = {}
    for i, c, t in zip(d.index, d.coin.values, d.ts.values):
        if c in last and t - last[c] < cool_s * 1000:
            continue
        last[c] = t; keep.append(i)
    f = d.loc[keep].copy()
    L = F.loc[f.index] * (1 + f.wdir * X / 100)                   # our level (placement uses ref/basis_col)
    for dt in (5, 60, 300):
        fair = f[f'bn{dt}'] * (1 + f['basis'])                    # valuation always at Binance x (1+basis_1h)
        f[f'P{dt}'] = 100 * (-f.wdir) * (fair / L - 1) - 0.03
    return f

tm = pd.read_parquet(SP + 'data/tx_matrix.parquet', columns=['addr', 'coin', 'h', 'BTCr60', 'BTCr300'])
res = []
for X in (1.5, 2.0):
    for part, sel in [('DISC', ~w.test), ('TEST', w.test)]:
        d = w[sel]; nd = d.day.nunique()
        variants = {
            'base (bn-1, basis_1h, cool 60s)': fills_ref(d, X),
            'cooldown 5s': fills_ref(d, X, cool_s=5),
            'cooldown 300s': fills_ref(d, X, cool_s=300),
            'stale ref bn-2': fills_ref(d, X, ref='bn-2'),
            'stale ref bn-5': fills_ref(d, X, ref='bn-5'),
            'no basis (0)': fills_ref(d.assign(zero=0.0), X, basis_col='zero'),
            'basis B (prints)': fills_ref(d[d.basisB.notna()], X, basis_col='basisB'),
        }
        for name, f in variants.items():
            r = dict(X=X, part=part, variant=name, n=len(f), per_day=len(f) / nd)
            for ex in ('P5', 'P60', 'P300'):
                r[ex + '_md'] = f[ex].median(); r[ex + '_mn'] = f[ex].mean()
            res.append(r)
        # BTC-adjusted and side split, drop-top-coin on the base variant
        f = variants['base (bn-1, basis_1h, cool 60s)'].merge(tm, on=['addr', 'coin', 'h'], how='left')
        f['P60_btc'] = f.P60 + f.BTCr60; f['P300_btc'] = f.P300 + f.BTCr300
        P(f'\nX={X} {part}: base n={len(f)}; BTC coverage {f.BTCr60.notna().mean():.2f}; P60 mean {f.P60.mean():+.3f} -> BTC-adj {f.P60_btc.mean():+.3f};'
          f' P300 mean {f.P300.mean():+.3f} -> BTC-adj {f.P300_btc.mean():+.3f}')
        f['side'] = np.where(f.wdir < 0, 'LONG', 'SHORT')
        for sd, g in f.groupby('side'):
            P(f'   our {sd}: n={len(g)} P5 {g.P5.mean():+.3f}/{g.P5.median():+.3f}  P60 {g.P60.mean():+.3f}/{g.P60.median():+.3f}  P300 {g.P300.mean():+.3f}')
        top = f.groupby('coin').P60.sum().sort_values(ascending=False)
        P('   top coins by P60 sum:', top.head(5).round(2).to_dict())
        g2 = f[f.coin != top.index[0]]
        P(f'   without top coin {top.index[0]}: n={len(g2)} P60 mean {g2.P60.mean():+.3f} median {g2.P60.median():+.3f}')
        # dislocation size vs outcome
        f['eb'] = pd.cut(f.exc_pl, [X, X + 0.5, X + 1.5, 100])
        P('   by exc_pl bin:', f.groupby('eb', observed=True).agg(n=('P60', 'size'), P5=('P5', 'mean'), P60=('P60', 'mean'), P300=('P300', 'mean')).round(3).to_dict('index'))
        f['tdb'] = pd.cut(f.td, [0, 0.3, 1, 1000])
        P('   by td bin:', f.groupby('tdb', observed=True).agg(n=('P60', 'size'), P5=('P5', 'mean'), P60=('P60', 'mean'), P300=('P300', 'mean')).round(3).to_dict('index'))
R = pd.DataFrame(res)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
P('\nSensitivity table (P&L per fill %, passive exit at Binance fair, fees 0.03):')
P(R.round(3).to_string())
out.close()
