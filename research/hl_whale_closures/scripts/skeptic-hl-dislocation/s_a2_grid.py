from s_a2 import *
import warnings, itertools; warnings.filterwarnings('ignore')
A['exc_self'] = 100 * A.wd * (A.pl / (A['b-1'] * (1 + A.bas)) - 1)
trigs = {'td>=0.2': A.td >= 0.2, 'td>=0.3': A.td >= 0.3, 'td>=0.5': A.td >= 0.5, 'td>=1': A.td >= 1.0,
         'exc>=0.5': A.exc_self >= 0.5, 'exc>=1': A.exc_self >= 1.0}
rows = []
for (tn, tm), X, Wsec in itertools.product(trigs.items(), (0.5, 0.75, 1.0, 1.25, 1.5, 2.0), (30, 60, 120, 300)):
    f = run(tm, X=X, Wsec=Wsec)
    r = dict(trig=tn, X=X, W=Wsec)
    for part, nm in ((False, 'D'), (True, 'T')):
        g = f[f.test == part] if len(f) else f
        r[f'n{nm}'] = len(g); r[f'P60{nm}'] = g.P60.mean() if len(g) else np.nan; r[f'P5{nm}'] = g.P5.mean() if len(g) else np.nan
        r[f'H{nm}'] = g.H.mean() if len(g) else np.nan
    rows.append(r)
G = pd.DataFrame(rows); G.to_csv(OUT + 's_a2_grid.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_rows', 300)
print('K =', len(G))
ok = G[(G.nD >= 20)]
print('variants with nD>=20:', len(ok), '| DISC P60>0:', (ok.P60D > 0).sum(), '| TEST P60>0:', (ok.P60T > 0).sum(), '| TEST P60>0.2:', (ok.P60T > 0.2).sum())
print('TEST P60 across nD>=20 variants: quantiles', ok.P60T.quantile([0, .1, .25, .5, .75, .9, 1]).round(3).to_dict())
print('corr DISC vs TEST P60 across variants', ok[['P60D', 'P60T']].corr().iloc[0, 1].round(3))
best = ok.sort_values('P60D', ascending=False).head(10)
print('Top-10 on DISC (nD>=20):'); print(best.round(3).to_string())
print('\nBy X (median over triggers & W):'); print(ok.groupby('X')[['nD', 'P60D', 'nT', 'P60T', 'HD', 'HT']].median().round(3))
print('\nBy trigger:'); print(ok.groupby('trig')[['nD', 'P60D', 'nT', 'P60T']].median().round(3))
print('\nBy W:'); print(ok.groupby('W')[['nD', 'P60D', 'nT', 'P60T']].median().round(3))
