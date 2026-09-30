# Is the "continuation" condition what separates good from bad fills? All tracked taker-close txs with exc>X (X=1), no cooldown,
# P60 vs level L; split by: a same-coin same-direction tracked taker close with td>=0.3 in [ts-61.5 s, ts-1.5 s] (known at ts).
from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
X = 1.0
c = A[A.taker_close].copy()
c['exc'] = 100 * c.wd * (c.pl / (c['b-1'] * (1 + c.bas)) - 1)
c['L'] = c['b-1'] * (1 + c.bas) * (1 + c.wd * X / 100)
for dt in (5, 60, 300):
    c[f'P{dt}'] = 100 * (-c.wd) * (c[f'b{dt}'] * (1 + c.bas) / c.L - 1) - 0.03
big = c[c.td >= 0.3]
f = c[c.exc > X].copy()
cont = []
for r in f.itertuples():
    b = big[(big.coin == r.coin) & (big.wd == r.wd) & (big.ts >= r.ts - 61500) & (big.ts <= r.ts - 1500)]
    cont.append((len(b) > 0, (b.addr == r.addr).any() if len(b) else False))
f['cont'] = [a for a, b in cont]; f['cont_same'] = [b for a, b in cont]
f = f.rename(columns={'addr': 'whale'})
f = add_epi(f)
for nm, m in [('continuation (any wallet)', f.cont), ('continuation same wallet', f.cont_same), ('continuation other wallet', f.cont & ~f.cont_same), ('first strike', ~f.cont)]:
    rep(f[m], 'P60', nm)
# exc-matched comparison
f['eb'] = pd.cut(f.exc, [1, 1.25, 1.5, 2, 3, 100])
print(f.groupby(['eb', 'cont'], observed=True).P60.agg(['size', 'mean', 'median']).round(3).unstack())
f.to_parquet(OUT + 's_cont.parquet')
