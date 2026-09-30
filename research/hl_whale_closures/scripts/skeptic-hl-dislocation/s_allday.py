# All-day resting order (candidate A / A-deep): fill = any tracked taker-close tx whose last fill is beyond X% of fair; 1 fill per coin per 60 s.
from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
def allday(X, ref='b-1', cool=60, fillset='lens'):
    c = A if fillset == 'all' else A[A.taker_close]
    F = c[ref] * (1 + c.bas)
    exc = 100 * c.wd * (c.pl / F - 1)
    f = c[exc > X].sort_values('ts')
    keep = []; last = {}
    for i, co, t in zip(f.index, f.coin.values, f.ts.values):
        if co in last and t - last[co] < cool * 1000:
            continue
        last[co] = t; keep.append(i)
    f = f.loc[keep]
    L = f[ref] * (1 + f.bas) * (1 + f.wd * X / 100)
    o = pd.DataFrame(dict(whale=f.addr, coin=f.coin, day=f.day, test=f.test, ts=f.ts, wd=f.wd, L=L, bas=f.bas, td=f.td, sym=f.sym,
                          exc=100 * f.wd * (f.pl / (f[ref] * (1 + f.bas)) - 1)))
    for dt in (5, 60, 300):
        o[f'P{dt}'] = 100 * (-f.wd) * (f[f'b{dt}'] * (1 + f.bas) / L - 1) - 0.03
    o['H'] = 100 * (-f.wd) * (f['hedge13'] * (1 + f.bas) / L - 1) - 0.16
    # continuation flag: a same-coin same-direction tracked taker close with td>=0.3 in [ts-61.5 s, ts-1.5 s]
    return add_epi(o)
if __name__ == '__main__':
    for X in (1.0, 1.5, 2.0, 3.0):
        f = allday(X)
        f.to_parquet(OUT + f's_allday_X{X}.parquet')
        print(f'=== X={X}: fills/day DISC {(~f.test).sum()/10:.1f} TEST {f.test.sum()/8:.1f}; LONG share {(f.wd<0).mean():.2f}')
        for col in ('P5', 'P60', 'P300', 'H'):
            rep(f, col, f'X{X}')
        if X in (1.5, 2.0):
            rep(allday(X, ref='b-2'), 'P60', f'X{X} ref b-2')
            rep(allday(X, ref='b-5'), 'P60', f'X{X} ref b-5')
            rep(allday(X, fillset='all'), 'P60', f'X{X} incl liq/opens')
