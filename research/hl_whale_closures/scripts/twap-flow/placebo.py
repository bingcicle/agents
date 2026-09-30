"""Random-time placebo: same coin, same UTC day, same holding length, same trade side; entry at a random minute."""
import sys; sys.path.insert(0, '.')
from rules import *
from build2 import pfun
def placebo(x, reps=200, seed=0):
    rng = np.random.default_rng(seed)
    x = x.copy(); x['sgn'] = np.where(x.our == 'LONG', 1, -1)
    x['T_en'] = x['T_ent'].astype('int64'); x['hold'] = (x.hold_min * 60000).astype('int64')
    x['d0'] = pd.to_datetime(x.dday).astype('datetime64[ms]').astype('int64')
    means = np.zeros(reps); cnt = np.zeros(reps)
    acc = np.zeros((reps, len(x)))
    for s, idx in x.groupby('sym').groups.items():
        k = kl(s); ot = k.index.values.astype('int64'); c = k.c.values
        xx = x.loc[idx]
        pos = np.array([x.index.get_loc(i) for i in idx])
        for r in range(reps):
            T = xx.d0.values + rng.integers(1, 1440, len(xx)) * 60000
            p0 = pfun(ot, c, T); p1 = pfun(ot, c, T + xx.hold.values)
            acc[r, pos] = xx.sgn.values * 100 * (p1 / p0 - 1) - COST
    m = np.nanmean(acc, axis=1)
    return m
