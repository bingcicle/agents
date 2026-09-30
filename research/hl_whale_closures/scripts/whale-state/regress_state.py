# Joint OLS: gross move in whale direction after the decision (mid_H, no spread/costs; and market-neutral 24h)
# on whale-state variables + impulse controls; cluster-robust SE by coin x day. Usage: python regress_state.py disc|test|all
import sys, numpy as np, pandas as pd, statsmodels.api as sm
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
from ws import *
which = sys.argv[1] if len(sys.argv) > 1 else 'disc'
ev = load()
d = ev[ev.day <= DISC_END] if which == 'disc' else ev[(ev.day >= TEST_START) & (ev.day <= TEST_END)] if which == 'test' else ev
d = d.copy()
d['mn24'] = d.L24 - d.alt24; d['mn8'] = d.L8 - d.alt8
d['mid_1800mn'] = d.mid_1800 - d.alt_s1800
X = pd.DataFrame({
    'herd30_n': np.log1p(d.nw_same_30), 'herd30_usd': np.clip(d.hs30, 0, 3), 'opp30_usd': np.clip(d.ho30, 0, 3),
    'herd120_n': np.log1p(d.nw_same_120),
    'fresh6': d.fresh6 * d.fresh6_known, 'fresh_unknown': 1 - d.fresh6_known, 'nearfull': (d.r_pct >= 50).astype(int),
    'multicoin': (d.ncoins_past >= 3).astype(int), 'whale_buys': (d.wdir == 1).astype(int),
    'log_rtd': np.log(np.clip(d.rtd, 1e-3, None)), 'log_ratio': np.log(d.batch_ratio), 'imp': np.clip(d.imp, -3, 3)})
X = sm.add_constant(X)
print(f'== {which}: n={len(d)} coin-days={d.cd.nunique()}')
for y in ('mid_300', 'mid_1800', 'mid_1800mn', 'mn8', 'mn24'):
    m = d[y].notna() & X.notna().all(axis=1)
    yy = np.clip(d.loc[m, y], -15, 15) if y.startswith('mn') else np.clip(d.loc[m, y], -5, 5)
    r = sm.OLS(yy, X[m]).fit(cov_type='cluster', cov_kwds={'groups': pd.factorize(d.loc[m, 'cd'])[0]})
    print(f'\n-- y={y} (winsorized) n={int(m.sum())} R2={r.rsquared:.3f}')
    print(pd.DataFrame({'coef': r.params, 't': r.tvalues, 'p': r.pvalues}).round(3).to_string())
