# S-grid (GRID_DECLARED.txt addendum): whale PnL / liq distance at the simulator trigger. Usage: python run_sim_grid.py disc | test
import sys, numpy as np, pandas as pd, statsmodels.api as sm
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
import ws
from ws import *
mode = sys.argv[1]
S = pd.read_parquet(W + 'sim_events_out.parquet')
S['cd'] = S.coin + '|' + S.day
ws.STATES.update({
    'S_WPROFIT3':  lambda d: (d.wpnl > 3, np.ones(len(d), bool)),
    'S_WPROFIT10': lambda d: (d.wpnl >= 10, np.ones(len(d), bool)),
    'S_WLOSS3':    lambda d: (d.wpnl < -3, np.ones(len(d), bool)),
    'S_LIQ5':      lambda d: (d.liq_dist < 5, np.ones(len(d), bool)),
    'S_DISTRESS':  lambda d: ((d.wpnl < 0) & (d.liq_dist < 5), np.ones(len(d), bool)),
    'ALL':         lambda d: (np.ones(len(d), bool), np.ones(len(d), bool)),
})
disc = S[S.day <= DISC_END].reset_index(drop=True)
d = disc if mode == 'disc' else S[(S.day >= TEST_START) & (S.day <= TEST_END)].reset_index(drop=True)
# generic-reversal control: fit y_dir ~ past returns on DISCOVERY (per horizon, whale-dir gross mn outcome)
def pastX(x):
    return sm.add_constant(pd.DataFrame({'p24': np.clip(x.past_mn_24h, -30, 30), 'p72': np.clip(x.past_mn_72h, -50, 50),
                                         'p7d': np.clip(x.past_mn_7d, -80, 80)}).fillna(0))
ctrl = {}
for hor, ycol in (('S300', None), ('L24', None)):
    y = (disc.fol_300 - disc.alt_s300.fillna(0)) if hor == 'S300' else (disc.L24 - disc.alt24)
    m = y.notna()
    ctrl[hor] = sm.OLS(np.clip(y[m], -15, 15), pastX(disc[m])).fit().params
out = open(W + f'out_sim_grid_{mode}.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n'); out.flush()
P(f'== S-GRID {mode.upper()} events {len(d)} days {d.day.nunique()} ({d.day.min()}..{d.day.max()}) wallets {d.addr.nunique()}')
P('reversal-control coefs (fit on discovery):', {k: v.round(4).to_dict() for k, v in ctrl.items()})
decl = None
if mode == 'test':
    decl = pd.read_csv(W + 'sim_grid_declared_dirs.csv'); decl = {(r.state, r.hor): r.dir for r in decl.itertuples()}
rows = []
for st in ['ALL', 'S_WPROFIT3', 'S_WPROFIT10', 'S_WLOSS3', 'S_LIQ5', 'S_DISTRESS']:
    for hor in ('S300', 'L24'):
        res = {dr: cell_frame(d, st, hor, dr) for dr in ('fol', 'fad')}
        bat = {dr: battery(res[dr]) for dr in res}
        if st == 'ALL':
            for dr in ('fol', 'fad'):
                P(f'{st:11s} {hor:4s} {dr}: n={bat[dr]["n"]} mean={bat[dr]["mean"]:+.3f} med={bat[dr]["median"]:+.3f} ciC={bat[dr]["ci_c"]} ciD={bat[dr]["ci_d"]}')
            continue
        dr = (decl[(st, hor)] if decl else ('fol' if bat['fol']['mean'] >= bat['fad']['mean'] else 'fad'))
        x, o = res[dr], bat[dr]
        s = 1 if dr == 'fol' else -1
        pred = pastX(x) @ ctrl[hor]
        exc = (x.y + 0.146 - s * pred) if True else None   # gross mn outcome minus predicted generic reversal (both in our side)
        p, nm, _ = null_p(d, st, hor, dr, o['mean'], nperm=300)
        P(f'{st:11s} {hor:4s} {dr}: n={o["n"]} pd={o["per_day"]:.1f} mean={o["mean"]:+.3f} med={o["median"]:+.3f} win={o["win"]:.2f} '
          f'ciC={o["ci_c"]} ciD={o["ci_d"]} ciW={o["ci_w"]} h1/h2={o["half1"]:+.2f}/{o["half2"]:+.2f} s_wo10={o["sum_wo_top10"]:+.1f} '
          f'maxsh c/w/d={o["maxsh_coin"]:.2f}/{o["maxsh_addr"]:.2f}/{o["maxsh_day"]:.2f} L {o["n_long"]}:{o["mean_long"]:+.2f} S {o["n_short"]}:{o["mean_short"]:+.2f} '
          f'| excess_over_reversal(gross)={exc.mean():+.3f} | null_mean={nm:+.3f} p_state={p:.3f} | other dir {bat["fad" if dr=="fol" else "fol"]["mean"]:+.3f}')
        rows.append(dict(state=st, hor=hor, dir=dr, n=o['n'], mean=o['mean'], median=o['median'], p_state=p, excess=exc.mean(),
                         ci_c=str(o['ci_c']), ci_d=str(o['ci_d'])))
R = pd.DataFrame(rows); R.to_csv(W + f'sim_grid_{mode}.csv', index=False)
if mode == 'disc':
    R[['state', 'hor', 'dir']].to_csv(W + 'sim_grid_declared_dirs.csv', index=False)
    c = R[(R.n >= 15) & (R['mean'] > 0) & (R['median'] > 0) & (R.p_state < 0.10)]
    P('DISCOVERY candidates:', 'none' if c.empty else c.to_string())
