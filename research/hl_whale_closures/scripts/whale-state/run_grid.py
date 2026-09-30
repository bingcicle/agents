# Evaluate the pre-declared 40-cell grid (GRID_DECLARED.txt). Usage: python run_grid.py disc | test
import sys, json, numpy as np, pandas as pd, time
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
from ws import *
mode = sys.argv[1]
GRID_STATES = ['HERD30_1', 'HERD30_2P', 'HERD120_3P', 'HERDUSD30', 'OPP30', 'FRESH6', 'NEARFULL', 'MULTICOIN', 'WDISTRESS', 'WPROFIT']
GRID_HOR = ['S300', 'S1800', 'L8', 'L24']
ev = load()
d = ev[ev.day <= DISC_END].reset_index(drop=True) if mode == 'disc' else ev[(ev.day >= TEST_START) & (ev.day <= TEST_END)].reset_index(drop=True)
STATES['ALL'] = lambda x: (np.ones(len(x), bool), np.ones(len(x), bool))
t0 = time.time()
rows = []
fmt = lambda o: (f"n={o.get('n',0):4d} pd={o.get('per_day',0):4.1f} mean={o.get('mean',np.nan):+.3f} med={o.get('median',np.nan):+.3f} "
                 f"win={o.get('win',np.nan):.2f} ciC={o.get('ci_c','')} ciD={o.get('ci_d','')} ciW={o.get('ci_w','')} "
                 f"h1/h2={o.get('half1',np.nan):+.2f}/{o.get('half2',np.nan):+.2f} s_wo10={o.get('sum_wo_top10',np.nan):+.1f} "
                 f"maxsh c/w/d={o.get('maxsh_coin',np.nan):.2f}/{o.get('maxsh_addr',np.nan):.2f}/{o.get('maxsh_day',np.nan):.2f} "
                 f"L {o.get('n_long',0)}:{o.get('mean_long',np.nan):+.2f} S {o.get('n_short',0)}:{o.get('mean_short',np.nan):+.2f} w/c={o.get('wallets',0)}/{o.get('coins',0)}")
out = open(W + f'out_grid_{mode}.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n'); out.flush()
P(f'== GRID {mode.upper()} : events {len(d)} days {d.day.nunique()} ({d.day.min()}..{d.day.max()})')
P('-- baselines (all universe events, deduped) --')
for hor in GRID_HOR:
    for dirn in ('fol', 'fad'):
        x = cell_frame(d, 'ALL', hor, dirn); o = battery(x)
        P(f'ALL {hor:5s} {dirn}: ' + fmt(o))
        rows.append(dict(state='ALL', hor=hor, dir=dirn, **{k: v for k, v in o.items() if not isinstance(v, tuple)},
                         ci_c=str(o.get('ci_c')), ci_d=str(o.get('ci_d')), ci_w=str(o.get('ci_w'))))
if mode == 'test':
    decl = pd.read_csv(W + 'grid_declared_dirs.csv')
    decl = {(r.state, r.hor): r.dir for r in decl.itertuples()}
P('-- grid cells --')
for st in GRID_STATES:
    for hor in GRID_HOR:
        res = {}
        for dirn in ('fol', 'fad'):
            x = cell_frame(d, st, hor, dirn); res[dirn] = (x, battery(x))
        if mode == 'disc':
            m_f = res['fol'][1].get('mean', -9); m_d = res['fad'][1].get('mean', -9)
            dirn = 'fol' if m_f >= m_d else 'fad'
        else:
            dirn = decl[(st, hor)]
        x, o = res[dirn]
        p, nm, _ = null_p(d, st, hor, dirn, o.get('mean', np.nan), nperm=300) if o.get('n', 0) >= 3 else (np.nan, np.nan, None)
        other = 'fad' if dirn == 'fol' else 'fol'
        P(f'{st:11s} {hor:5s} {dirn} ' + fmt(o) + f' | null_mean={nm:+.3f} p_state={p:.3f} | other dir mean={res[other][1].get("mean", np.nan):+.3f}')
        rows.append(dict(state=st, hor=hor, dir=dirn, null_mean=nm, p_state=p, other_mean=res[other][1].get('mean', np.nan),
                         **{k: v for k, v in o.items() if not isinstance(v, tuple)}, ci_c=str(o.get('ci_c')), ci_d=str(o.get('ci_d')), ci_w=str(o.get('ci_w'))))
R = pd.DataFrame(rows)
R.to_csv(W + f'grid_{mode}.csv', index=False)
g = R[R.state != 'ALL']
if mode == 'disc':
    g[['state', 'hor', 'dir']].to_csv(W + 'grid_declared_dirs.csv', index=False)
    cand = g[(g.n >= 15) & (g['mean'] > 0) & (g['median'] > 0) & (g.p_state < 0.10)]
    P(f'\nDISCOVERY candidates (n>=15, mean>0, median>0, p_state<0.10): {len(cand)}')
    P(cand[['state', 'hor', 'dir', 'n', 'mean', 'median', 'p_state']].to_string() if len(cand) else '  none')
P(f'\ncells with p_state<0.05: {(g.p_state<0.05).sum()} of {g.p_state.notna().sum()} (chance ~{0.05*g.p_state.notna().sum():.1f}); '
  f'cells with mean>0: {(g["mean"]>0).sum()} ; with mean>0 & median>0: {((g["mean"]>0)&(g["median"]>0)).sum()}')
P(f'time {time.time()-t0:.0f}s')
