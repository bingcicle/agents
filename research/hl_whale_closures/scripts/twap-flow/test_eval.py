"""ONE-TIME evaluation on TEST (22.09-29.09) of the rules chosen on discovery (grid top by CI lower bound, distinct families)
plus the user-focus 'reduce' rule. Also discovery numbers side by side, placebo percentile, cost sensitivity."""
import sys; sys.path.insert(0, '.')
from rules import *
from placebo import placebo
D = base()
picks = [('A dur>180 follow e1->end', lambda x: x.dur_min > 180, 1, 'e1', 'end'),
         ('B dur>180 follow e1->+240m', lambda x: x.dur_min > 180, 1, 'e1', 'e1_240'),
         ('C increase fade e1->+60m', lambda x: x.kind2 == 'increase', -1, 'e1', 'e1_60'),
         ('D reduce follow end->+240m', lambda x: x.kind2 == 'reduce', 1, 'end', 'end_240')]
for name, f, dr, ek, xk in picks:
    print(f'\n######## {name}')
    for per in ('disc', 'test'):
        s = D[D.per == per]
        x = trades(s[f(s)], dr, ek, xk)
        x['T_ent'] = x['T_' + ek]
        print(per.upper(), battery(x, 'net', 'net0.146'))
        print(per.upper(), battery(x, 'net_a', 'net_xalt'))
        g = x.gross.values
        print(per.upper(), f'cost sens: net@0.10={np.mean(g-0.10):+.3f} net@0.04(maker)={np.mean(g-0.04):+.3f}  median@0.10={np.median(g-0.10):+.3f}')
        pm = placebo(x, reps=200)
        print(per.upper(), f'placebo (random minute same coin/day/hold/side): mean of placebo means={pm.mean():+.3f} q5..q95=[{np.percentile(pm,5):+.3f},{np.percentile(pm,95):+.3f}]  real={x.net.mean():+.3f}  pctile={100*(pm<x.net.mean()).mean():.0f}%')
