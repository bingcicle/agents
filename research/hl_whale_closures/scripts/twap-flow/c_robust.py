"""Robustness of candidate C (fade 'increase' TWAPs from e1 for 60 min): 1s worst-in-3s prices, neighbours, kind source."""
import sys; sys.path.insert(0, '.')
from rules import *
D = base()
def tape(x, hold_s):
    """worst-in-3s entry at second T_e1/1000 (fade side), worst exit at +hold_s"""
    out = []
    for r in x.itertuples():
        sec = int(r.T_e1 // 1000); side = -r.tdir   # our side (fade)
        b = bars(r.sym, (sec - 5) * 1000, (sec + hold_s + 15) * 1000)
        if b is None: out.append(np.nan); continue
        en = worst_px(b, sec, side); ex = worst_px(b, sec + hold_s, -side)
        out.append(100 * side * (ex / en - 1))
    return np.array(out)
for per in ('disc', 'test'):
    s = D[D.per == per]
    x = trades(s[s.kind2 == 'increase'], -1, 'e1', 'e1_60')
    x['tape'] = tape(x, 3600) - COST
    print(per, 'C k1m last  :', line(x.net, x.cd, 'net'))
    print(per, 'C 1s worst3s:', line(x.tape, x.cd, 'net'), ' hasbars', np.isfinite(x.tape).mean().round(3))
    for xk in ('e1_15', 'e1_30', 'e1_60', 'e1_120', 'e1_240', 'end'):
        y = trades(s[s.kind2 == 'increase'], -1, 'e1', xk)
        print(per, f'   neighbour exit {xk:7s}', line(y.net, y.cd, 'net'))
    for src, f in (('slice-kind', s.kind == 'increase'), ('inferred-only', s.kind.isna() & (s.kind_inf == 'increase'))):
        y = trades(s[f], -1, 'e1', 'e1_60')
        print(per, f'   {src:14s}', line(y.net, y.cd, 'net'))
    for k in ('open', 'reduce', 'unknown'):
        y = trades(s[s.kind2 == k], -1, 'e1', 'e1_60')
        print(per, f'   same rule on kind={k:8s}', line(y.net, y.cd, 'net'))
