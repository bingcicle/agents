"""T2-like fade (all eligible <=15-min TWAPs, bot decision near end, Binance move since start >= 1% in TWAP direction),
split by intensity; deduped one position per coin; k1m and 1s worst-in-3s. NOTE: the intensity>=0.5 cell was seen on test
in out_t2b.txt before this script -> not a clean test."""
import sys; sys.path.insert(0, '.')
from rules import *
D = base()
D['mv'] = R(D, 'start', 'dec'); D['frac'] = D.lag_s / D.dur_s
E = D[(D.eligible == 1) & (D.frac > 0.5) & (D.frac < 1.1)]
def tape(x, hold_s):
    out = []
    for r in x.itertuples():
        sec = int(r.T_dec // 1000); side = -r.tdir
        b = bars(r.sym, (sec - 5) * 1000, (sec + hold_s + 15) * 1000)
        if b is None: out.append(np.nan); continue
        out.append(100 * side * (worst_px(b, sec + hold_s, -side) / worst_px(b, sec, side) - 1))
    return np.array(out)
for per in ('disc', 'test'):
    s = E[E.per == per]
    for lab, f in (('mv>=1 all', s.mv >= 1), ('mv>=1 int<0.5', (s.mv >= 1) & (s.intensity < 0.5)), ('mv>=1 int>=0.5', (s.mv >= 1) & (s.intensity >= 0.5)),
                   ('any mv int>=0.5', s.intensity >= 0.5), ('mv>=1 reduce', (s.mv >= 1) & (s.kind == 'reduce'))):
        x = trades(s[f], -1, 'dec', 'dec_60')
        x['tape'] = tape(x, 3600) - COST
        print(per, f'{lab:18s}', battery(x, 'net', 'k1m'))
        print(per, f'{lab:18s}', line(x.tape, x.cd, 'tape worst3s net'))
