"""Final battery for all candidates (disc and test), incl. wallet(addr)-cluster CI. Output -> out_final_cands.txt"""
import sys; sys.path.insert(0, '.')
from rules import *
from placebo import placebo
D = base()
D['mv'] = R(D, 'start', 'dec'); D['frac'] = D.lag_s / D.dur_s
elig_end = (D.eligible == 1) & (D.frac > 0.5) & (D.frac < 1.1)
C = [('A dur>180 follow e1->end', lambda x: x.dur_min > 180, 1, 'e1', 'end'),
     ('B dur>180 follow e1->+240m', lambda x: x.dur_min > 180, 1, 'e1', 'e1_240'),
     ('C increase fade e1->+60m', lambda x: x.kind2 == 'increase', -1, 'e1', 'e1_60'),
     ('D reduce follow end->+240m', lambda x: x.kind2 == 'reduce', 1, 'end', 'end_240'),
     ('E T2-like fade mv>=1 & int>=0.5 dec->+60m', lambda x: elig_end.loc[x.index] & (x.mv >= 1) & (x.intensity >= 0.5), -1, 'dec', 'dec_60'),
     ('E0 T2-like fade mv>=1 (all int) dec->+60m', lambda x: elig_end.loc[x.index] & (x.mv >= 1), -1, 'dec', 'dec_60'),
     ('F follow all e1->+15m', lambda x: x.index == x.index, 1, 'e1', 'e1_15'),
     ('G follow all e1->end', lambda x: x.index == x.index, 1, 'e1', 'end'),
     ('H fade all end->+60m', lambda x: x.index == x.index, -1, 'end', 'end_60')]
for name, f, dr, ek, xk in C:
    print(f'\n######## {name}')
    for per in ('disc', 'test'):
        s = D[D.per == per]
        x = trades(s[f(s)], dr, ek, xk); x['T_ent'] = x['T_' + ek]
        print(per.upper(), battery(x, 'net', 'net0.146'))
        lo, hi, p = boot_ci(x.net.values, x.addr.values, np.mean, n=1000)
        print(per.upper(), f'CI90mean_wallet=[{lo:+.3f},{hi:+.3f}] wallets={x.addr.nunique()} coins={x.coin.nunique()} days={x.dday.nunique()}  xalt_net_mean={x.net_a.mean():+.3f} net@0.10={x.gross.mean()-0.10:+.3f} net@maker0.04={x.gross.mean()-0.04:+.3f}')
        if len(x) >= 10:
            pm = placebo(x, reps=100)
            print(per.upper(), f'placebo mean={pm.mean():+.3f} q5..q95=[{np.percentile(pm,5):+.3f},{np.percentile(pm,95):+.3f}] real pctile={100*(pm<x.net.mean()).mean():.0f}%')
