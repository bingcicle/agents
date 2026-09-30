from fade_eval import *
for sn in ('td>=0.2', 'rel24>=1'):
    m = SIZE[sn] & (E.pre60 >= 0.5) & (E.m120 >= 0.3)
    d = trades(m, 300, 3600)
    for per in ('D', 'T', None):
        x = d if per is None else d[d.per == per]
        print(battery(x, f'FADE {sn} & pre60>=0.5 & m120>=0.3, enter +300s, exit +3600s, period={per or "ALL"}'))
    dl = trades(m & (E.detect_lag_s <= 290), 300, 3600)
    print(f'   bot-realistic (detected <=290 s): n={len(dl)} net mean={(dl.g-COST).mean():+.3f} med={(dl.g-COST).median():+.3f}; D {(dl[dl.per=="D"].g-COST).mean():+.3f} T {(dl[dl.per=="T"].g-COST).mean():+.3f}')
    for H in (900, 1800):
        dh = trades(m, 300, H)
        print(f'   exit {H}s: D n={len(dh[dh.per=="D"])} net {(dh[dh.per=="D"].g-COST).mean():+.3f} | T n={len(dh[dh.per=="T"])} net {(dh[dh.per=="T"].g-COST).mean():+.3f}')
