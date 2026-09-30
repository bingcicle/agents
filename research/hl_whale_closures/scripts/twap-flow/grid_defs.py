import numpy as np
q40, q60, q80 = 0.0122, 0.0388, 0.1203       # intensity quintile edges (all-period; used only as round thresholds)
FILT = {
    'all': lambda x: x.index == x.index,
    'int>=q60': lambda x: x.intensity >= q60, 'int>=q80': lambda x: x.intensity >= q80, 'int<q40': lambda x: x.intensity < q40,
    'dur<=15': lambda x: x.dur_min <= 15, 'dur15-180': lambda x: (x.dur_min > 15) & (x.dur_min <= 180), 'dur>180': lambda x: x.dur_min > 180,
    'reduce': lambda x: x.kind2 == 'reduce', 'increase': lambda x: x.kind2 == 'increase', 'open': lambda x: x.kind2 == 'open',
    'inc|open': lambda x: x.kind2.isin(['increase', 'open']), 'buy': lambda x: x.twap_side == 'buy', 'sell': lambda x: x.twap_side == 'sell',
    'hl_src': lambda x: x.hl_src, 'usd>=1M': lambda x: x.usd >= 1e6,
}
SHAPES = []
for dr in (1, -1):
    for xk in ('e1_15', 'e1_60', 'e1_240', 'mid', 'end'):
        SHAPES.append((dr, 'e1', xk))
    for xk in ('end_15', 'end_60', 'end_120', 'end_240'):
        SHAPES.append((dr, 'end', xk))
