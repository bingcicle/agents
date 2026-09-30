import sys; sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
from hl import *
import pandas as pd, numpy as np
def load_twaps():
    d = pd.read_csv(BOT + 'twap_signals.csv')
    d = d[d.eol == '^'].copy()
    for c in ('date', 'start', 'end'):
        d[c + '_ms'] = (pd.to_datetime(d[c]) - pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
    d['sym'] = d.coin.map(SYM)
    d['tdir'] = np.where(d.twap_side == 'buy', 1, -1)
    d['lag_s'] = (d.date_ms - d.start_ms) / 1000
    d['day'] = pd.to_datetime(d.start_ms, unit='ms').dt.strftime('%Y-%m-%d')
    return d
