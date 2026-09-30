"""Shared helpers for validate-bars lens (read-only data, outputs only in this dir)."""
import sys, os, math, json
import numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
from hl import *  # noqa

OUT = SP + 'work/validate-bars/'
BARS_END_MS = int(pd.Timestamp('2026-09-30 00:00:00').value // 10**6)  # Binance bars end 29.09 23:59:59 UTC


def load_follow():
    f = pd.read_csv(BOT + 'follow_trades.csv', low_memory=False)
    f = f[f.eol == '^']
    fl = pd.read_csv(BOT + 'follow_trades.csv.legacy-1789241806.csv', low_memory=False)
    fl = fl[fl.eol == '^'] if 'eol' in fl else fl
    return f, fl


def load_settle():
    s = pd.read_csv(BOT + 'settlements.csv', low_memory=False)
    s = s[s.eol == '^']
    return s.drop_duplicates('key', keep='last')


def gross(side, px_in, px_out):
    """% gross for OUR side (+1 long, -1 short); short = (in-out)/in, as in settlements."""
    return 100 * (px_out / px_in - 1) if side > 0 else 100 * (px_in - px_out) / px_in


def sim_tp_sl(b, s0, side, px_in, tp, sl, cap_s, det=2, start_off=1):
    """First 1s bar (close) with gross >= tp or <= -sl, scanning seconds s0+start_off..s0+cap_s;
    exit at sec+det worst-in-3s; else exit at s0+cap_s worst-in-3s. Returns (px_out, reason, exit_sec)."""
    seg = b.loc[s0 + start_off: s0 + cap_s]
    c = seg.c.values
    if side > 0:
        g = 100 * (c / px_in - 1)
    else:
        g = 100 * (px_in - c) / px_in
    hit = np.where((g >= tp) | (g <= -sl))[0]
    if len(hit):
        sec = int(seg.index[hit[0]])
        reason = 'tp' if g[hit[0]] >= tp else 'sl'
        xs = sec + det
    else:
        reason, xs = 'cap', s0 + cap_s
    px = worst_px(b, xs, -side)
    return px, reason, xs


def btc_ret(t0_ms, t1_ms, side):
    r = ret_min('BTCUSDT', t0_ms, t1_ms)
    return side * r if not np.isnan(r) else np.nan


def battery(d, col, wallet='whale', day='day', coin='coin'):
    """honesty() + formatted string."""
    h = honesty(d, col, wallet, day, coin)
    return h


def fmt(h):
    if h.get('n', 0) == 0:
        return 'n=0'
    keys = ['n', 'median', 'mean', 'win']
    s = ' '.join(f'{k}={h[k]:+.3f}' if isinstance(h[k], float) else f'{k}={h[k]}' for k in keys)
    rest = {k: v for k, v in h.items() if k not in keys}
    return s + ' | ' + ', '.join(f'{k}={v if not isinstance(v, float) else round(v, 3)}' for k, v in rest.items())


def day_stats(d, col, day='day'):
    g = d.groupby(day)[col]
    return pd.DataFrame({'n': g.size(), 'med': g.median(), 'mean': g.mean()})
