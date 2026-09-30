"""Generic TWAP rule evaluator: filter -> direction -> entry/exit price points (k1m last price at minute boundaries),
one open position per coin (later signals in the same coin are skipped while a position is open), net = gross - costs."""
import sys; sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/twap-flow')
from common import *
COST = COSTS_OFFICIAL
def base():
    d = get().merge(pd.read_parquet('/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/twap-flow/kind_inf.parquet'), on='twap_id')
    d = d[(d.per != 'none')].copy()
    d['dur_min'] = d.dur_s / 60
    d['hl_src'] = d.src.str.contains('HL_TWAP')
    return d
def trades(d, direction, ek, xk, cap_key=None):
    """direction +1 follow TWAP, -1 fade. ek/xk price keys. Returns deduped trade frame with gross/net/xalt/xbtc."""
    x = d.copy()
    x = x[x['T_' + xk] > x['T_' + ek]]
    x = x[np.isfinite(x['p_' + ek]) & np.isfinite(x['p_' + xk])]
    x = x.sort_values('T_' + ek)
    keep = []; busy = {}
    for i, c, te, tx in zip(x.index, x.coin, x['T_' + ek].values, x['T_' + xk].values):
        if busy.get(c, 0) > te:
            continue
        keep.append(i); busy[c] = tx
    x = x.loc[keep].copy()
    x['gross'] = direction * R(x, ek, xk); x['gross_a'] = direction * R(x, ek, xk, 'a'); x['gross_b'] = direction * R(x, ek, xk, 'b')
    x['net'] = x.gross - COST; x['net_a'] = x.gross_a - COST
    x['our'] = np.where(direction * x.tdir > 0, 'LONG', 'SHORT')
    x['hold_min'] = (x['T_' + xk] - x['T_' + ek]) / 60000
    return x
def battery(x, col='net', label='', nb=1000):
    if len(x) < 5:
        return f'{label} n={len(x)}'
    v = x[col].values
    days = sorted(x.dday.unique()); nd = len(days)
    out = [f'{label} n={len(x)} per_day={len(x)/max(nd,1):.1f} med={np.median(v):+.3f} mean={v.mean():+.3f} win={100*(v>0).mean():.0f}%']
    for c in ('cd', 'coin', 'dday'):
        lo, hi, p = boot_ci(v, x[c].values, np.mean, n=nb)
        out.append(f'CI90mean_{c}=[{lo:+.3f},{hi:+.3f}]')
    lo, hi, p = boot_ci(v, x['cd'].values, np.median, n=nb)
    out.append(f'CI90med_cd=[{lo:+.3f},{hi:+.3f}]')
    h = nd // 2
    out.append(f'halves={x[x.dday.isin(days[:h])][col].mean():+.3f}/{x[x.dday.isin(days[h:])][col].mean():+.3f}')
    s = np.sort(v)[::-1]; k = max(1, int(round(len(v) * .1)))
    out.append(f'sum={v.sum():+.2f} sum_wo_top10={s[k:].sum():+.2f}')
    for c in ('coin', 'dday', 'addr'):
        pos = x[x[col] > 0].groupby(c)[col].sum()
        out.append(f'maxshare_{c}={pos.max()/pos.sum():.2f}' if pos.sum() > 0 else f'maxshare_{c}=nan')
    for sd in ('LONG', 'SHORT'):
        y = x[x.our == sd]
        out.append(f'{sd}={len(y)}/{y[col].mean():+.3f}' if len(y) else f'{sd}=0')
    out.append(f'gross={x.gross.mean():+.3f} xalt={x.gross_a.mean():+.3f} xbtc={x.gross_b.mean():+.3f}')
    return ' '.join(out)
