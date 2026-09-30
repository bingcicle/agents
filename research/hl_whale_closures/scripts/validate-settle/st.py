import numpy as np, pandas as pd

END_LOCAL = pd.Timestamp('2026-09-30 21:07:06')   # snapshot time (CEST)


def boot(values, clusters, stat=np.median, n=3000, seed=1):
    v = np.asarray(values, float)
    cl = np.asarray(clusters)
    if len(v) == 0:
        return (np.nan, np.nan, np.nan)
    u, inv = np.unique(cl, return_inverse=True)
    groups = [v[inv == i] for i in range(len(u))]
    k = len(groups)
    rng = np.random.default_rng(seed)
    out = np.empty(n)
    for b in range(n):
        pick = rng.integers(0, k, k)
        out[b] = stat(np.concatenate([groups[i] for i in pick]))
    return (float(np.percentile(out, 5)), float(np.percentile(out, 95)), float((out <= 0).mean()))


def bat(d, start, col='net', label='', end=END_LOCAL, nboot=3000, full=True):
    """Standard battery on trades frame d (one row per trade of one card). start = window start (local Timestamp)."""
    d = d[d[col].notna()].sort_values('ts_ms')
    days_w = (end - pd.Timestamp(start)).total_seconds() / 86400
    x = d[col].values
    n = len(x)
    r = {'label': label, 'n': n, 'events': d['event'].nunique() if n else 0,
         'wallets': d['wallet'].nunique() if n else 0, 'days': d['day'].nunique() if n else 0,
         'coins': d['coin'].nunique() if n else 0, 'window_days': round(days_w, 2),
         'per_day': round(n / days_w, 2) if days_w > 0 else np.nan}
    if n == 0:
        return r
    r.update(median=float(np.median(x)), mean=float(x.mean()), win=float((x > 0).mean()), sum_pp=float(x.sum()),
             usd_1000=float(x.sum() * 10), usd_month=float(x.sum() * 10 / days_w * 30))
    xs = np.sort(x)[::-1]
    k = max(1, int(round(n * 0.1)))
    r['sum_wo_top10'] = float(xs[k:].sum())
    if full and n >= 3:
        for c in ('wallet', 'day', 'coin'):
            lo, hi, p = boot(x, d[c].values, np.median, nboot)
            r[f'med_ci90_{c}'] = (round(lo, 3), round(hi, 3))
            lo, hi, p = boot(x, d[c].values, np.mean, nboot)
            r[f'mean_ci90_{c}'] = (round(lo, 3), round(hi, 3))
            r[f'P_mean_le0_{c}'] = round(p, 3)
    h = n // 2
    if n >= 2:
        r['half1'] = (round(float(np.median(x[:h])), 3), round(float(x[:h].mean()), 3), h)
        r['half2'] = (round(float(np.median(x[h:])), 3), round(float(x[h:].mean()), 3), n - h)
    tot = x.sum()
    for c in ('wallet', 'day', 'coin'):
        g = d.groupby(c)[col].sum()
        r[f'max_share_sum_{c}'] = round(float(g.max() / tot), 3) if tot > 0 else np.nan
        r[f'top_{c}'] = (str(g.idxmax())[:10], round(float(g.max()), 2))
        r[f'max_share_n_{c}'] = round(float(d[c].value_counts().iloc[0] / n), 3)
    for sd in ('LONG', 'SHORT'):
        y = d[d['side'] == sd][col].values
        r[sd] = (len(y), round(float(np.median(y)), 3) if len(y) else np.nan, round(float(y.mean()), 3) if len(y) else np.nan,
                 round(float(y.sum()), 2) if len(y) else 0.0)
    return r


def fmt(r):
    keys = ['label', 'n', 'events', 'wallets', 'days', 'coins', 'per_day', 'median', 'mean', 'win', 'sum_pp', 'usd_1000',
            'usd_month', 'sum_wo_top10']
    s = ' '.join(f'{k}={r[k]:+.3f}' if isinstance(r.get(k), float) else f'{k}={r.get(k)}' for k in keys if k in r)
    rest = {k: v for k, v in r.items() if k not in keys}
    return s + '\n    ' + '; '.join(f'{k}={v}' for k, v in rest.items())
