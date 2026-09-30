"""Rebuild per-TWAP price table with corrected decision semantics.
For ELIGIBLE TWAPs (<=15 min) the CSV 'date' is the bot's evaluation time (~90% of duration), not first-seen.
first-seen estimate: ineligible -> date; eligible -> min(date, start + 90 s)  (q85 of observed first-seen lags is ~90 s)."""
from load import *
import time
M = 60000
END_BN = pd.Timestamp('2026-09-30').value // 10**6
z = np.load('alt_index.npz'); grid = z['grid']; alt_cum = z['cum']; g0 = grid[0]
def pfun(ot, c, T, stale=5):
    T = np.asarray(T, dtype='int64')
    i = np.searchsorted(ot, T - M, side='right') - 1
    ok = (i >= 0) & (T <= END_BN)
    ii = np.clip(i, 0, len(ot) - 1)
    ok &= (ot[ii] >= T - M - stale * M)
    return np.where(ok, c[ii], np.nan)
def palt(T):
    T = np.asarray(T, dtype='int64'); i = (T - M - g0) // M
    ok = (i >= 0) & (i < len(grid))
    return np.where(ok, np.exp(alt_cum[np.clip(i, 0, len(grid) - 1)]), np.nan)
ceil = lambda x: (np.ceil(np.asarray(x, float) / M) * M).astype('int64')
floor = lambda x: (np.floor(np.asarray(x, float) / M) * M).astype('int64')
def add_prices(d, times):
    btc = kl('BTCUSDT'); b_ot = btc.index.values.astype('int64'); b_c = btc.c.values
    for k, v in times.items():
        d['p_' + k] = np.nan; d['b_' + k] = pfun(b_ot, b_c, np.asarray(v)); d['a_' + k] = palt(np.asarray(v))
    for s, idx in d.groupby('sym').groups.items():
        k = kl(s)
        if k is None: continue
        ot = k.index.values.astype('int64'); c = k.c.values; idx = np.asarray(idx)
        for kk, v in times.items():
            d.loc[idx, 'p_' + kk] = pfun(ot, c, np.asarray(v)[idx])
    return d
if __name__ == '__main__':
    t0 = time.time()
    old = pd.read_parquet('twaps_px.parquet')
    keep = [c for c in old.columns if not c.startswith(('p_', 'b_', 'a_', 'T_'))]
    d = old[keep].copy().reset_index(drop=True)
    d['seen_ms'] = np.where(d.eligible == 1, np.minimum(d.date_ms, d.start_ms + 90000), d.date_ms)
    d['T_start'] = floor(d.start_ms); d['T_e1'] = ceil(d.seen_ms + M); d['T_dec'] = ceil(d.date_ms)
    d['T_end'] = ceil(d.end_ms); d['T_mid'] = ceil(d.start_ms + d.dur_s * 500)
    times = {'pre60': d.T_start - 60 * M, 'pre15': d.T_start - 15 * M, 'start': d.T_start, 'e1': d.T_e1, 'dec': d.T_dec,
             'mid': d.T_mid, 'end': d.T_end}
    for h in (5, 15, 30, 60, 120, 240):
        times[f'e1_{h}'] = d.T_e1 + h * M
    for h in (5, 15, 30, 60, 120):
        times[f'dec_{h}'] = d.T_dec + h * M
    for h in (5, 15, 30, 60, 120, 240):
        times[f'end_{h}'] = d.T_end + h * M
    for f in (1, 2, 3):
        times[f'q{f}'] = ceil(d.start_ms + d.dur_s * 1000 * f / 4)
    times = {k: np.asarray(v, dtype='int64') for k, v in times.items()}
    d = add_prices(d, times)
    for k, v in times.items():
        d['T_' + k] = v
    d.to_parquet('twaps_px2.parquet')
    print('done', time.time() - t0, d.shape)
