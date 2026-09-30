"""Build per-TWAP table with Binance k1m returns (in TWAP direction), BTC/alt-index controls, intensity."""
from load import *
import os, time
t0 = time.time()
M = 60000
d = load_twaps()
END_BN = pd.Timestamp('2026-09-30').value // 10**6   # Binance data ends 29.09 23:59 UTC
d = d[d.sym.notna()].copy()
# ---- alt index: equal-weight mean of 1m log returns over all symbols except BTC
syms = sorted(os.listdir(DATA + 'k1m'))
g0 = pd.Timestamp('2026-08-20').value // 10**6; g1 = END_BN
grid = np.arange(g0, g1, M)
acc = np.zeros(len(grid)); cnt = np.zeros(len(grid))
for s in syms:
    if s == 'BTCUSDT':
        continue
    k = kl(s)
    if k is None or len(k) < 100:
        continue
    c = k.c.reindex(grid).ffill().values
    lr = np.diff(np.log(c), prepend=np.nan)
    ok = np.isfinite(lr) & (np.abs(lr) < 0.3)
    acc[ok] += lr[ok]; cnt[ok] += 1
alt_lr = np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0)
alt_cum = np.cumsum(alt_lr)              # log index; value at grid i = close of candle opened at grid[i]
np.savez(f'alt_index.npz', grid=grid, cum=alt_cum, cnt=cnt)
print('alt index built', time.time() - t0, 'median n syms', np.median(cnt))

def pfun(ot, c, T, stale=5):
    """last price at minute boundary T (close of candle opened at T-60s); NaN if stale > stale min"""
    T = np.asarray(T, dtype='int64')
    i = np.searchsorted(ot, T - M, side='right') - 1
    ok = (i >= 0) & (T <= END_BN)
    ii = np.clip(i, 0, len(ot) - 1)
    ok &= (ot[ii] >= T - M - stale * M)
    return np.where(ok, c[ii], np.nan)

def palt(T):
    T = np.asarray(T, dtype='int64')
    i = (T - M - g0) // M
    ok = (i >= 0) & (i < len(grid))
    return np.where(ok, np.exp(alt_cum[np.clip(i, 0, len(grid) - 1)]), np.nan)

btc = kl('BTCUSDT'); b_ot = btc.index.values.astype('int64'); b_c = btc.c.values
ceil = lambda x: (np.ceil(np.asarray(x, float) / M) * M).astype('int64')
floor = lambda x: (np.floor(np.asarray(x, float) / M) * M).astype('int64')
d['T_start'] = floor(d.start_ms)                 # price just before start
d['T_dec'] = ceil(d.date_ms)                     # first minute boundary after the bot knew
d['T_e1'] = ceil(d.date_ms + M)                  # entry at date + 1 min
d['T_end'] = ceil(d.end_ms)
d['T_mid'] = ceil(d.start_ms + d.dur_s * 500)
times = {'pre60': d.T_start - 60 * M, 'pre15': d.T_start - 15 * M, 'start': d.T_start, 'dec': d.T_dec, 'e1': d.T_e1,
         'mid': d.T_mid, 'end': d.T_end}
for h in (5, 15, 30, 60, 120, 240):
    times[f'e1_{h}'] = d.T_e1 + h * M
for h in (15, 30, 60, 120):
    times[f'end_{h}'] = d.T_end + h * M
out = {k: np.full(len(d), np.nan) for k in times}
outb = {k: np.full(len(d), np.nan) for k in times}
outa = {k: palt(v.values) for k, v in times.items()}
for k, v in times.items():
    outb[k] = pfun(b_ot, b_c, v.values)
vol24 = np.full(len(d), np.nan); sig24 = np.full(len(d), np.nan); vol1h = np.full(len(d), np.nan)
d = d.reset_index(drop=True)
for s, idx in d.groupby('sym').groups.items():
    k = kl(s)
    if k is None:
        continue
    ot = k.index.values.astype('int64'); c = k.c.values; qv = k.qv.values
    cq = np.concatenate([[0], np.cumsum(qv)])
    lr = np.diff(np.log(c), prepend=np.nan); lr[0] = 0; lr = np.nan_to_num(lr)
    c1 = np.concatenate([[0], np.cumsum(lr)]); c2 = np.concatenate([[0], np.cumsum(lr ** 2)])
    idx = np.asarray(idx)
    for kk, v in times.items():
        out[kk][idx] = pfun(ot, c, v.values[idx])
    Ts = d.T_start.values[idx]
    a = np.searchsorted(ot, Ts - 1440 * M); b = np.searchsorted(ot, Ts)
    n = b - a
    vol24[idx] = np.where(n > 600, (cq[b] - cq[a]) / np.maximum(n, 1), np.nan)
    m1 = (c1[b] - c1[a]) / np.maximum(n, 1); m2 = (c2[b] - c2[a]) / np.maximum(n, 1)
    sig24[idx] = np.where(n > 600, 100 * np.sqrt(np.maximum(m2 - m1 ** 2, 0)), np.nan)
    a1 = np.searchsorted(ot, Ts - 60 * M)
    vol1h[idx] = np.where(b - a1 > 30, (cq[b] - cq[a1]) / np.maximum(b - a1, 1), np.nan)
for k in times:
    d['p_' + k] = out[k]; d['b_' + k] = outb[k]; d['a_' + k] = outa[k]
d['vol24pm'] = vol24; d['sig1m'] = sig24; d['vol1hpm'] = vol1h
d['flow_pm'] = d.usd / (d.dur_s / 60)
d['intensity'] = d.flow_pm / d.vol24pm
d['size_vs_day'] = d.usd / (d.vol24pm * 1440)
d.to_parquet('twaps_px.parquet')
print('done', time.time() - t0, d.shape, 'with p_start', d.p_start.notna().sum(), 'p_end', d.p_end.notna().sum())
