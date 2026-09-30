"""REV part 2: placebos (random time; dump WITHOUT whale) and P2 = own episode reconstruction from whale_txs."""
from rev import *
out = []
rng = np.random.default_rng(11)
allsig = pd.read_csv(OUT + 'rev_p1_all.csv')   # all bot rev signals after in-sample (any move) with trades
dd = pd.read_csv(OUT + 'rev_p1_dedup.csv')     # registered set, dedup
rs = pd.read_csv(BOT + 'rev_signals.csv', low_memory=False); rs = rs[rs.eol == '^']
rs['det_ms'] = (rs.sig_id.str.split('-').str[0].astype('int64') + rs.lag_s * 1000).astype('int64')
sig_times = {c: np.sort(g.det_ms.values) for c, g in rs.groupby('coin')}
w = pd.read_parquet(DATA + 'whale_txs.parquet', columns=['addr', 'coin', 'wside', 'ts', 'px', 'sz', 'sp', 'liq', 'bot_ts', 'detect_src', 'tx_usd'])
w = w[w.ts >= IS_END - 3600_000]
big_tx_times = {c: np.sort(g[g.tx_usd >= 10_000].ts.values) for c, g in w.groupby('coin')}


def near(arr, t, before, after):
    if arr is None or len(arr) == 0:
        return False
    i = np.searchsorted(arr, t - before)
    return i < len(arr) and arr[i] <= t + after

# ---------- placebo A: random times, same coin / UTC day / side, >=90 min from any rev signal on the coin ----------
pa = []
for _, x in dd.iterrows():
    sym = SYM.get(x.coin); day0 = int(x.t_in // 86400000 * 86400)
    k = 0; tries = 0
    while k < 5 and tries < 60:
        tries += 1
        s0 = int(day0 + rng.integers(200, 86400 - 200))
        if near(sig_times.get(x.coin), s0 * 1000, 90 * 60_000, 90 * 60_000):
            continue
        a = rev_trade(sym, s0 * 1000, int(x.side), COSTS_OFFICIAL)
        if a is None:
            continue
        a.update(coin=x.coin, whale=x.whale, day=x.day, side=x.side); pa.append(a); k += 1
pa = pd.DataFrame(pa)
report('PLACEBO random time (same coin/day/side, 5 per signal)', pa, out)

# ---------- placebo B: dump WITHOUT whale: Binance 3-min move >=1.5% on the same coin-days, no rev signal +-90 min,
# no whale close tx >= $10k in the prior 10 min; first second of each run, then 60 min lockout; entry at +2 s ----------
pb = []
cd = allsig[['coin', 'day']].drop_duplicates()
for _, x in cd.iterrows():
    sym = SYM.get(x.coin)
    d0 = int(pd.Timestamp('2026-' + x.day).value // 10**6)
    b = bars(sym, d0, d0 + 86399_000)
    if b is None:
        continue
    cf = b.cf.values; idx = b.index.values
    mv = np.full(len(cf), np.nan); mv[180:] = 100 * (cf[180:] / cf[:-180] - 1)
    for dump_sign, side in ((-1, 1), (1, -1)):  # price down -> fade LONG ; price up -> fade SHORT
        cond = np.where(dump_sign * mv >= 1.5)[0]
        last = -10**12
        for j in cond:
            t = int(idx[j])
            if t < last + 3600:
                continue
            tm = t * 1000
            if near(sig_times.get(x.coin), tm, 90 * 60_000, 90 * 60_000) or near(big_tx_times.get(x.coin), tm, 600_000, 0):
                continue
            last = t
            a = rev_trade(sym, tm + 2000, side, COSTS_OFFICIAL)
            if a is None:
                continue
            a.update(coin=x.coin, whale='none', day=x.day, side=side); pb.append(a)
pb = pd.DataFrame(pb)
report('PLACEBO dump WITHOUT whale (same coin-days, move3>=1.5)', pb, out)
report('PLACEBO dump WITHOUT whale, one-pos-per-coin', dedup(pb), out)
report('PLACEBO dump WITHOUT whale LONG only (fade of dumps)', pb[pb.side > 0], out)
pa.to_csv(OUT + 'rev_placebo_random.csv', index=False); pb.to_csv(OUT + 'rev_placebo_nowhale.csv', index=False)

# ---------- P2: own close-episode reconstruction from whale_txs ----------
wt = w[w.liq == 0].sort_values(['addr', 'coin', 'wside', 'ts']).copy()
wt['closed'] = np.minimum(wt.sz, wt.sp.abs())
key = wt.addr + '|' + wt.coin + '|' + wt.wside
gap = wt.ts.diff().fillna(10**12)
newep = (key != key.shift()) | (gap > 60_000)
wt['ep'] = newep.cumsum()
g = wt.groupby('ep')
wt['P0'] = g.sp.transform(lambda s: abs(s.iloc[0]))
wt['cum'] = g.closed.cumsum()
wt['cum_usd'] = (wt.closed * wt.px).groupby(wt.ep).cumsum()
cross = wt[(wt.cum >= 0.3 * wt.P0)].groupby('ep').head(1)
cross = cross[cross.cum_usd >= 38_000]
out.append(f'P2 episodes crossing 30% with closed >= $38k since in-sample end: {len(cross)} (detect_src {cross.detect_src.value_counts().to_dict()})')
rows = []
for _, x in cross.iterrows():
    sym = SYM.get(x.coin)
    if not isinstance(sym, str):
        continue
    side = 1 if x.wside == 'LONG' else -1   # fade: whale closes LONG (sells) -> we buy
    for mode in ('bot', 'ideal'):
        det = max(int(x.ts) + 1500, int(x.bot_ts * 1000)) if mode == 'bot' else int(x.ts) + 1500
        se = int(math.ceil(det / 1000))
        if (se + H + 15) * 1000 >= BARS_END_MS:
            continue
        b = bars(sym, (se - 200) * 1000, se * 1000)
        if b is None:
            continue
        m3 = move3(b, se, -side)
        if np.isnan(m3) or m3 < 1.5:
            continue
        a = rev_trade(sym, det, side, COSTS_OFFICIAL)
        if a is None:
            continue
        a.update(mode=mode, coin=x.coin, whale=x.addr, side=side, m3=m3, lag=(det - x.ts) / 1000, src=x.detect_src,
                 full=bool(x.cum >= 0.95 * x.P0), day=pd.Timestamp(se, unit='s').strftime('%m-%d'),
                 bot_sig=near(sig_times.get(x.coin), det, 300_000, 300_000))
        rows.append(a)
p2 = pd.DataFrame(rows)
p2.to_csv(OUT + 'rev_p2_all.csv', index=False)
for mode in ('bot', 'ideal'):
    d = p2[p2['mode'] == mode]
    report(f'P2 [{mode} detection] raw', d, out)
    dd2 = dedup(d)
    report(f'P2 [{mode} detection] one-pos-per-coin', dd2, out)
    report(f'P2 [{mode}] dedup TEST 22.09-29.09', dd2[dd2.t_in >= TEST0], out)
    report(f'P2 [{mode}] dedup 13.09-21.09', dd2[dd2.t_in < TEST0], out)
    report(f'P2 [{mode}] dedup NOT near a bot rev signal', dd2[~dd2.bot_sig], out)
    report(f'P2 [{mode}] dedup full close only', dd2[dd2.full], out)
    if mode == 'bot':
        out.append('P2 bot dedup per day:\n' + day_stats(dd2, 'net').round(3).to_string())
open(OUT + 'rev_out_p2.txt', 'w').write('\n'.join(out) + '\n')
