"""H1/H2 TP stack (registered 19.09) — out-of-sample on F1 events since 20.09 00:00 CEST.
Rule: F1 event, tx_pct_of_pos<20, 0<=lag_s<=2; H1 also skips wallets with PRIOR F1 official median < -0.2 (n>=3).
Entry: settlement entry_px_tape at entry_ts_ms (fallback worst-in-3s on bars). Exit: first 1s bar close with gross>=+1.0 or
<=-1.5 -> exit 2 s later worst-in-3s; else at 30 min worst-in-3s. net = gross - costs_pct (funding ~0, see notes)."""
from common import *

T0 = int((pd.Timestamp('2026-09-20 00:00:00') - pd.Timedelta(hours=2)).value // 10**6)  # 20.09 00:00 CEST
CAP = 1800
f, fl = load_follow()
s = load_settle()
sf = s[s.strategy == 'F1_1хв'][['key', 'symbol', 'entry_ts_ms', 'entry_px_tape', 'entry_px_live', 'exit_ts_ms', 'costs_pct',
                                'net_official_pct', 'net_tape_pct', 'funding_pct', 'settled_at', 'status']]

# ---------- wallet history (all F1 trades incl. legacy) ----------
f1_all = pd.concat([f[f.strategy == 'F1_1хв'], fl[fl.strategy == 'F1_1хв']], ignore_index=True)
f1_all = f1_all.merge(sf, left_on='trade_id', right_on='key', how='left')
cl = f1_all['close_ts_ms'].copy()
m = cl.isna()
cl[m] = (pd.to_datetime(f1_all.loc[m, 'date_close']) - pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
f1_all['close_ms'] = cl
f1_all['settled_ms'] = (pd.to_datetime(f1_all.settled_at) - pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
print('settled_ms sample', f1_all.settled_ms.head(2).tolist(), 'close_ms legacy sample', f1_all.close_ms.tail(2).tolist())
hist = f1_all[~f1_all.net_official_pct.isna()][['whale_addr', 'close_ms', 'settled_ms', 'net_official_pct']].copy()
print('history rows with official:', len(hist), 'of', len(f1_all))


def hist_med(addr, t_ms, lag_ms=0, col='close_ms'):
    h = hist[(hist.whale_addr == addr) & (hist[col] < t_ms - lag_ms)]
    return (len(h), float(h.net_official_pct.median()) if len(h) else np.nan)

# ---------- events ----------
ev = f[(f.strategy == 'F1_1хв') & (f.open_ts_ms >= T0)].merge(sf, left_on='trade_id', right_on='key', how='left')
ev = ev[ev.open_ts_ms + (CAP + 10) * 1000 < BARS_END_MS]
print('F1 events since 20.09 CEST with bars horizon:', len(ev))
ev['side'] = np.where(ev.our_side == 'LONG', 1, -1)
ev['day'] = pd.to_datetime(ev.open_ts_ms, unit='ms').dt.strftime('%m-%d')
ev['whale'] = ev.whale_addr
base = ev[(ev.tx_pct_of_pos < 20) & (ev.lag_s >= 0) & (ev.lag_s <= 2)].copy()
print('after tx<20 & 0<=lag<=2:', len(base), ' (lag NaN:', ev.lag_s.isna().sum(), ')')
for c in ('close_ms', 'settled_ms'):
    for lag in (0, 24 * 3600 * 1000):
        r = [hist_med(a, t, lag, c) for a, t in zip(base.whale_addr, base.open_ts_ms)]
        base[f'hn_{c}_{lag}'] = [x[0] for x in r]; base[f'hm_{c}_{lag}'] = [x[1] for x in r]
base['excl'] = (base['hn_close_ms_0'] >= 3) & (base['hm_close_ms_0'] < -0.2)
base['excl_settled'] = (base['hn_settled_ms_0'] >= 3) & (base['hm_settled_ms_0'] < -0.2)
base['excl_24h'] = (base['hn_close_ms_86400000'] >= 3) & (base['hm_close_ms_86400000'] < -0.2)


def run(row, side_mult=1, tp=1.0, sl=1.5, cap=CAP, det=2, shift_s=0, use_settle_entry=True):
    side = int(row.side) * side_mult
    sym = row.symbol if isinstance(row.symbol, str) else SYM.get(row.coin)
    if not isinstance(sym, str):
        return None
    t0 = int(row.entry_ts_ms if not np.isnan(row.entry_ts_ms) else row.open_ts_ms) + shift_s * 1000
    b = bars(sym, t0 - 5000, t0 + (cap + 20) * 1000)
    if b is None:
        return None
    s0 = t0 // 1000
    my_in = worst_px(b, s0, side)
    px_in = row.entry_px_tape if (use_settle_entry and side_mult == 1 and shift_s == 0 and not np.isnan(row.entry_px_tape)) else my_in
    if np.isnan(px_in):
        return None
    px_out, why, xs = sim_tp_sl(b, s0, side, px_in, tp, sl, cap, det)
    if np.isnan(px_out):
        return None
    g = gross(side, px_in, px_out)
    costs = row.costs_pct_y if not np.isnan(row.costs_pct_y) else (row.costs_pct_x if not np.isnan(row.costs_pct_x) else COSTS_OFFICIAL)
    return dict(gross=g, net=g - costs, why=why, hold=xs - s0, my_in=my_in, px_in=px_in, net_c10=g - TAKER_FEES,
                btc=btc_ret(xs * 1000 - 0, s0 * 1000, 0) if False else np.nan)


rows = []
for i, r in base.iterrows():
    o = run(r)
    if o is None:
        rows.append(dict(idx=i)); continue
    o['idx'] = i
    o5 = run(r, det=5); o['net_det5'] = o5['net'] if o5 else np.nan
    om = run(r, side_mult=-1); o['net_mirror'] = om['net'] if om else np.nan
    # BTC over the same holding window in our direction
    t_in = int(r.entry_ts_ms if not np.isnan(r.entry_ts_ms) else r.open_ts_ms); t_out = t_in + int(o['hold']) * 1000
    rb = ret_min('BTCUSDT', t_in, t_out)
    o['btc'] = int(r.side) * rb if not np.isnan(rb) else np.nan
    rows.append(o)
res = pd.DataFrame(rows).set_index('idx')
base = base.join(res)
base['net_btc'] = base.net - base.btc.fillna(0)
base['entry_diff_pct'] = 100 * (base.my_in / base.entry_px_tape - 1) * base.side  # + = my entry worse
print('entry check (my worst-in-3s vs settlement tape): median diff %.4f, |d|<0.01%%: %.0f%%' % (
    base.entry_diff_pct.median(), 100 * (base.entry_diff_pct.abs() < 0.01).mean()))
base.to_csv(OUT + 'h1h2_trades.csv', index=False)

out = []
def rep(label, d, col='net'):
    d = d[~d[col].isna()]
    h = honesty(d, col)
    ndays = d.day.nunique()
    ds = day_stats(d, col)
    days_pos = (ds['mean'] > 0).mean() if len(ds) else np.nan
    pos = d[d[col] > 0].groupby('coin')[col].sum()
    line = (f'{label}: {fmt(h)} | per_day={len(d)/max(ndays,1):.1f} ({ndays} d) days_mean>0={days_pos:.0%} '
            f'max_share_coin={pos.max()/pos.sum() if pos.sum()>0 else float("nan"):.2f} '
            f'P(net>=0.5)={(d[col]>=0.5).mean():.2f}')
    print(line); out.append(line)
    return h

H2 = base[~base.net.isna()]
H1 = H2[~H2.excl]
out.append(f'events F1 since 20.09 CEST (bars horizon): {len(ev)}; base tx<20&lag0-2: {len(base)}; simulated: {len(H2)}')
out.append('--- H1 (registered, wallet exclusion by trades closed before event)')
h1 = rep('H1 net', H1)
rep('H1 net cost0.10', H1, 'net_c10'); rep('H1 net det5s', H1, 'net_det5'); rep('H1 net-BTC', H1, 'net_btc')
rep('H1 mirror (placebo, opposite side)', H1, 'net_mirror')
rep('H1 F1 official same events', H1, 'net_official_pct')
rep('H1 LONG', H1[H1.side > 0]); rep('H1 SHORT', H1[H1.side < 0])
out.append('--- H2 (no wallet exclusion)')
h2 = rep('H2 net', H2)
rep('H2 net cost0.10', H2, 'net_c10'); rep('H2 net det5s', H2, 'net_det5'); rep('H2 net-BTC', H2, 'net_btc')
rep('H2 mirror (placebo)', H2, 'net_mirror')
rep('H2 F1 official same events', H2, 'net_official_pct')
rep('H2 LONG', H2[H2.side > 0]); rep('H2 SHORT', H2[H2.side < 0])
out.append('--- exclusion sensitivity')
rep('H1 excl by settled_at', H2[~H2.excl_settled]); rep('H1 excl lag24h', H2[~H2.excl_24h])
rep('excluded group', H2[H2.excl])
out.append('--- exit reasons H1: ' + str(H1.why.value_counts().to_dict()) + ' mean by reason ' + str(H1.groupby('why').net.mean().round(3).to_dict()))
out.append('--- exit reasons H2: ' + str(H2.why.value_counts().to_dict()) + ' mean by reason ' + str(H2.groupby('why').net.mean().round(3).to_dict()))
out.append('--- per day H1\n' + day_stats(H1, 'net').round(3).to_string())
out.append('--- per day H2\n' + day_stats(H2, 'net').round(3).to_string())
out.append('--- top coins H2\n' + H2.groupby('coin').net.agg(['size', 'mean', 'median', 'sum']).sort_values('size', ascending=False).head(12).round(3).to_string())
# day-cluster CI of mean (criterion)
for lab, d in (('H1', H1), ('H2', H2)):
    lo, hi, p = boot_ci(d.net.values, d.day.values, np.mean)
    out.append(f'{lab} mean CI90 by day [{lo:+.3f},{hi:+.3f}] P(mean<=0)={p:.3f}; wallet-cluster: ' + str(boot_ci(d.net.values, d.whale.values, np.mean)))
    lo, hi, p = boot_ci(d.net.values, d.day.values, np.median)
    out.append(f'{lab} median CI90 by day [{lo:+.3f},{hi:+.3f}] P(median<0.4)=' + f'{np.mean([np.median(np.concatenate([d[d.day==x].net.values for x in np.random.default_rng(k).choice(d.day.unique(), d.day.nunique())])) < 0.4 for k in range(1000)]):.3f}')
    xs = np.sort(d.net.values)[::-1]; k = max(1, int(round(len(xs) * .1)))
    out.append(f'{lab} sum={xs.sum():+.2f} sum_wo_top10={xs[k:].sum():+.2f}')
open(OUT + 'h1h2_out.txt', 'w').write('\n'.join(out) + '\n')
print('\n'.join(out[-30:]))
