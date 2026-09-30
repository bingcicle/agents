"""Extra checks for H1/H2: one-position-per-coin dedup, random-time placebo (same coin/day/side), per-wallet LOO."""
from common import *
CAP = 1800
d = pd.read_csv(OUT + 'h1h2_trades.csv', low_memory=False)
d = d[~d.net.isna()].copy()
d['t_in'] = np.where(d.entry_ts_ms.isna(), d.open_ts_ms, d.entry_ts_ms).astype('int64')
d['t_out'] = d.t_in + d.hold.astype('int64') * 1000
out = []

def dedup(x):
    x = x.sort_values('t_in'); keep = []; busy = {}
    for i, r in x.iterrows():
        if busy.get(r.coin, 0) > r.t_in:
            continue
        keep.append(i); busy[r.coin] = r.t_out
    return x.loc[keep]

for lab, x in (('H2', d), ('H1', d[~d.excl])):
    y = dedup(x)
    h = honesty(y, 'net')
    lo, hi, p = boot_ci(y.net.values, y.day.values, np.mean)
    out.append(f'{lab} one-pos-per-coin: {fmt(h)} | mean CI90 day [{lo:+.3f},{hi:+.3f}]')
    # wallet leave-one-out range of mean
    lo_m = [y[y.whale != w].net.mean() for w in y.whale.unique()]
    out.append(f'{lab} LOO wallet mean range [{min(lo_m):+.3f},{max(lo_m):+.3f}]; LOO coin mean range '
               f'[{min(y[y.coin != c].net.mean() for c in y.coin.unique()):+.3f},{max(y[y.coin != c].net.mean() for c in y.coin.unique()):+.3f}]')

# random-time placebo: same symbol, same UTC day, same side, entry at random second with bars; 3 draws per event
rng = np.random.default_rng(7)
pl = []
for i, r in d.iterrows():
    sym = r.symbol if isinstance(r.symbol, str) else SYM.get(r.coin)
    day0 = int(r.t_in // 86400000 * 86400)
    for k in range(3):
        s0 = int(day0 + rng.integers(0, 86400 - CAP - 30))
        if s0 * 1000 + (CAP + 20) * 1000 >= BARS_END_MS:
            continue
        b = bars(sym, s0 * 1000 - 5000, (s0 + CAP + 20) * 1000)
        if b is None:
            continue
        side = int(r.side)
        pin = worst_px(b, s0, side)
        if np.isnan(pin):
            continue
        po, why, xs = sim_tp_sl(b, s0, side, pin, 1.0, 1.5, CAP, 2)
        if np.isnan(po):
            continue
        pl.append(dict(net=gross(side, pin, po) - (r.costs_pct_y if not np.isnan(r.costs_pct_y) else COSTS_OFFICIAL),
                       why=why, whale=r.whale, day=r.day, coin=r.coin, excl=r.excl))
pl = pd.DataFrame(pl)
pl.to_csv(OUT + 'h1h2_placebo_random.csv', index=False)
for lab, x in (('H2', pl), ('H1', pl[~pl.excl])):
    h = honesty(x, 'net')
    out.append(f'{lab} RANDOM-TIME placebo (same coin/day/side, 3 draws): {fmt(h)} P(net>=0.5)={(x.net>=0.5).mean():.2f} why={x.why.value_counts(normalize=True).round(2).to_dict()}')
open(OUT + 'h1h2_extra_out.txt', 'w').write('\n'.join(out) + '\n')
print('\n'.join(out))
