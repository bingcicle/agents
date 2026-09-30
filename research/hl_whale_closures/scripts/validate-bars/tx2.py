"""tx2 rules registered 22.09 — rebuild on NEW F1 anchors (open >= 22.09 00:00 UTC) from whale_txs + 1s bars.
tx2 = FIRST taker (liq==0) close by the same wallet+coin+side after the trigger, closing >= 5% of the position at that moment
(|sp|) and >= $5k (registered do_now: both txs pass bot gates), within 62 min. ratio2 = |sp2|*px2 / depth(tx1), depth = pos_usd/ratio.
Rule A: gap <= 40 s & ratio2 >= 5.  Rule B: ratio2 >= 10 (any gap <= 62 min).
Entry: second ceil((t2+1400)/1000), worst-in-3s. Exit E1: 60 s silence since whale's last taker close on that side (+2 s)
or whale position <= 5% of pos_trig (+3 s), cap 60 min from tx2; worst-in-3s. net = gross - costs_pct(row) - funding(~0)."""
from common import *
import sys
T0 = int(pd.Timestamp(sys.argv[1] if len(sys.argv) > 1 else '2026-09-22 00:00:00').value // 10**6)
TAG = sys.argv[2] if len(sys.argv) > 2 else 'main'
LAT = 1400

f, fl = load_follow()
s = load_settle()
sf = s[s.strategy == 'F1_1хв'][['key', 'symbol', 'entry_ts_ms', 'entry_px_tape', 'costs_pct', 'net_official_pct']]
ev = f[(f.strategy == 'F1_1хв') & (f.open_ts_ms >= T0) & (f.open_ts_ms < BARS_END_MS)].merge(sf, left_on='trade_id', right_on='key', how='left')
n_all = len(ev)
ev = ev[(ev.tx_pct_of_pos >= 5) & (ev.tx_usd >= 5000) & (ev.ratio >= 2)].copy()
print(f'F1 anchors since {pd.Timestamp(T0, unit="ms")}: {n_all}; bot-qualifying (>=5%, >=$5k, ratio>=2): {len(ev)}')
ev['side'] = np.where(ev.our_side == 'LONG', 1, -1)
ev['wside'] = np.where(ev.our_side == 'LONG', 'SHORT', 'LONG')  # we follow the closing trade
ev['depth1'] = ev.pos_usd / ev.ratio
ev['day'] = pd.to_datetime(ev.open_ts_ms, unit='ms').dt.strftime('%m-%d')
ev['whale'] = ev.whale_addr

w = pd.read_parquet(DATA + 'whale_txs.parquet', columns=['addr', 'coin', 'wside', 'h', 'ts', 'px', 'sz', 'sp', 'liq', 'bot_ts', 'detect_src'])
w = w[w.addr.isin(ev.whale_addr.unique()) & (w.ts >= T0 - 3600_000)]
cb = pd.read_parquet(DATA + 'close_batches.parquet', columns=['addr', 'coin', 'wside', 'new_size', 'full_close', 'last_tx_ms', 'snap_ms'])
cb = cb[cb.addr.isin(ev.whale_addr.unique()) & (cb.last_tx_ms >= T0 - 3600_000)]
gw = {k: g.sort_values('ts') for k, g in w.groupby(['addr', 'coin', 'wside'])}
gb = {k: g.sort_values('last_tx_ms') for k, g in cb.groupby(['addr', 'coin', 'wside'])}

rows = []
for i, r in ev.iterrows():
    key = (r.whale_addr, r.coin, r.wside)
    g = gw.get(key)
    o = dict(idx=i)
    if g is None:
        o['status'] = 'no_txs'; rows.append(o); continue
    hp = str(r.trig_hash)[:18]
    trig = g[g.h == hp]
    if len(trig):
        t1, sp1 = int(trig.ts.iloc[0]), abs(float(trig.sp.iloc[0]))
    else:
        t1, sp1 = int(r.fill_ts_ms), np.nan
    if np.isnan(sp1):
        sp1 = r.pos_usd / r.entry_px
    o.update(t1=t1, pos_trig=sp1)
    cand = g[(g.h != hp) & (g.liq == 0) & (g.ts <= t1 + 62 * 60_000) &
             ((g.ts > t1) | ((g.ts == t1) & (g.sp.abs() < sp1 - 1e-9)))].copy()
    cand['closed'] = np.minimum(cand.sz, cand.sp.abs())
    cand['pct'] = 100 * cand.closed / cand.sp.abs()
    cand['usd'] = cand.closed * cand.px
    q5 = cand[cand.pct >= 5]
    q = q5[q5.usd >= 5000]
    o['n_qual'] = len(q)
    for lab, qq in (('', q), ('_p5', q5)):
        if len(qq) == 0:
            continue
        t2 = qq.iloc[0]
        o['t2' + lab] = int(t2.ts); o['gap' + lab] = (int(t2.ts) - t1) / 1000
        o['ratio2' + lab] = abs(t2.sp) * t2.px / r.depth1
        o['tx2pct' + lab] = t2.pct; o['tx2usd' + lab] = t2.usd; o['bot_ts2' + lab] = t2.bot_ts * 1000; o['src2' + lab] = t2.detect_src
    rows.append(o)
res = pd.DataFrame(rows).set_index('idx')
ev = ev.join(res)


def exit_e1(r, t2):
    """Exit decision ms by E1 anchored at t2 (tx2 time)."""
    key = (r.whale_addr, r.coin, r.wside)
    g = gw.get(key)
    cap = t2 + 3600_000
    later = g[(g.ts > t2) & (g.ts <= cap) & (g.liq == 0)] if g is not None else g
    last = t2; ex, why = None, None
    fc_ms = None
    # near-full close by tx-level position after each taker close
    if later is not None and len(later):
        pos_after = (later.sp.abs() - np.minimum(later.sz, later.sp.abs())).values
        fcm = later.ts.values[pos_after <= 0.05 * r.pos_trig]
        if len(fcm):
            fc_ms = int(fcm[0])
    b = gb.get(key)
    if b is not None:
        bb = b[(b.last_tx_ms >= t2) & (b.last_tx_ms <= cap) & ((b.new_size.abs() <= 0.05 * r.pos_trig) | b.full_close)]
        if len(bb):
            fc_ms = min(fc_ms, int(bb.last_tx_ms.iloc[0])) if fc_ms else int(bb.last_tx_ms.iloc[0])
    times = later.ts.values if later is not None else np.array([])
    for tc in list(times) + [cap + 10**9]:
        if fc_ms is not None and fc_ms <= min(tc, last + 60_000):
            return fc_ms + 3000, 'full_close'
        if tc - last > 60_000:
            ex = last + 60_000 + 2000
            return (min(ex, cap), 'silence') if ex <= cap else (cap, 'cap')
        last = tc
    return cap, 'cap'


def trade(r, t2, lat=LAT, entry_ms=None):
    sym = r.symbol if isinstance(r.symbol, str) else SYM.get(r.coin)
    if not isinstance(sym, str):
        return None
    side = int(r.side)
    xm, why = exit_e1(r, t2)
    em = entry_ms if entry_ms is not None else t2 + lat
    se = int(math.ceil(em / 1000)); sx = int(math.ceil(xm / 1000))
    if sx <= se:
        sx = se + 1
    if (sx + 12) * 1000 >= BARS_END_MS:
        return None
    b = bars(sym, (se - 5) * 1000, (sx + 12) * 1000)
    if b is None:
        return None
    pin, pout = worst_px(b, se, side), worst_px(b, sx, -side)
    if np.isnan(pin) or np.isnan(pout):
        return None
    costs = r.costs_pct_y if not np.isnan(r.costs_pct_y) else (r.costs_pct_x if not np.isnan(r.costs_pct_x) else COSTS_OFFICIAL)
    g = gross(side, pin, pout)
    # control: entry at tx1 (settlement tape entry), exit at the same second
    ctrl = np.nan
    if not np.isnan(r.entry_px_tape):
        ctrl = gross(side, r.entry_px_tape, pout) - costs
    rb = ret_min('BTCUSDT', se * 1000, sx * 1000)
    return dict(net=g - costs, gross=g, why=why, hold=sx - se, t_in=se * 1000, t_out=sx * 1000, net_tx1_same_exit=ctrl,
                btc=side * rb if not np.isnan(rb) else np.nan)


out = []
out.append(f'anchors={len(ev)} with_tx2={ev.t2.notna().sum()} with_tx2(5% only)={ev.t2_p5.notna().sum()}')
allres = []
for rule in ('A', 'B'):
    for var in ('main', 'p5', 'botts', 'lat0', 'lat3'):
        sfx = '_p5' if var == 'p5' else ''
        if rule == 'A':
            sel = ev[(ev['gap' + sfx] <= 40) & (ev['ratio2' + sfx] >= 5)]
        else:
            sel = ev[ev['ratio2' + sfx] >= 10]
        tr = []
        for i, r in sel.iterrows():
            t2 = int(r['t2' + sfx])
            if var == 'botts':
                o = trade(r, t2, entry_ms=max(t2 + LAT, r['bot_ts2']))
            elif var == 'lat0':
                o = trade(r, t2, lat=0)
            elif var == 'lat3':
                o = trade(r, t2, lat=3000)
            else:
                o = trade(r, t2)
            if o is None:
                continue
            o.update(idx=i, rule=rule, var=var, coin=r.coin, whale=r.whale, day=r.day, ratio2=r['ratio2' + sfx],
                     gap=r['gap' + sfx], side=r.side, f1_official=r.net_official_pct, ratio1=r.ratio, src2=r['src2' + sfx],
                     lag_bot2=(r['bot_ts2' + sfx] - t2) / 1000)
            tr.append(o)
        tr = pd.DataFrame(tr)
        if len(tr) == 0:
            out.append(f'rule {rule} {var}: n=0'); continue
        # one position per coin (bot constraint)
        tr = tr.sort_values('t_in'); keep = []; busy = {}
        for j, x in tr.iterrows():
            if busy.get(x.coin, 0) > x.t_in:
                continue
            keep.append(j); busy[x.coin] = x.t_out
        tr['dedup_keep'] = tr.index.isin(keep)
        allres.append(tr)
        d = tr[tr.dedup_keep]
        h = honesty(d, 'net')
        ndays = (min(BARS_END_MS, int(d.t_in.max()) + 1) - T0) / 86400000
        lo, hi, p = boot_ci(d.net.values, d.whale.values, np.median, n=3000)
        wo10 = d[d.ratio2 <= 10]; wo3 = d[~d.coin.isin(['PONS', 'VVV', 'CHIP'])]
        line = (f'rule {rule} [{var}] (dedup n={len(d)}, raw {len(tr)}): {fmt(h)}\n   per_day={len(d)/ndays:.2f} '
                f'median CI90 wallet [{lo:+.3f},{hi:+.3f}] P(med<=0)_wallet={p:.3f} | '
                f'wo ratio2>10: n={len(wo10)} med={wo10.net.median() if len(wo10) else float("nan"):+.3f} mean={wo10.net.mean() if len(wo10) else float("nan"):+.3f} | '
                f'wo PONS/VVV/CHIP: n={len(wo3)} med={wo3.net.median() if len(wo3) else float("nan"):+.3f} mean={wo3.net.mean() if len(wo3) else float("nan"):+.3f} | '
                f'ctrl entry@tx1 same exit: med={d.net_tx1_same_exit.median():+.3f} mean={d.net_tx1_same_exit.mean():+.3f} | '
                f'F1 official same events med={d.f1_official.median():+.3f} mean={d.f1_official.mean():+.3f} | '
                f'net-BTC mean={(d.net-d.btc.fillna(0)).mean():+.3f} | LONG n={(d.side>0).sum()} mean={d[d.side>0].net.mean():+.3f} SHORT n={(d.side<0).sum()} mean={d[d.side<0].net.mean():+.3f} | '
                f'why={d.why.value_counts().to_dict()} coins={d.coin.value_counts().head(6).to_dict()} wallets={d.whale.nunique()}')
        print(line); out.append(line)
pd.concat(allres).to_csv(OUT + f'tx2_trades_{TAG}.csv', index=False)
ev.to_csv(OUT + f'tx2_anchors_{TAG}.csv', index=False)
# benchmark: all anchors F1 official, and F1 with ratio(tx1)>=12 (control named in the 22.09 report)
out.append(f'F1 official all anchors: n={ev.net_official_pct.notna().sum()} med={ev.net_official_pct.median():+.3f} mean={ev.net_official_pct.mean():+.3f}')
r12 = ev[ev.ratio >= 12]
out.append(f'control F1 ratio(tx1)>=12: n={len(r12)} med={r12.net_official_pct.median():+.3f} mean={r12.net_official_pct.mean():+.3f}')
out.append('ratio2 distribution of first tx2 (gap<=40): ' + str(ev[ev.gap <= 40].ratio2.describe().round(2).to_dict()))
out.append('gap distribution: ' + str(ev.gap.describe().round(1).to_dict()))
open(OUT + f'tx2_out_{TAG}.txt', 'w').write('\n'.join(out) + '\n')
print('\n'.join(out[-4:]))
