"""REV after big fast dump (feasibility 13.09): close episode (full or partial >=30%), Binance 3-min move in the whale's
closing direction >= 1.5% at detection -> enter AGAINST (fade) at detection worst-in-3s; TP +3% gross on the first trade
touching the level (searched from entry+3 s) -> exit 2 s later worst-in-3s; else exit at 60 min worst-in-3s.
Population P1 = bot rev_signals (the registered stream) after the in-sample end (13.09 07:00 UTC).
Population P2 = own reconstruction of close episodes from whale_txs (see rev_episodes())."""
from common import *
IS_END = int(pd.Timestamp('2026-09-13 07:00:00').value // 10**6)
TEST0 = int(pd.Timestamp('2026-09-22 00:00:00').value // 10**6)
H = 3600
TP = 3.0


def to_ms(s):
    return (pd.to_datetime(s) - pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')


def move3(b, sec, wdir):
    """% move over the 180 s before `sec` in the whale's closing trade direction (wdir=+1 whale buys)."""
    p0, p1 = last_px(b, sec - 180), last_px(b, sec)
    return wdir * 100 * (p1 / p0 - 1)


def rev_trade(sym, det_ms, side, costs, tp=TP, h=H, tp_mode='touch'):
    se = int(math.ceil(det_ms / 1000))
    if (se + h + 15) * 1000 >= BARS_END_MS:
        return None
    b = bars(sym, (se - 200) * 1000, (se + h + 15) * 1000)
    if b is None:
        return None
    pin = worst_px(b, se, side)
    if np.isnan(pin):
        return None
    seg = b.loc[se + 3: se + h]
    if tp_mode == 'touch':
        x = seg.h.values if side > 0 else seg.l.values
    else:  # 'close': level must hold on the 1s close
        x = seg.c.values
    lvl = pin * (1 + tp / 100) if side > 0 else pin * (1 - tp / 100)
    hit = np.where(x >= lvl)[0] if side > 0 else np.where(x <= lvl)[0]
    if len(hit) and tp < 99:
        ts = int(seg.index[hit[0]]); xs = ts + 2; why = 'tp'
    else:
        xs = se + h; why = 'm60'
    pout = worst_px(b, xs, -side)
    if np.isnan(pout):
        return None
    g = gross(side, pin, pout)
    m3 = move3(b, se, -side)  # whale direction = opposite of our fade side
    rb = ret_min('BTCUSDT', se * 1000, xs * 1000)
    return dict(net=g - costs, gross=g, why=why, hold=xs - se, t_in=se * 1000, t_out=xs * 1000, m3_bars=m3,
                btc=side * rb if not np.isnan(rb) else np.nan)


def dedup(x):
    x = x.sort_values('t_in'); keep = []; busy = {}
    for i, r in x.iterrows():
        if busy.get(r.coin, 0) > r.t_in:
            continue
        keep.append(i); busy[r.coin] = r.t_out
    return x.loc[keep]


def report(lab, d, out, col='net'):
    d = d[~d[col].isna()]
    if len(d) == 0:
        out.append(f'{lab}: n=0'); print(out[-1]); return
    h = honesty(d, col)
    nd = max(1e-9, (min(BARS_END_MS, d.t_in.max()) - d.t_in.min()) / 86400000)
    out.append(f'{lab}: {fmt(h)} | TP%={100*(d.why=="tp").mean():.0f} per_day~{len(d)/max(nd,1):.2f} '
               f'LONG n={(d.side>0).sum()} mean={d[d.side>0][col].mean():+.3f} SHORT n={(d.side<0).sum()} mean={d[d.side<0][col].mean():+.3f} '
               f'net-BTC mean={(d[col]-d.btc.fillna(0)).mean():+.3f} coins={d.coin.value_counts().head(5).to_dict()} wallets={d.whale.nunique()}')
    print(out[-1])


if __name__ == '__main__':
    out = []
    # ---------------- P1: bot rev_signals ----------------
    r = pd.read_csv(BOT + 'rev_signals.csv', low_memory=False); r = r[r.eol == '^']
    o = pd.read_csv(BOT + 'rev_outcomes.csv', low_memory=False)
    o = o[o.eol == '^'] if 'eol' in o else o
    r = r.merge(o[['sig_id', 'costs_pct']].drop_duplicates('sig_id'), on='sig_id', how='left')
    r['sig_ms'] = r.sig_id.str.split('-').str[0].astype('int64')
    r['det_ms'] = (r.sig_ms + r.lag_s * 1000).round().astype('int64')
    r['date_ms'] = to_ms(r.date)
    out.append('det_ms - date_ms (s): ' + str(((r.det_ms - r.date_ms) / 1000).describe().round(2).to_dict()))
    r['side'] = np.where(r.fade_side == 'LONG', 1, -1)
    r['whale'] = r.whale_addr
    r['day'] = pd.to_datetime(r.det_ms, unit='ms').dt.strftime('%m-%d')
    r['symbol'] = r.coin.map(SYM)
    r = r[(r.det_ms >= IS_END)]
    out.append(f'rev_signals after in-sample: {len(r)}; move_3m>=1.5: {(r.move_3m_pct>=1.5).sum()}; src={r[r.move_3m_pct>=1.5].src.value_counts().to_dict()}')
    rows = []
    for i, x in r.iterrows():
        if not isinstance(x.symbol, str):
            continue
        c = x.costs_pct if not np.isnan(x.costs_pct) else COSTS_OFFICIAL
        a = rev_trade(x.symbol, x.det_ms, x.side, c)
        if a is None:
            continue
        a2 = rev_trade(x.symbol, x.det_ms, x.side, c, tp_mode='close')
        a3 = rev_trade(x.symbol, x.det_ms, x.side, c, tp=999)
        a.update(idx=i, net_tpclose=a2['net'] if a2 else np.nan, net_m60=a3['net'] if a3 else np.nan, net_c10=a['gross'] - TAKER_FEES)
        rows.append(a)
    t = pd.DataFrame(rows).set_index('idx')
    r = r.join(t, how='inner')
    r.to_csv(OUT + 'rev_p1_all.csv', index=False)
    out.append('move_3m_pct (bot) vs my bars move3 at detection: corr=%.3f, median |diff|=%.3f' % (
        r[['move_3m_pct', 'm3_bars']].corr().iloc[0, 1], (r.move_3m_pct - r.m3_bars).abs().median()))
    sig = r[r.move_3m_pct >= 1.5]
    for lab, d in (('P1 OOS 13.09-29.09 raw', sig), ('P1 OOS 13.09-29.09 one-pos-per-coin', dedup(sig)),
                   ('P1 TEST 22.09-29.09 one-pos-per-coin', dedup(sig[sig.det_ms >= TEST0])),
                   ('P1 13.09-21.09 one-pos-per-coin', dedup(sig[sig.det_ms < TEST0]))):
        report(lab, d, out)
        report(lab + ' [TP only if 1s close holds]', d, out, 'net_tpclose')
        report(lab + ' [no TP, m60]', d, out, 'net_m60')
        report(lab + ' [costs 0.10]', d, out, 'net_c10')
    dd = dedup(sig)
    report('P1 OOS dedup full-close (src=wallet) only', dd[dd.src == 'wallet'], out)
    report('P1 OOS dedup partial only', dd[dd.src == 'partial'], out)
    report('P1 OOS dedup ws-detected only', dd[dd.detect_src == 'ws'], out)
    report('P1 OOS dedup my bars move3>=1.5 (instead of bot move)', dedup(r[r.m3_bars >= 1.5]), out)
    report('P1 OOS all rev signals (no move filter) dedup', dedup(r), out)
    report('P1 OOS move3<1.5 dedup', dedup(r[r.move_3m_pct < 1.5]), out)
    out.append('P1 OOS dedup per day:\n' + day_stats(dd, 'net').round(3).to_string())
    out.append('P1 OOS dedup rows:\n' + dd[['day', 'coin', 'src', 'side', 'move_3m_pct', 'm3_bars', 'lag_s', 'why', 'hold', 'net', 'net_m60']].round(3).to_string())
    dd.to_csv(OUT + 'rev_p1_dedup.csv', index=False)
    open(OUT + 'rev_out_p1.txt', 'w').write('\n'.join(out) + '\n')
