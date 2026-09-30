# Outcomes for every event (Binance USDT-M prices).
# Short horizon (1s bars): decision second dec = ceil(max(ts+1.5 s, bot_ts)); entry = worst-in-3s [dec, dec+2]
#   (buy: max high, sell: min low; fallback 10 s); exit = worst-in-3s at dec+H for H = 60, 300, 1800 s.
#   fol_H / fad_H = gross % for follow (our side = whale trade dir) / fade (opposite), before costs.
#   mid_H = move in whale dir from last price before dec to last price at dec+H (no spread) ; imp = move between tx and dec.
# Long horizon (1m klines): entry = close of the 1m candle opening at the first full minute after dec (price at dec+60..120 s),
#   exit h hours later (2, 8, 24, 72). L_h in whale dir; alt_h / btc_h = EW alt index / BTC over the same window (whale dir).
#   fund_h = HL hourly funding summed over the holding window, in % paid by a LONG (proxy for Binance funding).
import sys, os, json, numpy as np, pandas as pd, time
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/whale-state/'
ev = pd.read_parquet(W + 'sim_events_state.parquet')
ev = ev[ev.day <= '2026-09-29'].copy()
P = np.load(W + 'pxmat.npz'); mins = P['mins']; C = P['C']; syms = list(P['syms']); A = P['altidx']
t0 = mins[0]; si = {s: i for i, s in enumerate(syms)}; ib = si['BTCUSDT']

def load_sym(sym):
    d = SP + 'data/bars1s/' + sym + '/'
    if not os.path.isdir(d): return None
    parts = [np.load(d + fn) for fn in sorted(os.listdir(d))]
    S = np.concatenate([z['sec'] for z in parts]); H = np.concatenate([z['h'] for z in parts])
    L = np.concatenate([z['l'] for z in parts]); Cc = np.concatenate([z['c'] for z in parts])
    o = np.argsort(S, kind='stable'); S, H, L, Cc = S[o], H[o], L[o], Cc[o]
    k = np.concatenate([S[1:] != S[:-1], [True]]); return S[k], H[k], L[k], Cc[k]

def worst(S, H, L, a, side):
    for w in (3, 10):
        i0 = np.searchsorted(S, a, 'left'); i1 = np.searchsorted(S, a + w, 'left')
        if i1 > i0:
            return H[i0:i1].max() if side > 0 else L[i0:i1].min()
    return np.nan

def last(S, Cc, a, maxstale=600):
    i = np.searchsorted(S, a, 'right') - 1
    if i < 0 or S[i] < a - maxstale: return np.nan
    return Cc[i]

fund = {}
def fund_cum(coin):
    if coin in fund: return fund[coin]
    fn = SP + f'work/long-horizon/funding/{coin}.json'  # read-only reuse of the long-horizon lens download (HL fundingHistory)
    if not os.path.exists(fn): fund[coin] = None; return None
    d = json.load(open(fn)); t = np.array([x['time'] for x in d], np.int64); r = np.array([float(x['fundingRate']) for x in d])
    o = np.argsort(t); fund[coin] = (t[o], np.cumsum(r[o])); return fund[coin]

rows = {}
t1 = time.time()
for sym, e in ev.groupby('symbol'):
    B = load_sym(sym)
    for idx, r in e.iterrows():
        o = {}
        wd = r.wdir; dec = int(r.dec_sec); stx = int(r.ts // 1000)
        if B is not None:
            S, H, L, Cc = B
            pre = last(S, Cc, stx - 1); bdec = last(S, Cc, dec - 1)
            o['imp'] = 100 * wd * (bdec / pre - 1)
            o['pre60'] = 100 * wd * (pre / last(S, Cc, stx - 61) - 1)
            o['pre300'] = 100 * wd * (pre / last(S, Cc, stx - 301) - 1)
            eb = worst(S, H, L, dec, +1); es = worst(S, H, L, dec, -1)
            o['spread_in'] = 100 * (eb / es - 1)
            for Hh in (60, 300, 1800):
                xb = worst(S, H, L, dec + Hh, +1); xs = worst(S, H, L, dec + Hh, -1)
                lg = 100 * (xs / eb - 1); sh = 100 * (es - xb) / es
                o[f'fol_{Hh}'] = lg if wd > 0 else sh
                o[f'fad_{Hh}'] = sh if wd > 0 else lg
                o[f'mid_{Hh}'] = 100 * wd * (last(S, Cc, dec + Hh) / bdec - 1)
        # minute-level market factors for the short windows and the long horizon
        j = int((r.dec_ms - t0) // 60000)  # minute containing dec ; A[j] ~ index level at start of minute j
        for Hh in (300, 1800):
            jj = j + Hh // 60
            if jj < len(A) - 1:
                o[f'alt_s{Hh}'] = 100 * wd * (A[jj + 1] / A[j + 1] - 1)
        j0 = j + 1; k = si.get(sym)
        o['pxL0'] = C[k, j0] if k is not None and j0 < C.shape[1] else np.nan
        fc = fund_cum(r.coin)
        for h in (2, 8, 24, 72):
            jh = j0 + 60 * h
            if k is None or jh >= C.shape[1]: continue
            o[f'L{h}'] = 100 * wd * (C[k, jh] / C[k, j0] - 1)
            o[f'alt{h}'] = 100 * wd * (A[jh + 1] / A[j0 + 1] - 1)
            o[f'btc{h}'] = 100 * wd * (C[ib, jh] / C[ib, j0] - 1)
            if fc is not None:
                ta, cs = fc; te = mins[j0] + 60000; tx_ = mins[jh] + 60000
                if ta[0] <= te and ta[-1] >= tx_ - 3600000:
                    a0 = np.searchsorted(ta, te, 'right') - 1; a1 = np.searchsorted(ta, tx_, 'right') - 1
                    o[f'fund{h}'] = 100 * (cs[a1] - cs[a0])
        rows[idx] = o
O = pd.DataFrame.from_dict(rows, orient='index')
ev = ev.join(O)
ev.to_parquet(W + 'sim_events_out.parquet')
print('events', len(ev), 'with fol_300', ev.fol_300.notna().sum(), 'with L24', ev.L24.notna().sum(), round(time.time() - t1), 's')
print('DISCOVERY ONLY'); print(ev[ev.day<='2026-09-21'][['spread_in', 'fol_60', 'fad_60', 'fol_300', 'fad_300', 'fol_1800', 'fad_1800', 'mid_300', 'L8', 'L24', 'alt24', 'fund24']].describe().T.round(3))
