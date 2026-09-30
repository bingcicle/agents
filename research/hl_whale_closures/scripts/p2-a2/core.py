"""A2 core: data + fill simulator + realistic HL exit. Used by all a2_*.py analysis scripts in this dir.
Data: skeptic's s_tx.parquet (every tracked whale tx 12.09-29.09 with Binance 1s refs b{o} = last Binance px at sec+o and HL/Binance
basis estimated from HL 15m (fallback 5m) candles, strictly past). F = fair = b-1 * (1+bas)."""
import os, sys, json, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/p2-a2/'
sys.path.insert(0, SP + 'lib')
TEST0 = pd.Timestamp('2026-09-22').value // 10**6
FEE_MK, FEE_TK = 0.015, 0.045

def load():
    A = pd.read_parquet(SP + 'work/skeptic-hl-dislocation/s_tx.parquet')
    A['bas'] = A.bas15.fillna(A.bas5)
    A = A[A['b-1'].notna() & A.bas.notna()].copy()
    A['F'] = A['b-1'] * (1 + A.bas)
    A['exc_pl'] = 100 * A.wd * (A.pl / A.F - 1)
    A['exc_pf'] = 100 * A.wd * (A.pf / A.F - 1)
    A['td'] = A.tx_usd / A.depth_usd
    dt = pd.to_datetime(A.ts, unit='ms')
    A['day'] = dt.dt.strftime('%m-%d')
    A['week'] = np.select([dt < '2026-09-16', dt < '2026-09-22', dt < '2026-09-26'], ['W1 12-15', 'W2 16-21', 'W3 22-25'], 'W4 26-29')
    A['test'] = A.ts >= TEST0
    A['taker_close'] = (A.liq == 0) & A.dir.isin(['Close Long', 'Close Short'])
    A = A.sort_values('ts').reset_index(drop=True)
    return A

A = load()
BYC = {c: g.index.values for c, g in A.groupby('coin')}          # row indices per coin, time-sorted
COL = {k: A[k].values for k in ('ts', 'wd', 'pl', 'pf', 'px', 'tx_usd', 'F', 'exc_pl', 'exc_pf', 'taker_close', 'liq', 'td', 'bas')}
ADDR = A.addr.values; H = A.h.values

def run(trig_mask, X=1.0, Wsec=60, lat_ms=1500, rearm_s=60, fill_any=False, margin=0.0, trig_filter=None):
    """Event-armed maker bid/ask: after a trigger tx (seen at ts+lat) rest a post-only order X% beyond fair on the side the whale
    hits, for Wsec. Filled by the FIRST later tracked tx in the same coin & direction whose LAST fill px is strictly beyond our
    level (X+margin). One armed order per (coin, direction); after a fill, re-arm blocked for rearm_s.
    Returns fills with trigger/fill row ids."""
    ts, wd, exc, tc = COL['ts'], COL['wd'], COL['exc_pl'], COL['taker_close']
    trig = np.where(trig_mask & tc)[0]
    busy = {}; rows = []
    for i in trig:
        tdet = ts[i] + lat_ms; key = (A.coin.iat[i], wd[i])
        if busy.get(key, 0) > tdet:
            continue
        idx = BYC[key[0]]; tsv = ts[idx]
        lo = np.searchsorted(tsv, tdet, side='right'); hi = np.searchsorted(tsv, tdet + Wsec * 1000, side='right')
        c = idx[lo:hi]
        m = (wd[c] == wd[i]) & (exc[c] > X + margin)
        if not fill_any:
            m &= tc[c]
        busy[key] = tdet + Wsec * 1000
        c = c[m]
        if not len(c):
            continue
        j = c[0]; busy[key] = ts[j] + rearm_s * 1000
        rows.append((i, j))
    if not rows:
        return pd.DataFrame(columns=['ti', 'fi'])
    R = pd.DataFrame(rows, columns=['ti', 'fi'])
    f = A.loc[R.fi, ['addr', 'coin', 'day', 'week', 'test', 'ts', 'wd', 'F', 'bas', 'exc_pl', 'exc_pf', 'td', 'tx_usd', 'pf', 'pl',
                     'b-1', 'b5', 'b10', 'b30', 'b60', 'b120', 'b300', 'b600', 'sym', 'liq']].reset_index(drop=True)
    f['ti'] = R.ti.values; f['fi'] = R.fi.values
    f['trig_addr'] = ADDR[R.ti.values]; f['same_wallet'] = f.addr == f.trig_addr
    f['wait'] = (f.ts - COL['ts'][R.ti.values] - lat_ms) / 1000
    f['X'] = X
    f['L'] = f.F * (1 + f.wd * X / 100)
    # conservative capacity: whale USD strictly beyond our level, linear walk pf->pl (live data: real sweeps put MORE volume
    # deep than linear, so linear is conservative)
    fr = ((f.exc_pl - X) / (f.exc_pl - f.exc_pf).clip(lower=1e-9)).clip(0, 1)
    fr[f.exc_pl - f.exc_pf <= 1e-9] = 1.0
    f['cap'] = f.tx_usd * fr
    f['side'] = np.where(f.wd < 0, 'LONG', 'SHORT')      # our side
    f['whale'] = f.addr
    return f

def g(f, dt):
    """Passive exit at Binance fair after dt s (optimistic P-metric, lens-compatible)."""
    return 100 * (-f.wd) * (f[f'b{dt}'] * (1 + f.bas) / f.L - 1) - 2 * FEE_MK

def exit_sim(f, S=5000, Hs=60, d=0.1, exit_lat_ms=1000, tk_slip=0.05, look_s=30, resid=None):
    """Realistic HL exit for each fill, size S USD (entry size = min(S, cap)).
    1) immediately post an ALO exit at A_t = F_t*(1 - side*d%) (re-pegged to fair at every print); it is filled only by LATER
       opposite-direction tracked prints (any wallet, any dir) and only with the USD volume strictly THROUGH A_t (linear walk);
       fills accumulate until the position is flat.
    2) remainder at tf+Hs: taker on HL. HL bid (for a long) = pf (first fill px = top of book) of the first same-direction
       tracked print in [tf+Hs, tf+Hs+look_s]; if none, fair(tf+Hs) * (1 - side*resid) with resid = median measured on
       the fills where a print exists. Plus tk_slip% slippage for size, fee 0.045.
    Returns arrays: pnl % on the filled size, maker share, entry size."""
    ts, wd, pl, pf, F, usd = COL['ts'], COL['wd'], COL['pl'], COL['pf'], COL['F'], COL['tx_usd']
    out = []
    for r in f.itertuples():
        size = min(S, r.cap) if S else r.cap
        idx = BYC[r.coin]; tsv = ts[idx]
        lo = np.searchsorted(tsv, r.ts + exit_lat_ms, side='right'); hi = np.searchsorted(tsv, r.ts + Hs * 1000, side='right')
        seg = idx[lo:hi]; seg = seg[wd[seg] == -r.wd]
        side = -r.wd                                           # +1 we are long
        rem = size; proceeds = 0.0
        for j in seg:
            Aj = F[j] * (1 - side * d / 100)
            through = (-side) * (Aj - pl[j])                   # >0: print went past our ask (long: pl > A)
            if through <= 0:
                continue
            span = abs(pl[j] - pf[j])
            frac = 1.0 if span < 1e-12 else min(1.0, max(0.0, (side * (pl[j] - Aj)) / span))
            v = usd[j] * frac
            q = min(rem, v); proceeds += q * (side * (Aj / r.L - 1)); rem -= q
            if rem <= 1e-9:
                break
        mk_share = 1 - rem / size if size > 0 else 0
        # taker remainder
        lo2 = np.searchsorted(tsv, r.ts + Hs * 1000, side='right'); hi2 = np.searchsorted(tsv, r.ts + (Hs + look_s) * 1000, side='right')
        s2 = idx[lo2:hi2]; s2 = s2[wd[s2] == r.wd]
        if len(s2):
            bid = pf[s2[0]]; src = 'print'
            rsd = side * (1 - bid / (A.at[s2[0], 'F']))          # measured discount of the touch vs fair (long: + = bid below fair)
        else:
            bid = np.nan; src = 'fair'; rsd = np.nan
        out.append(dict(size=size, mk_share=mk_share, mk_ret=(proceeds / (size - rem) * 100 if size - rem > 0 else np.nan),
                        rem=rem, tk_px=bid, tk_src=src, tk_rsd=rsd))
    O = pd.DataFrame(out, index=f.index)
    med_rsd = O.tk_rsd.median() if O.tk_rsd.notna().any() else 0.25 / 100
    if resid is not None:
        med_rsd = resid / 100
    fairH = f[f'b{Hs}'] * (1 + f.bas)
    side = -f.wd
    tkpx = O.tk_px.where(O.tk_px.notna(), fairH * (1 - side * med_rsd))
    tk_ret = 100 * side * (tkpx / f.L - 1) - tk_slip
    mkr = O.mk_ret.fillna(0)
    pnl = (O.mk_share * (mkr - FEE_MK) + (1 - O.mk_share) * (tk_ret - FEE_TK)) - FEE_MK
    O['tk_ret'] = tk_ret; O['pnl'] = pnl; O['med_rsd_pct'] = 100 * med_rsd
    return O

def cboot(x, cl, n=2000, seed=0):
    x = np.asarray(x, float); cl = np.asarray(cl)
    ok = ~np.isnan(x); x = x[ok]; cl = cl[ok]
    if len(x) < 3:
        return (np.nan, np.nan)
    u, inv = np.unique(cl, return_inverse=True)
    s = np.bincount(inv, weights=x); k = np.bincount(inv)
    rng = np.random.default_rng(seed)
    p = rng.integers(0, len(u), (n, len(u)))
    m = s[p].sum(1) / k[p].sum(1)
    return tuple(float(v) for v in np.percentile(m, [5, 95]).round(3))

def add_epi(f, gap=300):
    f = f.sort_values(['whale', 'coin', 'ts']).copy()
    f['epi'] = ((f.whale != f.whale.shift()) | (f.coin != f.coin.shift()) | (f.ts.diff() > gap * 1000)).cumsum()
    return f.sort_values('ts')

def rep(f, col):
    """One-line honest summary for DISC and TEST."""
    if 'epi' not in f:
        f = add_epi(f)
    s = []
    for part in (False, True):
        x = f[(f.test == part) & f[col].notna()]
        if len(x) < 3:
            s.append(f'{"TEST" if part else "DISC"} n={len(x)}'); continue
        top = x[col].sort_values(ascending=False)
        wo = x[col].sum() - top.iloc[:max(1, int(round(len(x) * 0.1)))].sum()
        s.append(f'{"TEST" if part else "DISC"} n={len(x)} ep={x.epi.nunique()} mean={x[col].mean():+.3f} med={x[col].median():+.3f} '
                 f'win={(x[col] > 0).mean():.2f} CIw{list(cboot(x[col], x.whale))} CId{list(cboot(x[col], x.day))} CIc{list(cboot(x[col], x.coin))} '
                 f'CIep{list(cboot(x[col], x.epi))} sum_wo_top10={wo:+.2f}')
    return ' | '.join(s)
