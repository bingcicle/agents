"""Millisecond-resolution impulse / latency study on raw Binance USDT-M aggTrades (data.binance.vision) for a SAMPLE of
symbol-days that contain large whale close episodes. For every episode anchor (first >=$1k tx of a 10 s chain) in the
downloaded symbol-days (all sizes -> within-day control) + 2 placebo times per anchor:
  path o{ms}: signed log ret x100 of last trade at t0+o vs last trade strictly before t0 (t0 = HL fill ts)
  a{L}: taker entry at latency L ms = price of the first trade AT OR AFTER t0+L whose aggressor is on OUR side
        (buyer-taker when we buy -> approximates best ask), signed vs base (positive = we pay more)
  w{L}: worst in-direction trade price in [t0+L, t0+L+250ms] (fallback a{L})
  x{H}: exit at t0+H s, first opposite-aggressor trade (approx touch for our closing order); xw{H}: worst in [H, H+3s]
Output: ms_events.parquet
"""
import os, sys, io, zipfile, time, urllib.request, numpy as np, pandas as pd
from multiprocessing import Pool
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/impulse-latency')
OUT = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/impulse-latency/'
TMP = OUT + 'ms_tmp/'
PATH = [-2000, -1000, -500, -250, -100, -50, 0, 25, 50, 75, 100, 125, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000,
        3000, 5000, 10000, 30000, 60000, 300000]
LAT = [0, 50, 100, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000]
HOR = [10, 30, 60, 300]

def fetch(sym, day):
    url = f'https://data.binance.vision/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{day}.zip'
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                raw = r.read()
            break
        except Exception as e:
            time.sleep(5 * (attempt + 1))
    else:
        return None
    z = zipfile.ZipFile(io.BytesIO(raw))
    with z.open(z.namelist()[0]) as f:
        head = f.read(200).decode()
    hdr = 0 if head.startswith('agg_trade_id') else None
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f, header=hdr, usecols=[1, 2, 5, 6] if hdr is None else None)
    if hdr is None:
        df.columns = ['price', 'quantity', 'transact_time', 'is_buyer_maker']
    else:
        df = df[['price', 'quantity', 'transact_time', 'is_buyer_maker']]
    df['is_buyer_maker'] = df.is_buyer_maker.astype(str).str.lower().isin(['true', '1'])
    df = df.sort_values('transact_time', kind='stable').reset_index(drop=True)
    df = df.astype({'price': 'float64', 'quantity': 'float32', 'transact_time': 'int64'})
    return df

def compute(args):
    sym, day, ev = args
    try:
        df = fetch(sym, day)
    except Exception as e:
        print('ERR', sym, day, e, flush=True)
        return None
    if df is None or len(df) == 0:
        return None
    t = df.transact_time.values; p = df.price.values; bm = df.is_buyer_maker.values
    lp = np.log(p)
    buy_t = t[~bm]; buy_p = p[~bm]; sell_t = t[bm]; sell_p = p[bm]
    from numpy.lib.stride_tricks import sliding_window_view
    res = []
    for r in ev.itertuples():
        t0 = int(r.t0); w = int(r.wdir)
        i_b = np.searchsorted(t, t0, side='left') - 1
        if i_b < 0:
            continue
        base = lp[i_b]
        o = {'row': r.row, 'kind': r.kind, 't0': t0, 'wdir': w, 'base_age_ms': t0 - t[i_b]}
        for off in PATH:
            j = np.searchsorted(t, t0 + off, side='right') - 1
            o[f'o{off}'] = w * (lp[j] - base) * 100 if j >= 0 else np.nan
        # our-side aggressor trades (we trade in whale dir): if w=+1 we buy -> buyer-taker trades (bm False)
        ot, op = (buy_t, buy_p) if w > 0 else (sell_t, sell_p)
        xt, xp = (sell_t, sell_p) if w > 0 else (buy_t, buy_p)
        for L in LAT:
            k = np.searchsorted(ot, t0 + L, side='left')
            a = op[k] if k < len(ot) and ot[k] - (t0 + L) < 10000 else np.nan
            o[f'a{L}'] = w * (np.log(a) - base) * 100
            j0 = np.searchsorted(t, t0 + L, side='left'); j1 = np.searchsorted(t, t0 + L + 250, side='right')
            if j1 > j0:
                ww = p[j0:j1].max() if w > 0 else p[j0:j1].min()
                o[f'w{L}'] = w * (np.log(ww) - base) * 100
            else:
                o[f'w{L}'] = o[f'a{L}']
            j1b = np.searchsorted(t, t0 + L + 3000, side='right')
            if j1b > j0:
                ww = p[j0:j1b].max() if w > 0 else p[j0:j1b].min()
                o[f'w3s{L}'] = w * (np.log(ww) - base) * 100
            else:
                o[f'w3s{L}'] = np.nan
        for H in HOR:
            th = t0 + H * 1000
            k = np.searchsorted(xt, th, side='left')
            x = xp[k] if k < len(xt) and xt[k] - th < 10000 else np.nan
            o[f'x{H}'] = w * (np.log(x) - base) * 100
            j0 = np.searchsorted(t, th, side='left'); j1 = np.searchsorted(t, th + 3000, side='right')
            if j1 > j0:
                ww = p[j0:j1].min() if w > 0 else p[j0:j1].max()
                o[f'xw{H}'] = w * (np.log(ww) - base) * 100
            else:
                o[f'xw{H}'] = np.nan
        # reaction timing: first trade time after t0-2000 where signed move >= half of move at +2000 ms
        j2 = np.searchsorted(t, t0 + 2000, side='right') - 1
        m2 = w * (lp[j2] - base) * 100
        o['m2000'] = m2
        if m2 >= 0.05:
            js = np.searchsorted(t, t0 - 2000, side='left')
            seg = w * (lp[js:j2 + 1] - base) * 100
            hit = np.where(seg >= 0.5 * m2)[0]
            o['t50'] = t[js + hit[0]] - t0 if len(hit) else np.nan
            hit = np.where(seg >= 0.9 * m2)[0]
            o['t90'] = t[js + hit[0]] - t0 if len(hit) else np.nan
            hit = np.where(seg >= 0.1 * m2)[0]
            o['t10'] = t[js + hit[0]] - t0 if len(hit) else np.nan
        # trades count in first 250ms / 1s after t0
        o['n250'] = np.searchsorted(t, t0 + 250) - np.searchsorted(t, t0)
        res.append(o)
    print('done', sym, day, len(res), flush=True)
    return pd.DataFrame(res)

def main():
    os.makedirs(TMP, exist_ok=True)
    from common import load
    X, P = load()
    Pall = pd.read_parquet(OUT + 'placebo.parquet')
    E = X[X.ep_first].copy()
    big = E[(E.tx_usd >= 50e3) | (E.td >= 0.15) | (E.rel24 >= 1.8)]
    sds = big.groupby(['symbol', 'day']).size().sort_values(ascending=False)
    sds = sds[~sds.index.get_level_values(0).isin(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])]
    print('symbol-days', len(sds), 'big events', sds.sum(), flush=True)
    jobs = []
    for (sym, day), _ in sds.items():
        ev = E[(E.symbol == sym) & (E.day == day)][['row', 'ts', 'wdir']].rename(columns={'ts': 't0'})
        ev['kind'] = 'ev'
        pl = Pall[Pall.row.isin(ev.row)][['row', 'psec', 'wdir', 'rep']].merge(ev[['row', 't0']], on='row')
        pl['t0'] = pl.psec * 1000 + (pl.t0 % 1000); pl['kind'] = 'pl' + pl.rep.astype(str)
        jobs.append((sym, day, pd.concat([ev, pl[['row', 't0', 'wdir', 'kind']]])))
    with Pool(3) as pool:
        outs = [o for o in pool.imap_unordered(compute, jobs) if o is not None]
    R = pd.concat(outs)
    R.to_parquet(OUT + 'ms_events.parquet')
    print('saved', R.shape)

if __name__ == '__main__':
    main()
