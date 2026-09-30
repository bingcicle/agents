# Tolerant parser of the live recorder (data/live/*.jsonl.gz, files may be still open -> truncated tail).
# Produces live_hl_trades.parquet, live_hl_book.parquet (top of book + cumulative depth by distance), live_bn_bbo.parquet.
import gzip, json, glob, zlib, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
REV = {v: k for k, v in SYM.items() if v}

def lines(fn):
    raw = open(fn, 'rb').read()
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    buf = b''
    try:
        buf = d.decompress(raw)
        # multi-member gzip (append mode): keep decompressing
        while d.unused_data:
            rest = d.unused_data
            d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            buf += d.decompress(rest)
    except Exception:
        pass
    for l in buf.split(b'\n'):
        if not l:
            continue
        try:
            yield json.loads(l)
        except Exception:
            continue

trades, book, bbo = [], [], []
DIST = [0.25, 0.5, 1.0, 1.5, 2.0, 3.0]
for fn in sorted(glob.glob(SP + 'data/live/hl_*.jsonl.gz')):
    for e in lines(fn):
        m = e['m']; ch = m.get('channel')
        if ch == 'trades':
            for t in m['data']:
                trades.append((t['coin'], t['time'], float(t['px']), float(t['sz']), t['side'], t['hash'], t['tid'],
                               t['users'][0] if t.get('users') else None, t['users'][1] if t.get('users') else None, e['r']))
        elif ch == 'l2Book':
            dd = m['data']; bids, asks = dd['levels']
            if not bids or not asks:
                continue
            bb, ba = float(bids[0]['px']), float(asks[0]['px']); mid = 0.5 * (bb + ba)
            row = [dd['coin'], dd['time'], bb, ba, e['r'], float(bids[-1]['px']) / mid - 1, float(asks[-1]['px']) / mid - 1]
            bp = np.array([float(x['px']) for x in bids]); bs = np.array([float(x['sz']) for x in bids])
            ap = np.array([float(x['px']) for x in asks]); as_ = np.array([float(x['sz']) for x in asks])
            for dist in DIST:
                row.append(float((bs * bp)[bp >= mid * (1 - dist / 100)].sum()))
                row.append(float((as_ * ap)[ap <= mid * (1 + dist / 100)].sum()))
            book.append(row)
for fn in sorted(glob.glob(SP + 'data/live/bn_*.jsonl.gz')):
    for e in lines(fn):
        m = e['m']; st = m.get('stream', '')
        if st.endswith('@bookTicker'):
            d = m['data']
            bbo.append((REV.get(d['s']), d['E'], float(d['b']), float(d['a']), e['r']))
T = pd.DataFrame(trades, columns=['coin', 'time', 'px', 'sz', 'side', 'hash', 'tid', 'u0', 'u1', 'r']).drop_duplicates('tid')
cols = ['coin', 'time', 'bid', 'ask', 'r', 'bid_last_dist', 'ask_last_dist'] + [f'{s}{d}' for d in DIST for s in ('bdep', 'adep')]
B = pd.DataFrame(book, columns=cols)
Q = pd.DataFrame(bbo, columns=['coin', 'time', 'bid', 'ask', 'r'])
T.to_parquet(W + 'live_hl_trades.parquet'); B.to_parquet(W + 'live_hl_book.parquet'); Q.to_parquet(W + 'live_bn_bbo.parquet')
print('trades', len(T), 'book', len(B), 'bbo', len(Q))
print('time range', pd.to_datetime(T.time.min(), unit='ms'), pd.to_datetime(T.time.max(), unit='ms'))
print(T.side.value_counts())
