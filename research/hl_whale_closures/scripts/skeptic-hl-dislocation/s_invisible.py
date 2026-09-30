# How many A2 fills would come from traders we do not see? Use HL 1m candles (27.09 08:26 - 29.09) over armed windows.
from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'; parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn); parts.append(pd.DataFrame({k: z[k] for k in ('o', 'h', 'l', 'c')}, index=z['ot']))
    s = pd.concat(parts).sort_index(); return s[~s.index.duplicated()]
X = 1.0; Wsec = 60
trig = A[(A.td >= 0.3) & A.taker_close]
busy = {}; rows = []
fills = pd.read_parquet(OUT + 's_a2_base.parquet')
for t in trig.itertuples():
    tdet = t.ts + 1500; key = (t.coin, t.wd)
    if busy.get(key, 0) > tdet:
        continue
    busy[key] = tdet + Wsec * 1000
    fl = fills[(fills.coin == t.coin) & (fills.ts > tdet) & (fills.ts <= tdet + Wsec * 1000) & (fills.wd == t.wd)]
    if len(fl):
        busy[key] = fl.ts.iloc[0] + 60000
    rows.append(dict(ts=t.ts, tdet=tdet, coin=t.coin, wd=t.wd, sym=t.sym, bas=t.bas, tracked_fill=len(fl) > 0, test=t.test))
Wn = pd.DataFrame(rows)
print('armed windows', len(Wn), 'DISC', (~Wn.test).sum(), 'TEST', Wn.test.sum(), '| tracked fill rate', Wn.tracked_fill.mean().round(3))
H1 = {}
out = []
for r in Wn.itertuples():
    fn = f'{SP}data/hl/{r.coin}_1m.json'
    if r.coin not in H1:
        d = pd.DataFrame(json.load(open(fn)))
        for c in 'ohlc': d[c] = d[c].astype(float)
        H1[r.coin] = d.set_index('t')
    d = H1[r.coin]
    if d.index.min() > r.ts - 60000:
        continue
    k = kline(r.sym)
    first = (r.ts // 60000 + 1) * 60000          # first full minute after the trigger's own minute
    bars = [b for b in range(first, r.tdet + Wsec * 1000, 60000) if b in d.index and b in k.index]
    if not bars:
        continue
    g_fill = False; p_fill = False; pnl_pess = []; pnl_opt = []
    for b in bars:
        hl_ext = d.loc[b, 'l'] if r.wd < 0 else d.loc[b, 'h']
        bl, bh = k.loc[b, 'l'], k.loc[b, 'h']
        Lg = (bl if r.wd < 0 else bh) * (1 + r.bas) * (1 + r.wd * X / 100)    # our level at Binance's extreme in that minute
        Lp = (bh if r.wd < 0 else bl) * (1 + r.bas) * (1 + r.wd * X / 100)    # our level at Binance's other extreme
        if (-r.wd) * (Lg - hl_ext) > 0: g_fill = True
        if (-r.wd) * (Lp - hl_ext) > 0: p_fill = True
    out.append(dict(ts=r.ts, coin=r.coin, tracked=r.tracked_fill, guaranteed=g_fill, possible=p_fill, nbars=len(bars)))
O = pd.DataFrame(out)
print('armed windows with a full 1m bar inside and HL 1m data:', len(O))
print(pd.crosstab(O.tracked, O.guaranteed, margins=True, rownames=['tracked fill in window'], colnames=['HL 1m low guaranteed beyond L']))
print(pd.crosstab(O.tracked, O.possible, margins=True, rownames=['tracked fill in window'], colnames=['HL 1m low possibly beyond L']))
