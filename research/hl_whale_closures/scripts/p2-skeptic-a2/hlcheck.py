# HL-native exit check with HL 1m candles (27.09 08:26 - 29.09): (a) our fills exit at HL 1m close; (b) HL-vs-fair discount after
# big sweeps (all tracked taker sweeps with exc>=1) using the HL 1m close of the minute containing t+60 (price ~60-120 s later).
import json, os, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/p2-skeptic-a2/'
SYM = json.load(open(SP + 'infra/symmap.json'))
w = pd.read_parquet(OUT + 'tx.parquet')
f = pd.read_parquet(OUT + 'fills_base.parquet')
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'; ps = []
    for fn in sorted(os.listdir(d)):
        if fn[:10] < '2026-09-26': continue
        z = np.load(d + fn); ps.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(ps).sort_index(); return s[~s.index.duplicated()]
HL = {}
def hl(coin):
    if coin not in HL:
        fn = f'{SP}data/hl/{coin}_1m.json'
        h = pd.DataFrame(json.load(open(fn))) if os.path.exists(fn) else pd.DataFrame()
        HL[coin] = pd.Series(h.c.astype(float).values, index=h.t.astype(np.int64).values) if len(h) else pd.Series(dtype=float)
    return HL[coin]
T1 = pd.Timestamp('2026-09-27 08:30').value // 10**6
def hl_exit(df, off_ms):
    r = []
    for x in df.itertuples():
        s = hl(x.coin)
        if s.empty or x.ts < T1: r.append((np.nan, np.nan)); continue
        m = (x.ts + off_ms) // 60000 * 60000
        v = s.get(m, np.nan)
        k = kline(SYM[x.coin]); bc = k.get(m, np.nan)
        r.append((v, v / bc - 1 if bc == bc else np.nan))
    return np.array(r)
res = hl_exit(f, 60000)
f['hlc'] = res[:, 0]; f['hl_bn'] = res[:, 1]
x = f[f.hlc.notna()].copy(); side = -x.wd
x['R_hl'] = 100 * side * (x.hlc / x.L - 1) - 0.05 - 0.06
x['disc_hl'] = 100 * side * ((x.hlc / (x.c60 * (1 + x.bas))) - 1)     # + = HL above fair for a long exit (good)
print('fills with HL 1m exit:', len(x))
print(x[['day', 'coin', 'side', 'R', 'R_hl', 'disc_hl', 'bas', 'hl_bn']].round(3).to_string())
print('mean R (fair-0.2 model) %.3f  mean R_hl (HL 1m close, slip .05, fees .06) %.3f  median disc_hl %.3f' % (x.R.mean(), x.R_hl.mean(), x.disc_hl.median()))
# (b) larger sample: all tracked taker closes with exc>=1 (td>=0.1), 27.09 08:30-29.09; HL close at t+60 minute vs Binance fair
w = w[(w.ts >= T1) & w.taker_close if 'taker_close' in w else (w.ts >= T1) & (w.liq == 0) & w.dir.isin(['Close Long', 'Close Short'])].copy()
w['F'] = w['c-1'] * (1 + w.basA); w['exc'] = 100 * w.wd * (w.pl / w.F - 1)
s = w[(w.exc >= 1) & (w.tx_usd / w.depth_usd >= 0.1)].copy()
s = s.sort_values('ts'); s = s[(s.coin != s.coin.shift()) | (s.ts.diff() > 60000)]          # one per coin-minute burst
r = hl_exit(s, 60000); s['hlc'] = r[:, 0]
s['disc'] = 100 * (-s.wd) * (s.hlc / (s.c60 * (1 + s.basA)) - 1)
s['disc_raw'] = 100 * (-s.wd) * (s.hlc / s.c60 - 1)
print('big sweeps with HL 1m close ~60-120 s later: n', s.disc.notna().sum(), 'HL-vs-fair (our side, + good): mean %.3f median %.3f p10 %.3f' %
      (s.disc.mean(), s.disc.median(), s.disc.quantile(.1)), '| same without basis: mean %.3f' % s.disc_raw.mean())
# basis sanity: quiet minutes, HL 1m close vs Binance 1m close vs basA
q = []
for coin in f.coin.unique():
    hs = hl(coin)
    if hs.empty: continue
    k = kline(SYM[coin]); b = (hs / k.reindex(hs.index).values - 1).dropna()
    ww = pd.read_parquet(OUT + 'tx.parquet', columns=['coin', 'ts', 'basA']) if not q else None
    q.append((coin, b.median() * 100, b.std() * 100))
print('HL1m/BN1m basis per coin (median %, sd %):', [(c, round(m, 3), round(sd, 3)) for c, m, sd in q])
