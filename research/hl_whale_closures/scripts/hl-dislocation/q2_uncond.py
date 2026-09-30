# Q2 Strategy A, UNCONDITIONAL part (adverse selection): for EVERY minute (HL 1m candles, 27.09 08:26-29.09) and every 5-min bar
# (HL 5m candles, 13.09-29.09), in every coin, would a resting HL order at X% beyond Binance fair have been filled, and what happened
# next? Fair = Binance x (1+basis), basis = rolling median of HL close/Binance close over the previous 60 min (1m) / 12 bars (5m).
#   sure fill (long side): HL_low < BN_low*(1+b)*(1-X)   (at the moment of the HL low Binance was >= its low, so our bid was above)
#   entry level L unknown inside the bar: L in [BN_low, BN_high]*(1+b)*(1-X). pnl_opt uses BN_low, pnl_pes uses BN_high.
#   exit: passive at Binance fair (close of bar t+k) fee 0.03; for the 1m set also k in 1,5,15,60 minutes.
# Each fill is tagged 'whale' if a tracked-whale taker tx in the same coin/bar/direction was itself beyond X (from q1 data).
from common import *
import json, os
out = open(W + 'q2_uncond_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)

w = load_tx()
XS = [0.3, 0.5, 1.0, 1.5, 2.0, 3.0]

def build(iv, mins, ks):
    rows = []
    for coin, s in SYM.items():
        if not s:
            continue
        fn = f'{SP}data/hl/{coin}_{iv}.json'
        if not os.path.exists(fn):
            continue
        d = json.load(open(fn))
        if not d:
            continue
        h = pd.DataFrame(d)[['t', 'o', 'h', 'l', 'c', 'v', 'n']]
        for c in 'ohlcv':
            h[c] = h[c].astype(float)
        h = h[(h.t < END) & (h.v > 0) & (h.n > 0)].set_index("t")   # drop zero-volume (stale) HL bars
        k = kl(s)
        if k is None:
            continue
        k = k[(k.index >= h.index.min()) & (k.index < END)]
        if mins > 1:
            g = (k.index // (mins * 60000)) * (mins * 60000)
            k = k.groupby(g).agg(o=('o', 'first'), h=('h', 'max'), l=('l', 'min'), c=('c', 'last'), qv=('qv', 'sum'))
        df = h.join(k[['h', 'l', 'c']].rename(columns={'h': 'bh', 'l': 'bl', 'c': 'bc'}), how='inner')
        if len(df) < 100:
            continue
        # full regular grid (bars without HL trades are absent in candles; Binance closes needed for exits)
        grid = pd.RangeIndex(df.index.min(), df.index.max() + 1, mins * 60000)
        bc_full = k.c.reindex(grid)
        df['basis'] = (df.c / df.bc - 1).rolling(max(12, 60 // mins), min_periods=6).median().shift(1)
        for kk in ks:
            df[f'bc{kk}'] = bc_full.reindex(df.index + kk * mins * 60000).values
        df['coin'] = coin
        rows.append(df.reset_index())
    return pd.concat(rows, ignore_index=True)

def analyze(df, label, ks, mins):
    df = df[df.basis.notna()].copy()
    df['day'] = pd.to_datetime(df.t, unit='ms').dt.strftime('%m-%d')
    df['test'] = df.t >= TEST0
    wc = set(w.coin.unique())
    df['whale_coin'] = df.coin.isin(wc)
    # whale-driven tag: tracked whale tx in the same coin & bar with exc_pl > X in the right direction
    w['bar'] = (w.ts // (mins * 60000)) * (mins * 60000)
    P(f'\n######## {label}: bars={len(df)} coins={df.coin.nunique()} days={df.day.nunique()} whale-coins={df[df.whale_coin].coin.nunique()}')
    recs = []
    for X in XS:
        for side in (1, -1):          # +1: our bid below fair (fills on dumps); -1: our ask above fair
            if side == 1:
                sure = df.l < df.bl * (1 + df.basis) * (1 - X / 100)
                poss = df.l < df.bh * (1 + df.basis) * (1 - X / 100)
                L_opt = df.bl * (1 + df.basis) * (1 - X / 100); L_pes = df.bh * (1 + df.basis) * (1 - X / 100)
            else:
                sure = df.h > df.bh * (1 + df.basis) * (1 + X / 100)
                poss = df.h > df.bl * (1 + df.basis) * (1 + X / 100)
                L_opt = df.bh * (1 + df.basis) * (1 + X / 100); L_pes = df.bl * (1 + df.basis) * (1 + X / 100)
            wk = set(zip(w[(w.exc_pl > X) & (w.wdir == -side)].coin, w[(w.exc_pl > X) & (w.wdir == -side)].bar))
            f = df[sure].copy()
            f['possible_only'] = False
            f['X'] = X; f['side'] = side
            f['whale'] = [(c, t) in wk for c, t in zip(f.coin, f.t)]
            for kk in ks:
                fair = f[f'bc{kk}'] * (1 + f.basis)
                f[f'opt{kk}'] = 100 * side * (fair / L_opt[sure] - 1) - 0.03
                f[f'pes{kk}'] = 100 * side * (fair / L_pes[sure] - 1) - 0.03
            f['n_poss'] = int(poss.sum())
            recs.append(f)
    F = pd.concat(recs, ignore_index=True)
    F.to_parquet(W + f'q2_uncond_fills_{label}.parquet')
    kk = ks[1]
    for scope, sel in [('all coins', F.index == F.index), ('whale coins', F.whale_coin)]:
        for part, tv in [('DISC', False), ('TEST', True)]:
            d = F[sel & (F.test == tv)]
            nd = df[df.test == tv].day.nunique()
            if not len(d) or nd == 0:
                continue
            P(f'\n-- {scope} / {part} ({nd} days); P&L = passive exit at Binance fair after {kk} bar(s) of {mins}m, fee 0.03, bounds opt/pes')
            g = d.groupby(['X', 'whale'])
            t = pd.DataFrame({'fills': g.size(), 'per_day': g.size() / nd, 'coins': g.coin.nunique(),
                              f'opt{kk}_md': g[f'opt{kk}'].median(), f'opt{kk}_mn': g[f'opt{kk}'].mean(),
                              f'pes{kk}_md': g[f'pes{kk}'].median(), f'pes{kk}_mn': g[f'pes{kk}'].mean()})
            P(t.round(3).to_string())
            g = d.groupby('X')
            t = pd.DataFrame({'fills': g.size(), 'per_day': g.size() / nd, 'whale_share': g.whale.mean()})
            for k2 in ks:
                t[f'opt{k2}_mn'] = g[f'opt{k2}'].mean(); t[f'pes{k2}_mn'] = g[f'pes{k2}'].mean()
            P('ALL fills (whale + non-whale):'); P(t.round(3).to_string())
    return F

F1 = analyze(build('1m', 1, [1, 5, 15, 60]), '1m', [1, 5, 15, 60], 1)
F5 = analyze(build('5m', 5, [1, 3, 12]), '5m', [1, 3, 12], 5)
out.close()
