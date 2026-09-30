# Live (30.09 evening, ~35 whale coins) check with REAL HL trades (sizes, takers) + Binance bookTicker (ms):
#  1) HL spread / depth at X% (book competition), HL-vs-Binance basis
#  2) UNCONDITIONAL fills of a resting order X% beyond Binance fair: every HL taker trade printing beyond X (any trader),
#     volume beyond X, P&L per unit at exit = Binance fair after dt, split tracked-whale takers vs others.
from common import *
out = open(W + 'q_live_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
T = pd.read_parquet(W + 'live_hl_trades.parquet'); B = pd.read_parquet(W + 'live_hl_book.parquet'); Q = pd.read_parquet(W + 'live_bn_bbo.parquet')
T = T[T.r >= T.r.min() + 5000]           # drop the initial snapshot of historical trades delivered at subscribe
T = T[T.time >= B.time.min()]
Q['mid'] = 0.5 * (Q.bid + Q.ask); B['mid'] = 0.5 * (B.bid + B.ask)
tracked = set(pd.read_parquet(SP + 'data/whale_txs.parquet', columns=['addr']).addr.unique())
P(f'live window: {pd.to_datetime(T.time.min(), unit="ms")} .. {pd.to_datetime(T.time.max(), unit="ms")} UTC, '
  f'{(T.time.max()-T.time.min())/3.6e6:.2f} h, trades {len(T)}, coins {T.coin.nunique()}, book snaps {len(B)}, bn quotes {len(Q)}')

# ---- per coin merge Binance mid onto book snapshots and trades
res_b, res_t = [], []
for coin in sorted(set(T.coin) & set(Q.coin.dropna())):
    q = Q[Q.coin == coin].sort_values('time')[['time', 'mid']].rename(columns={'mid': 'bmid'})
    b = B[B.coin == coin].sort_values('time')
    t = T[T.coin == coin].sort_values('time')
    if len(b) < 20 or len(q) < 100:
        continue
    b = pd.merge_asof(b, q, on='time', direction='backward')
    b['bas'] = b.mid / b.bmid - 1
    b['basis'] = b.bas.rolling(200, min_periods=20).median().shift(1)      # past snapshots only
    t = pd.merge_asof(t, q, on='time', direction='backward')
    t = pd.merge_asof(t, b[['time', 'basis', 'mid', 'bid', 'ask']].rename(columns={'mid': 'hmid'}), on='time', direction='backward')
    for dt in (5, 60, 300):
        qq = q.rename(columns={'time': 'tq', 'bmid': f'bmid{dt}'})
        t['tq'] = t.time + dt * 1000
        t = pd.merge_asof(t.sort_values('tq'), qq, on='tq', direction='backward').sort_values('time')
    res_b.append(b); res_t.append(t)
B2 = pd.concat(res_b); T2 = pd.concat(res_t)
B2['spread_bp'] = 1e4 * (B2.ask - B2.bid) / B2.mid
P('\n1) HL book: median spread (bp), median depth $k within 0.5/1/2% (bid side), share of snapshots whose 20 visible bid levels do not reach -1%:')
g = B2.groupby('coin')
tb = pd.DataFrame({'spread_bp': g.spread_bp.median(), 'bdep0.5_k': g['bdep0.5'].median() / 1e3, 'bdep1_k': g['bdep1.0'].median() / 1e3,
                   'bdep2_k': g['bdep2.0'].median() / 1e3, 'book20_not_reach_1%': g.bid_last_dist.apply(lambda x: (x > -0.01).mean()),
                   'basis_%': 100 * g.bas.median(), 'basis_iqr_%': 100 * (g.bas.quantile(.75) - g.bas.quantile(.25))})
P(tb.round(3).to_string())
P('median over coins: spread %.1f bp, basis IQR %.3f%%' % (tb.spread_bp.median(), tb['basis_iqr_%'].median()))

# ---- side semantics check: taker buys should print at/above HL mid
T2['vs_mid'] = T2.px / T2.hmid - 1
P('\nside check: median px/HLmid-1 (bp) for side B = %.2f, side A = %.2f' % (1e4 * T2[T2.side == 'B'].vs_mid.median(), 1e4 * T2[T2.side == 'A'].vs_mid.median()))
T2['tdir'] = np.where(T2.side == 'B', 1, -1)                 # taker direction
T2['taker'] = np.where(T2.side == 'B', T2.u0, T2.u1)
T2['tracked'] = T2.taker.isin(tracked)
T2 = T2[T2.basis.notna() & T2.bmid.notna()]
T2['F'] = T2.bmid * (1 + T2.basis)
T2['exc'] = 100 * T2.tdir * (T2.px / T2.F - 1)
T2['usd'] = T2.px * T2.sz
hours = (T2.time.max() - T2.time.min()) / 3.6e6

P('\n2) UNCONDITIONAL fills (any taker) of a resting order X% beyond Binance fair; volume-weighted; 60 s cooldown per coin/side for event counts')
rows = []
for X in (0.3, 0.5, 1.0, 1.5, 2.0):
    f = T2[T2.exc > X].copy()
    if not len(f):
        rows.append(dict(X=X, events=0)); continue
    # our P&L per unit filled at our level L = F*(1 - tdir*X) (we are the opposite side), exit at Binance fair after dt
    for dt in (5, 60, 300):
        f[f'P{dt}'] = 100 * (-f.tdir) * ((f[f'bmid{dt}'] * (1 + f.basis)) / (f.F * (1 + f.tdir * X / 100)) - 1) - 0.03
    f = f.sort_values('time')
    ev = f.groupby(['coin', 'tdir', (f.time // 60000)]).agg(t=('time', 'first'), usd=('usd', 'sum'), tracked=('tracked', 'max'),
                                                            P5=('P5', 'mean'), P60=('P60', 'mean'), P300=('P300', 'mean'), exc=('exc', 'max'))
    r = dict(X=X, events=len(ev), ev_per_h=len(ev) / hours, usd_beyond_per_h=f.usd.sum() / hours, ev_tracked=int(ev.tracked.sum()),
             P5_ev=ev.P5.mean(), P60_ev=ev.P60.mean(), P300_ev=ev.P300.mean(),
             P60_vw=np.average(f.P60.fillna(0), weights=f.usd), P60_ev_untracked=ev[~ev.tracked].P60.mean(), P60_ev_tracked=ev[ev.tracked].P60.mean(),
             med_usd_ev=ev.usd.median())
    rows.append(r)
P(pd.DataFrame(rows).round(3).to_string())
T2[T2.exc > 0.3].to_parquet(W + 'live_fills_x03.parquet')
out.close()

# ---- 3) A2 pilot on live data with ANY taker: trigger = one taker order (hash) whose notional >= 0.3 x visible same-side depth
#         within 1% (last book snapshot before it); arm at +1.5 s for 60 s at X=1% beyond fair; fill by ANY later taker trade beyond.
out = open(W + 'q_live_results.txt', 'a')
T2 = T2.sort_values('time')
B2s = B2.sort_values('time')
orders = T2.groupby(['coin', 'hash', 'tdir']).agg(time=('time', 'first'), usd=('usd', 'sum'), exc=('exc', 'max'), tracked=('tracked', 'max')).reset_index()
orders = orders.sort_values('time')
orders = pd.merge_asof(orders, B2s[['time', 'coin', 'bdep1.0', 'adep1.0']], on='time', by='coin', direction='backward')
orders['depth'] = np.where(orders.tdir < 0, orders['bdep1.0'], orders['adep1.0'])
orders['td'] = orders.usd / orders.depth
trig = orders[orders.td >= 0.3]
P(f'\n3) A2 live pilot: triggers (any taker, order >= 0.3 x visible 1% depth): {len(trig)} (tracked takers {int(trig.tracked.sum())}), per hour {len(trig)/hours:.1f}')
rows = []; busy = {}
for t in trig.itertuples():
    key = (t.coin, t.tdir); t0 = t.time + 1500
    if busy.get(key, 0) > t0:
        continue
    busy[key] = t0 + 60000
    c = T2[(T2.coin == t.coin) & (T2.tdir == t.tdir) & (T2.time > t0) & (T2.time <= t0 + 60000) & (T2.exc > 1.0)]
    if not len(c):
        continue
    f = c.iloc[0]; busy[key] = f.time + 60000
    L = f.F * (1 + f.tdir * 0.01)
    rows.append(dict(coin=t.coin, trig_tracked=t.tracked, fill_tracked=f.tracked, fill_usd_beyond=c[c.hash == f.hash].usd.sum(),
                     P5=100 * (-f.tdir) * (f.bmid5 * (1 + f.basis) / L - 1) - 0.03, P60=100 * (-f.tdir) * (f.bmid60 * (1 + f.basis) / L - 1) - 0.03))
A = pd.DataFrame(rows)
P(f'   armed windows filled: {len(A)}; ' + (A.round(3).to_string() if len(A) else 'none'))
out.close()
