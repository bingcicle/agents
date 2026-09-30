# (1) Re-derive A2 fills and realistic HL-priced P&L with the conservative queue rule.
from core import *
out = open(W + 'a2_base_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s, flush=True); out.write(s + '\n')
f = add_epi(run(A.td.values >= 0.3))
P('A2 base (td>=0.3, X=1, W=60 s, lat 1.5 s): fills', len(f), 'DISC', (~f.test).sum(), 'TEST', f.test.sum(), '| episodes(5 min)', f.epi.nunique(),
  '| wallets', f.whale.nunique(), 'coins', f.coin.nunique(), '| same wallet as trigger', f.same_wallet.mean().round(2), '| LONG', (f.side == 'LONG').mean().round(2))
P('fills/day DISC %.2f TEST %.2f' % ((~f.test).sum() / 10, f.test.sum() / 8))
P('capacity strictly beyond L (USD, linear walk) p10/p25/p50/p75:', f.cap.quantile([.1, .25, .5, .75]).round(0).tolist())
for dt in (5, 30, 60, 300):
    f[f'P{dt}'] = g(f, dt)
    P(f'P{dt} (passive exit at Binance fair, optimistic):', rep(f, f'P{dt}'))
res = {}
for S in (1000, 2000, 5000, 20000):
    for Hs in (60, 120, 300):
        for d in (0.0, 0.1, 0.2):
            O = exit_sim(f, S=S, Hs=Hs, d=d)
            k = f'R_S{S}_H{Hs}_d{d}'
            f[k] = O.pnl.values; res[k] = O
            if S in (1000, 5000) or (Hs == 60 and d == 0.1):
                P(f'{k}: maker-exit share {O.mk_share.mean():.2f} (fully {(O.mk_share > 0.999).mean():.2f}), taker px from print {(O.tk_src == "print").mean():.2f}, '
                  f'median touch discount {O.med_rsd_pct.iloc[0]:.3f}% |', rep(f, k))
f.to_parquet(W + 'a2_base_fills.parquet')
# HL-candle cross-check from the skeptic (HL 5m close 1-6 min later, fee 0.06)
sk = pd.read_parquet(SP + 'work/skeptic-hl-dislocation/s_a2_exit.parquet')[['ts', 'whale', 'HLx_5m', 'HLx_1m', 'res_5m']]
m = f.merge(sk, on=['ts', 'whale'], how='left')
P('HL 5m-close exit (skeptic):', rep(m, 'HLx_5m'))
P('HL residual below fair at 5m close (%%): mean %.3f median %.3f' % (m.res_5m.mean(), m.res_5m.median()))
out.close()
