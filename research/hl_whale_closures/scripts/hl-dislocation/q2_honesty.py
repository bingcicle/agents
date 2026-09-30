# Honesty battery for Strategy A (whale-conditional fills). Residual HL discount for exits estimated on DISCOVERY events only,
# by dislocation bin (deeper sweeps leave a deeper book hole).
from strat import *
out = open(W + 'q2_honesty_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')

R = pd.read_parquet(W + 'q1_rev_prints.parquet')
R = R[pd.to_datetime('2026-' + R.day) < pd.Timestamp('2026-09-22')]          # DISCOVERY only
EB = [0.3, 0.75, 1.25, 2, 100]
R['e0b'] = pd.cut(R.exc0, EB, labels=False)
def table(same):
    t = R[R.same_side == same].groupby(['ep', 'e0b', 'bk']).exc.median().groupby(['e0b', 'bk']).median().unstack()
    return t.ffill(axis=1).bfill(axis=1)
RS, RO = table(True), table(False)
P('residual same-side (taker exit) by exc bin x dt (DISC):'); P(RS.round(2).to_string())
P('residual opposite-side (maker exit level) by exc bin x dt (DISC):'); P(RO.round(2).to_string())
def lookup(T, exc, dt):
    b = np.clip(pd.cut(exc, EB, labels=False).fillna(0).astype(int), 0, len(EB) - 2)
    col = max(k for k in T.columns if k <= dt)
    return T[col].reindex(b).values

def fills2(df, X, cool_s=60):
    f = fills(df, X, cool_s)
    Lr = (1 + f.wdir * X / 100)
    for dt in (5, 60, 300):
        fair = f[f'bn{dt}'] / f['bn-1']
        gross = 100 * (-f.wdir) * (fair / Lr - 1)
        f[f'T{dt}'] = gross - lookup(RS, f.exc_pl, dt) - 0.06          # taker exit on HL at the (still depressed) side
        f[f'M{dt}'] = gross - lookup(RO, f.exc_pl, dt) - 0.03          # maker exit at the level where opposite prints trade
    f['whale'] = f.addr; f['side'] = np.where(f.wdir < 0, 'LONG', 'SHORT')
    return f

EXITS = ['P5', 'P60', 'P300', 'M5', 'M60', 'M300', 'T60', 'H1']
summary_rows = []
for cond_name, cond in [('ALL', w.index == w.index), ('coin_known>=5', w.coin_known >= 5)]:
    for X in (1.0, 1.5, 2.0, 3.0):
        for part, sel in [('DISC', ~w.test), ('TEST', w.test)]:
            d = w[cond & sel]; nd = d.day.nunique()
            f = fills2(d, X)
            for ex in EXITS:
                h = honesty(f, ex, wallet='whale', day='day', coin='coin')
                summary_rows.append(dict(cond=cond_name, X=X, part=part, exit=ex, n=h['n'], per_day=h['n'] / nd, med=h['median'], mean=h['mean'],
                                         win=h['win'], ci_mean_wallet=h.get('mean_ci90_whale'), ci_mean_day=h.get('mean_ci90_day'),
                                         ci_mean_coin=h.get('mean_ci90_coin'), ci_med_wallet=h.get('med_ci90_whale'),
                                         half1=h.get('half1_mean'), half2=h.get('half2_mean'), sum=h['sum'], sum_wo_top10=h['sum_wo_top10'],
                                         maxsh_wallet=h.get('max_share_whale'), maxsh_day=h.get('max_share_day'), maxsh_coin=h.get('max_share_coin')))
            if cond_name == 'ALL' and X in (1.5, 2.0):
                P(f'\n--- {cond_name} X={X} {part}: fills={len(f)} ({len(f)/nd:.1f}/day) coins={f.coin.nunique()} wallets={f.addr.nunique()}')
                for ex in ('P60', 'M60', 'T60', 'H1'):
                    P(f'  {ex}: ' + fmt_h(honesty(f, ex, wallet='whale', day='day', coin='coin')))
                    for sd in ('LONG', 'SHORT'):
                        s = f[f.side == sd]
                        if len(s):
                            P(f'     {sd}: n={len(s)} med={s[ex].median():+.3f} mean={s[ex].mean():+.3f}')
                P('  capacity beyond our level (linear walk), $k: p25/p50/p75 = %.1f / %.1f / %.1f' % tuple(f.cap_usd.quantile([.25, .5, .75]) / 1000))
                for size in (1000, 2000, 5000, 20000):
                    fill_usd = np.minimum(size, f.cap_usd)
                    P(f'  size ${size}: filled ${fill_usd.sum()/nd:,.0f}/day, P&L/day M60 ${(fill_usd*f.M60/100).sum()/nd:,.1f}  T60 ${(fill_usd*f.T60/100).sum()/nd:,.1f}  P60 ${(fill_usd*f.P60/100).sum()/nd:,.1f}  H1 ${(fill_usd*f.H1/100).sum()/nd:,.1f}')
                f.to_parquet(W + f'q2_fills_{cond_name}_X{X}_{part}.parquet')
S = pd.DataFrame(summary_rows)
S.to_csv(W + 'q2_honesty_table.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40); pd.set_option('display.max_rows', 400)
P('\nSUMMARY (mean/median per fill in %, cluster CI90 of mean by wallet/day/coin):')
P(S[['cond', 'X', 'part', 'exit', 'n', 'per_day', 'med', 'mean', 'win', 'ci_mean_wallet', 'ci_mean_day', 'ci_mean_coin', 'half1', 'half2', 'sum_wo_top10', 'maxsh_wallet', 'maxsh_coin']].round(3).to_string())
# variant bookkeeping: how many DISC-positive variants stay positive on TEST
piv = S.pivot_table(index=['cond', 'X', 'exit'], columns='part', values='mean')
P('\nvariants K=%d; DISC mean>0: %d; of those TEST mean>0: %d' % (len(piv), (piv.DISC > 0).sum(), ((piv.DISC > 0) & (piv.TEST > 0)).sum()))
out.close()
