"""Matched placebo p-values and tx2 pass-probability simulation."""
from common import *
rng = np.random.default_rng(3)
out = []

# REV P1: matched random-time placebo (5 draws per signal, same coin/day/side) -> distribution of the mean of 1 draw per signal
dd = pd.read_csv(OUT + 'rev_p1_dedup.csv')
pa = pd.read_csv(OUT + 'rev_placebo_random.csv')
obs = dd.net.mean()
groups = [pa[(pa.coin == c) & (pa.day == d) & (pa.side == s)].net.values for c, d, s in zip(dd.coin, dd.day, dd.side)]
ok = [g for g in groups if len(g)]
sims = np.array([np.mean([g[rng.integers(len(g))] for g in ok]) for _ in range(5000)])
out.append(f'REV P1 dedup obs mean={obs:+.3f} (n={len(dd)}); matched random-time placebo mean of means={sims.mean():+.3f} '
           f'P95={np.percentile(sims,95):+.3f} p(placebo>=obs)={(sims>=obs).mean():.3f} (matched {len(ok)} signals)')
obs_tp = (dd.why == 'tp').mean()
out.append(f'REV P1 TP rate {obs_tp:.2f} vs random placebo {(pa.why=="tp").mean():.2f}')
pb = pd.read_csv(OUT + 'rev_placebo_nowhale.csv')
L = pb[pb.side > 0]
sims2 = np.array([L.groupby('day').net.apply(lambda s: s.sample(1, random_state=int(rng.integers(1e9))).iloc[0]).sample(
    len(dd), replace=True, random_state=int(rng.integers(1e9))).mean() for _ in range(1000)])
out.append(f'REV no-whale LONG dump-fade: mean={L.net.mean():+.3f} n={len(L)}; P1 LONG mean={dd[dd.side>0].net.mean():+.3f} n={(dd.side>0).sum()}; '
           f'draws of n={len(dd)} (1 per day-resample) P(mean>=obs)={(sims2>=obs).mean():.3f}')
# sensitivity of REV P1 to single trades
s = np.sort(dd.net.values)
out.append(f'REV P1 dedup mean without best 1/3/5 trades: {s[:-1].mean():+.3f} / {s[:-3].mean():+.3f} / {s[:-5].mean():+.3f}; '
           f'days with mean>0: {(dd.groupby("day").net.mean()>0).mean():.2f} of {dd.day.nunique()}')
# m60 counterfactual tail: how many trades would lose > 5% without TP
out.append(f'REV P1 tail: trades with net_m60 < -5%: {(dd.net_m60 < -5).sum()} (min {dd.net_m60.min():+.1f}); realized net min {dd.net.min():+.2f}')

# H1/H2: matched random-time placebo difference
h = pd.read_csv(OUT + 'h1h2_trades.csv', low_memory=False); h = h[~h.net.isna()]
hp = pd.read_csv(OUT + 'h1h2_placebo_random.csv')
for lab, d, p in (('H2', h, hp), ('H1', h[~h.excl], hp[~hp.excl])):
    out.append(f'{lab}: obs mean {d.net.mean():+.3f} median {d.net.median():+.3f} TP {100*(d.why=="tp").mean():.0f}% | random placebo mean {p.net.mean():+.3f} '
               f'median {p.net.median():+.3f} TP {100*(p.why=="tp").mean():.0f}% | diff mean {d.net.mean()-p.net.mean():+.3f}')

# tx2 rule A: probability to pass registered criteria at n=40, if the next 25 trades follow the in-sample cell distribution
t = pd.read_csv(OUT + 'tx2_trades_main.csv'); A = t[(t.rule == 'A') & (t['var'] == 'main') & t.dedup_keep]
ins = pd.read_csv(RES + 'tx2/tx2_events.csv', low_memory=False)
cell = ins[(ins.dt12_s <= 40) & (ins.ratio2 >= 5) & ins.net2_E1.notna()].net2_E1.values
need = 40 - len(A)
passes = 0
for _ in range(20000):
    x = np.concatenate([A.net.values, rng.choice(cell, need, replace=True)])
    passes += (np.median(x) >= 0.15) and ((x > 0).mean() >= 0.5)
out.append(f'tx2 rule A: observed n={len(A)} med={A.net.median():+.3f} win={(A.net>0).mean():.2f}; in-sample cell n={len(cell)} med={np.median(cell):+.3f}; '
           f'P(median>=0.15 & win>=50% at n=40 | next {need} ~ in-sample) = {passes/20000:.3f} (upper bound: other criteria ignored)')
B = t[(t.rule == 'B') & (t['var'] == 'main') & t.dedup_keep]
out.append(f'tx2 rule B: n={len(B)} nets={B.net.round(3).tolist()}')
open(OUT + 'stats_out.txt', 'w').write('\n'.join(out) + '\n')
print('\n'.join(out))
