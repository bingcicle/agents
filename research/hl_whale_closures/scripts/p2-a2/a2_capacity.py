# (3) HL taker-exit discount on a larger sample, frequency, $/day, capital; queue view.
from core import *
out = open(W + 'a2_capacity_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s, flush=True); out.write(s + '\n')
ts, wd, pf, F = COL['ts'], COL['wd'], COL['pf'], COL['F']
# --- HL touch vs fair X s after big sweeps: same-direction tracked prints' FIRST fill px = top of book on the side we would hit
trig = np.where((COL['td'] >= 0.3) & (COL['exc_pl'] >= 1.0) & COL['taker_close'])[0]
rec = []
for i in trig:
    idx = BYC[A.coin.iat[i]]; tsv = ts[idx]
    for a, b in ((30, 60), (60, 90), (90, 150), (150, 300)):
        lo = np.searchsorted(tsv, ts[i] + a * 1000); hi = np.searchsorted(tsv, ts[i] + b * 1000)
        s = idx[lo:hi]; s = s[wd[s] == wd[i]]
        if len(s):
            rec.append((i, f'{a}-{b}s', 100 * wd[i] * (pf[s[0]] / F[s[0]] - 1)))   # + = touch beyond fair in whale dir (bid below fair)
R = pd.DataFrame(rec, columns=['i', 'win', 'disc'])
P('HL touch on the whale side vs fair after big sweeps (td>=0.3 & exc>=1; n triggers %d): median / mean / p75 / p90 by window' % len(trig))
P(R.groupby('win').disc.describe(percentiles=[.5, .75, .9])[['count', '50%', 'mean', '75%', '90%']].round(3).to_string())
# --- base fills and $/day
f = add_epi(run(A.td.values >= 0.3))
days_d = sorted(A[~A.test].day.unique()); days_t = sorted(A[A.test].day.unique())
P('\nfills/day: DISC %.2f TEST %.2f ; fills per active day median %.1f; days with 0 fills %d of %d' % (
  (~f.test).sum() / len(days_d), f.test.sum() / len(days_t), f.groupby('day').size().median(), len(days_d) + len(days_t) - f.day.nunique(), len(days_d) + len(days_t)))
P('capacity strictly beyond our level (USD): p10 %.0f p25 %.0f p50 %.0f p75 %.0f ; share of fills with cap >= $1k %.2f, >= $2k %.2f, >= $5k %.2f, >= $20k %.2f' % (
  tuple(f.cap.quantile([.1, .25, .5, .75])) + tuple((f.cap >= v).mean() for v in (1000, 2000, 5000, 20000))))
for S in (1000, 2000, 5000):
    for resid in (None, 0.35):
        O = exit_sim(f, S=S, Hs=60, d=0.1, resid=resid)
        f['usd'] = O['size'].values * O.pnl.values / 100
        for part, days in ((False, days_d), (True, days_t)):
            dd = f[f.test == part].groupby('day').usd.sum().reindex(days, fill_value=0)
            P(f'S=${S} exit-resid={"measured" if resid is None else resid}: {"TEST" if part else "DISC"} $/day mean {dd.mean():6.1f} median {dd.median():6.1f} '
              f'worst {dd.min():7.1f} best {dd.max():6.1f} days>0 {(dd > 0).sum()}/{len(dd)} | $/fill mean {f[f.test == part].usd.mean():6.2f}')
# --- concurrency / capital: armed windows + open positions
trig = np.where((COL['td'] >= 0.3) & COL['taker_close'])[0]
arm = []
busy = {}
for i in trig:
    key = (A.coin.iat[i], wd[i]); t0 = ts[i] + 1500
    if busy.get(key, 0) > t0:
        continue
    busy[key] = t0 + 60000; arm.append((t0, t0 + 60000))
ev = sorted([(a, 1) for a, b in arm] + [(b, -1) for a, b in arm])
c = 0; mx = 0; cs = []
for t, d in ev:
    c += d; mx = max(mx, c); cs.append(c)
P('\narmed windows: %d (%.1f/day); max simultaneously armed coins %d; p99 %d' % (len(arm), len(arm) / 18, mx, int(np.percentile(cs, 99))))
pos = sorted([(t, 1) for t in f.ts] + [(t + 61000, -1) for t in f.ts])
c = 0; mxp = 0
for t, d in pos:
    c += d; mxp = max(mxp, c)
P('max simultaneous open A2 positions (60 s hold): %d' % mxp)
P('capital on HL (cross margin, 5x lev, orders reserve margin): S=$1k -> ~$%d, S=$2k -> ~$%d, S=$5k -> ~$%d (incl. 2x buffer for max %d armed + stress)' % (
  tuple(int(2 * S * max(mx, 3) / 5) for S in (1000, 2000, 5000)) + (mx,)))
out.close()
