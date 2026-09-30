"""Pattern-matched null for the fade family: NON-whale times in the same symbol-days (>=300 s from any >=$1k whale tx
of that coin) where the price pattern matches: |60 s pre-move| >= a (direction d = sign of pre-move), continuation
d*(p(+120)-p0) >= b. Fade entry at +300 s worst-in-3s, exit +3600 s worst-in-3s. One trade per coin per hour.
Compare with whale anchors satisfying the same pattern (with and without size filter)."""
import sys; sys.path.insert(0, '.')
from common import *
from build import dense
X, P = load()
E = X[X.ep_first].copy(); E['pre60'] = -E['m-60']
rows = []
for (sym, day), grp in X.groupby(['symbol', 'day']):
    dd = dense(sym, day)
    base, cf, hw, lw, N = dd['base'], dd['cf'], dd['hw'], dd['lw'], dd['N']
    D0 = int(pd.Timestamp(day).timestamp())
    s = np.arange(D0 + 61, D0 + 86400, 2)
    i = s - base
    lp = np.log(cf)
    p0 = lp[i - 1]; pm60 = lp[i - 61]; p120 = lp[i + 120]
    mv = (p0 - pm60) * 100
    d = np.sign(mv)
    cont = d * (p120 - p0) * 100
    wsec = np.unique(grp.sec.values)
    near = np.zeros(len(s), bool)
    pos = np.searchsorted(wsec, s)
    for k in (pos - 1, pos):
        kk = np.clip(k, 0, len(wsec) - 1)
        near |= np.abs(wsec[kk] - s) <= 300
    for a in (0.5, 1.0):
        for b in (0.3,):
            m = (np.abs(mv) >= a) & (cont >= b) & ~near & np.isfinite(p120)
            idx = np.where(m)[0]
            last = -10**12
            for j in idx:
                if s[j] < last + 3600: continue
                last = s[j]
                ie = i[j] + 300; ix = i[j] + 3600
                if ix + 3 >= N: continue
                dj = d[j]
                ent = (lw if dj > 0 else hw)[ie]; ex = (hw if dj > 0 else lw)[ix]
                if not (np.isfinite(ent) and np.isfinite(ex)): continue
                fade = -dj * (np.log(ex) - np.log(ent)) * 100
                rows.append(dict(sym=sym, day=day, sec=s[j], a=a, b=b, d=dj, mv=mv[j], fade=fade))
R = pd.DataFrame(rows)
R['per'] = np.where(R.day <= DISC_END, 'D', 'T')
R.to_parquet(OUT + 'pattern_null.parquet')
print('NULL (no whale), fade +300 -> +3600, one per coin-hour:')
print(R.groupby(['a', 'per']).fade.agg(['size', 'mean', 'median']).round(3).to_string())
print(R.groupby(['a', 'per', 'd']).fade.agg(['size', 'mean', 'median']).round(3).to_string())
from fade_eval import trades, SIZE
for a in (0.5, 1.0):
    for sn in ('none', 'td>=0.2', 'td>=0.3', 'rel24>=1', 'pct>=10', 'usd>=30k'):
        d = trades(SIZE[sn] & (E.pre60 >= a) & (E.m120 >= 0.3), 300, 3600)
        print(f'WHALE a={a} {sn:9s}', ' | '.join(f'{p}: n={len(d[d.per==p])} mean={d[d.per==p].g.mean():+.3f} med={d[d.per==p].g.median():+.3f}' for p in ('D', 'T')))
