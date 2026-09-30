import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
rng = np.random.default_rng(7)
REPS = 200
for per in ('disc', 'test'):
    x = pd.read_parquet(W + f'C_{per}.parquet')
    x = x[np.isfinite(x.t_net)].copy()
    x['sg'] = np.where(x.our == 'LONG', 1, -1)
    x['te'] = (x.seen_s + 60).astype('int64')
    x['d0'] = pd.to_datetime(x.dday).astype('datetime64[s]').astype('int64')
    A = np.full((REPS, len(x)), np.nan); Bm = np.full((REPS, len(x)), np.nan)
    pos = {i: k for k, i in enumerate(x.index)}
    for s, idx in x.groupby('sym').groups.items():
        S = Sym1s(s)
        for i in idx:
            r = x.loc[i]; k = pos[i]
            for rep in range(REPS):
                # (1) random second in same UTC day
                t = int(r.d0 + rng.integers(0, 86400 - 3600))
                en = S.worst(t, r.sg); ex = S.worst(t + 3600, -r.sg)
                A[rep, k] = 100 * r.sg * (ex / en - 1) - COST
                # (2) same coin, entry shifted by +-2..12 h (keeps regime, avoids the exact TWAP)
                sh = int(rng.integers(2 * 3600, 12 * 3600)) * (1 if rng.random() < .5 else -1)
                t2 = int(r.te + sh)
                en = S.worst(t2, r.sg); ex = S.worst(t2 + 3600, -r.sg)
                Bm[rep, k] = 100 * r.sg * (ex / en - 1) - COST
        del S
    real = x.t_net.mean()
    pa = np.nanmean(A, 1); pb = np.nanmean(Bm, 1)
    print(f'{per} C tape real={real:+.3f} n={len(x)} | placebo same-day random: mean={pa.mean():+.3f} q5/q95={np.percentile(pa,5):+.3f}/{np.percentile(pa,95):+.3f} pctile={100*(pa<real).mean():.0f}%'
          f' | placebo +-2..12h shift: mean={pb.mean():+.3f} q5/q95={np.percentile(pb,5):+.3f}/{np.percentile(pb,95):+.3f} pctile={100*(pb<real).mean():.0f}%')
