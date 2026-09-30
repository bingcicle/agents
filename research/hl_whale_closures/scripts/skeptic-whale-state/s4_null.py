import sys; sys.path.insert(0,'.')
from sk import *
ep=pd.read_parquet('sim_ep_L24b.parquet')
rng=np.random.default_rng(11)
def cellmean(d, mask):
    x=dedupe(d[mask],24); return x.y.mean(), len(x)
def shuffle_within(d, lab, grp):
    out=lab.copy(); g=d[grp].astype(str).agg('|'.join,axis=1).values if isinstance(grp,list) else d[grp].values
    for key in np.unique(g):
        idx=np.flatnonzero(g==key)
        if len(idx)>1: out[idx]=out[idx][rng.permutation(len(idx))]
    return out
for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
    d=ep[(ep.day>=a)&(ep.day<=b)&ep.y.notna()].reset_index(drop=True)
    d['cd']=d.coin+'|'+d.day
    lab=d.distress.values.copy()
    obs,n=cellmean(d,lab)
    print(f'== {per} observed mean {obs:+.3f} n={n}')
    for name,grp in (('coin x day',['coin','day']),('day',['day']),('side x day',['side','day']),('coin',['coin']),('side (whole period)',['side'])):
        res=np.array([cellmean(d,shuffle_within(d,lab,grp))[0] for _ in range(400)])
        print(f'   shuffle within {name:20s}: null mean {np.nanmean(res):+.3f} sd {np.nanstd(res):.3f}  p(null>=obs)={np.nanmean(res>=obs):.3f}')
    # placebo: same coin & side as each kept trade, random entry time in the same period (uniform), 400 draws
    x=dedupe(d[lab],24)
    lo,hi=d.dec_ms.min(),d.dec_ms.max()
    pm=[]
    for _ in range(400):
        z=x[['coin','symbol','side']].copy(); z['dec_ms']=rng.integers(lo,hi,len(z))
        O=outcomes(z,z.side.values,24); pm.append(np.nanmean(O.ew-COST-O.fund.fillna(0)))
    pm=np.array(pm); print(f'   placebo same coin+side random time: mean {pm.mean():+.3f} sd {pm.std():.3f} p={np.mean(pm>=obs):.3f}')
