import sys; sys.path.insert(0,'.')
from sk import *
from scipy import stats
ep=pd.read_parquet('sim_ep_L24b.parquet')
def cr_t(x, cl):
    v=x.y.values; N=len(v); m=v.mean(); g=pd.Series(v-m).groupby(x[cl].values).sum(); G=len(g)
    se=np.sqrt((g**2).sum()*G/(G-1))/N; t=m/se; p=1-stats.t.cdf(t,G-1); return m,se,t,G,p
for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
    d=ep[(ep.day>=a)&(ep.day<=b)&ep.y.notna()]
    x=dedupe(d[d.distress],24)
    for cl in ('day','coin','addr'):
        m,se,t,G,p=cr_t(x,cl); print(f'{per} cluster-robust t by {cl:4s}: mean {m:+.2f} se {se:.2f} t {t:.2f} G={G} one-sided p(t, G-1 df)={p:.3f}')
    dm=x.groupby('day').y.mean(); print(f'{per} equal-weight day means: n_days={len(dm)} mean {dm.mean():+.2f} sd {dm.std():.2f} t {dm.mean()/dm.std()*np.sqrt(len(dm)):.2f} pos days {(dm>0).mean():.2f}')
    if per=='DISC':
        x['wk']=pd.to_datetime(x.day).dt.isocalendar().week
        print(x.groupby(['wk']).agg(n=('y','size'),mean=('y','mean'),med=('y','median'),short=('side',lambda s:(s<0).mean())).round(2).to_string())
        xs=x[x.side<0]; print(' DISC SHORT by week', xs.groupby('wk').y.agg(['size','mean','median']).round(2).to_dict('index'))
