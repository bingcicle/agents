# Specification null: random 2-condition states on sim-trigger variables known at trigger; lens pipeline
# (DISC: pick better of fol/fad on L24 mn net; candidate if n>=15, mean>0, median>0) -> TEST criteria
# (mean>0, median>0, CI90 mean by coin AND day >0, both halves>0, sum w/o top10%>0, max share coin/day<0.5).
import sys; sys.path.insert(0,'.')
from sk import *
ep=pd.read_parquet('sim_ep_L24b.parquet')
ep=ep[ep.y.notna()].copy()
ep['y_fad']=ep.y; ep['y_fol']=-ep.ew-COST+ep.fund.fillna(0)
ep['lpos']=np.log(ep.whale_pos_usd); ep['ldep']=np.log(ep.depth_1pct_usd); ep['lratio']=np.log(ep.whale_ratio); ep['lrpm']=np.log1p(ep.ratio_per_min)
VARS=['wpnl','liq_dist','lpos','ldep','lratio','lrpm']
D=ep[ep.day<='2026-09-21']; T=ep[ep.day>='2026-09-22']
def dd(x): return dedupe(x,24)
def crit_test(x):
    v=x.y.values
    if len(v)<10 or v.mean()<=0 or np.median(v)<=0: return False
    if cboot(v,x.coin,n=500)[0]<=0 or cboot(v,x.day,n=500)[0]<=0: return False
    days=sorted(x.day.unique()); h=len(days)//2
    if x[x.day.isin(days[:h])].y.mean()<=0 or x[x.day.isin(days[h:])].y.mean()<=0: return False
    xs=np.sort(v)[::-1]; k=max(1,int(round(len(v)*.1)))
    if xs[k:].sum()<=0: return False
    pos=x[x.y>0]
    for c in ('coin','day'):
        g=pos.groupby(c).y.sum()
        if g.max()/g.sum()>=0.5: return False
    return True
rng=np.random.default_rng(3)
res=[]
for it in range(500):
    v1,v2=rng.choice(VARS,2,replace=False)
    q1,q2=rng.uniform(0.1,0.9,2); s1,s2=rng.choice([-1,1],2)
    t1=D[v1].quantile(q1); t2=D[v2].quantile(q2)
    def mask(z): return ((s1*(z[v1]-t1))>0)&((s2*(z[v2]-t2))>0)
    md=mask(D)
    xs={}
    for dr in ('fol','fad'):
        z=D[md].copy(); z['side']=z.wtd*(1 if dr=='fol' else -1); z['y']=z['y_'+dr]; xs[dr]=dd(z)
    dr=max(xs,key=lambda k: xs[k].y.mean() if len(xs[k]) else -9)
    xd=xs[dr]
    cand=len(xd)>=15 and xd.y.mean()>0 and xd.y.median()>0
    if not cand: res.append(dict(cand=False)); continue
    z=T[mask(T)].copy(); z['side']=z.wtd*(1 if dr=='fol' else -1); z['y']=z['y_'+dr]; xt=dd(z)
    res.append(dict(cand=True,v1=v1,v2=v2,q1=q1,q2=q2,s1=s1,s2=s2,t1=t1,t2=t2,dir=dr,nD=len(xd),mD=xd.y.mean(),nT=len(xt),mT=xt.y.mean() if len(xt) else np.nan,
                    shortshare=(xt.side<0).mean() if len(xt) else np.nan,passT=crit_test(xt) if len(xt)>=10 else False))
R=pd.DataFrame(res); R.to_csv('specnull2.csv',index=False)
c=R[R.cand]
print('random specs',len(R),'DISC candidates',len(c),'dir share fad',(c.dir=='fad').mean().round(2))
print('TEST pass rate among DISC candidates', c.passT.mean().round(3), ' TEST mean>0 rate', (c.mT>0).mean().round(3), ' median TEST mean', c.mT.median().round(2))
print('pass rate by dir', c.groupby('dir').passT.mean().round(3).to_dict(), c.groupby('dir').size().to_dict())
print('P(at least 1 of 4 candidates passes) if independent', round(1-(1-c.passT.mean())**4,3))
