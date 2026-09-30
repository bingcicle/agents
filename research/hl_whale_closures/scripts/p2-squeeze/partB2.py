import pandas as pd, numpy as np
from stats import cboot
E=pd.read_parquet('partB_whale.parquet'); G=pd.read_parquet('partB_generic.parquet')
def dedupe(x,hours=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+hours*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
L=E[E.wpnl<0].copy()
L['lb']=pd.cut(L.liq.fillna(1e9),[0,1,5,20,1e8,1e10],labels=['<1','1-5','5-20','>20','nan'])
print('liq_dist bins (short whale in loss, dedup within bin):')
for b,x in L.groupby('lb',observed=True):
    x=dedupe(x); print(f'  {b}: n={len(x)} y24={x.y24.mean():+.2f} med={x.y24.median():+.2f} ciC={cboot(x.y24,x.sym)} coins={x.sym.nunique()}')
D=dedupe(L[L.liq<5])
D['half']=np.where(D.day<'09-13','A 27.08-12.09','B 13.09-28.09')
for h,x in D.groupby('half'): print(h,len(x),round(x.y24.mean(),2),cboot(x.y24,x.sym))
print('weeks',D.groupby('week').y24.agg(['count','mean']).round(2).to_dict('index'))
for ex in (['NILUSDT'],['NILUSDT','CHIPUSDT'],['NILUSDT','CHIPUSDT','PONSUSDT']):
    x=D[~D.sym.isin(ex)]; print('without',ex,len(x),round(x.y24.mean(),2),cboot(x.y24,x.sym),cboot(x.y24,x.day))
x=D[D.day!='09-28']; print('without 28.09',len(x),round(x.y24.mean(),2),cboot(x.y24,x.sym))
xs=np.sort(D.y24.values)[::-1]; k=int(round(len(xs)*.1)); print('sum',xs.sum().round(1),'wo top10%',xs[k:].sum().round(1))
# same-coin controls split pre / post
pre=[];post=[]
for r in D.itertuples():
    g=G[(G.i==r.i)]
    a=g[(g.dec>=r.dec-12*3600000)&(g.dec<=r.dec-3600000)].y24; b=g[(g.dec>=r.dec+3600000)&(g.dec<=r.dec+12*3600000)].y24
    pre.append(r.y24nf-a.mean() if len(a) else np.nan); post.append(r.y24nf-b.mean() if len(b) else np.nan)
D['dpre']=pre; D['dpost']=post
for c in ('dpre','dpost'):
    z=D[np.isfinite(D[c])]; print(c,round(z[c].mean(),2),cboot(z[c],z.sym),cboot(z[c],z.day))
# placebo: random times in same coin & same day (generic hourly entries), 2000 draws of the D mean
rng=np.random.default_rng(1); Gd={k:v.y24.values for k,v in G.groupby(['sym','day'])}
keys=list(zip(D.sym,D.day)); vals=[Gd.get(k,np.array([])) for k in keys]; ok=[len(v)>0 for v in vals]
means=np.array([np.mean([v[rng.integers(len(v))] for v,o in zip(vals,ok) if o]) for _ in range(2000)])
obs=D[np.array(ok)].y24nf.mean(); print('placebo same coin-day: obs',round(obs,2),'null mean',means.mean().round(2),'p',(means>=obs).mean())
# same day, other coins, random (market-level placebo)
Gday={k:v.y24.values for k,v in G.groupby('day')}
m2=np.array([np.mean([Gday[d][rng.integers(len(Gday[d]))] for d in D.day]) for _ in range(2000)])
print('placebo random coin same day: null',m2.mean().round(2),'p',(m2>=D.y24nf.mean()).mean())
# funding and cost sensitivity; BTC hedge not available here -> note
print('fund mean paid',D.f24.mean().round(3))
D.to_parquet('partB_Dorig.parquet')
