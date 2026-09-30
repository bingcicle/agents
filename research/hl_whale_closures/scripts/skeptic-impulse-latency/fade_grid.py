from common_s import *
import itertools
A=pd.read_parquet(OUT+'anchors.parquet'); R=pd.read_parquet(OUT+'s1_anchor.parquet')
X=A.merge(R,on='ep'); X['sec']=X.ts//1000; X['pre60']=-X['m-60']
COST=0.146
SIZE={'none':X.tx_usd>0}
for t in (0.1,0.2,0.3): SIZE[f'td>={t}']=X.td>=t
for t in (0.5,1,2): SIZE[f'rel24>={t}']=X.rel24>=t
SIZE['usd>=30k']=X.tx_usd>=3e4; SIZE['pct>=10']=X.tx_pct>=10
def trades(m,H):
    d=X[m].copy(); d['g']=d['ae300']-d[f'ax{H}']; d=d[d.g.notna()].sort_values('ts')
    keep=[];busy={}
    for r in d.itertuples():
        if busy.get(r.coin,0)>r.ts: continue
        keep.append(r.Index); busy[r.coin]=r.ts+H*1000
    d=d.loc[keep]; d['s1']=d.sec+300; d['s2']=d.sec+H
    d=add_alt(d,'s1','s2','alt'); d['g_alt']=d.g+d.wdir*d.alt
    return d
rows=[]
for (sn,sm),a,b,H in itertools.product(SIZE.items(),(0.3,0.5,0.7,1.0),(None,0.3,0.6),(1800,3600)):
    m=sm&(X.pre60>=a)&(X.wdir<0)
    if b is not None: m&=X.m120>=b
    d=trades(m,H)
    for per in ('D','T'):
        x=d[d.per==per]
        rows.append(dict(size=sn,a=a,b=b,H=H,per=per,n=len(x),net=x.g.mean()-COST,net_alt=x.g_alt.mean()-COST,med=x.g.median()-COST))
G=pd.DataFrame(rows)
W=G.pivot_table(index=['size','a','b','H'],columns='per',values=['n','net','net_alt','med'],dropna=False)
W.columns=[f'{x}_{y}' for x,y in W.columns]; W=W.reset_index()
W.to_csv(OUT+'fade_grid.csv',index=False)
ok=W[W.n_D>=15]
print('K',len(W),'eligible',len(ok))
print('share net>0 D %.2f T %.2f | alt-neutral D %.2f T %.2f | corr net D/T %.2f alt %.2f'%((ok.net_D>0).mean(),(ok.net_T>0).mean(),(ok.net_alt_D>0).mean(),(ok.net_alt_T>0).mean(),ok[['net_D','net_T']].corr().iloc[0,1],ok[['net_alt_D','net_alt_T']].corr().iloc[0,1]))
best=ok.sort_values('net_D',ascending=False).head(10); print(best.round(3).to_string())
bestA=ok.sort_values('net_alt_D',ascending=False).head(5); print(bestA.round(3).to_string())
print('median across grid: T net %.3f, T net_alt %.3f'%(ok.net_T.median(),ok.net_alt_T.median()))
