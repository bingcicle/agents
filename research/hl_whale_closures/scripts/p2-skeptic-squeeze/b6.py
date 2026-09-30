import pandas as pd,numpy as np
E=pd.read_parquet('simE.parquet'); E=E[E.dec<=pd.Timestamp('2026-09-28 23:57').value//10**6]
def dd(x,h=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+h*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
S=E[E.side=='LONG']; L=S[S.wpnl<0].copy(); L['liq']=L.liq.fillna(1e9)
L['lev']=(L.liq<5)
wl=L.groupby('addr').lev.mean(); print('wallets',len(wl),'share of wallets mixed',((wl>0)&(wl<1)).mean().round(2))
obs=dd(L[L.lev]).y.mean(); rng=np.random.default_rng(2); nu=[]
ad=wl.index.values
for k in range(1000):
    mp=dict(zip(ad,rng.permutation(wl.values)))  # permute wallet-level leverage share
    u=rng.random(len(L)); lab=u<L.addr.map(mp).values
    nu.append(dd(L[lab]).y.mean())
nu=np.array(nu); print('wallet-level perm: obs',round(obs,2),'null',nu.mean().round(2),'sd',nu.std().round(2),'p',(nu>=obs).mean())
# day-level: is the effect in D_ORIG present on the same days in the non-lev group?  (market regime)
D=dd(L[L.lev]); G=dd(L[~L.lev])
m=D.groupby('day').y.mean().rename('D').to_frame().join(G.groupby('day').y.mean().rename('G'),how='inner')
print('days both',len(m),'mean D',m.D.mean().round(2),'mean G',m.G.mean().round(2),'D>G share',(m.D>m.G).mean().round(2))
