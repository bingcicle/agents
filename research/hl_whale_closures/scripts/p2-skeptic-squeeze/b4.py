import pandas as pd,numpy as np
from st2 import *
E=pd.read_parquet('simE.parquet'); E=E[E.dec<=pd.Timestamp('2026-09-28 23:57').value//10**6]
def dd(x,h=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+h*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
S=E[E.side=='LONG']
D=dd(S[(S.wpnl<0)&(S.liq<5)])
print('wallets',D.addr.nunique()); w=D.groupby('addr').y.agg(['count','sum']).sort_values('sum'); print(w.tail(5).round(1)); print(w.head(3).round(1))
print('wallet cluster CI',cb(D.y,D.addr))
wp=w['sum'].clip(lower=0); print('max wallet share of pos sum',round(wp.max()/wp.sum(),2))
# leave-one-wallet-out
lo=[D[D.addr!=a].y.mean() for a in D.addr.unique()]; print('LOWO min',round(min(lo),2))
# Coin overlap with liq>=10 group
G=dd(S[(S.wpnl<0)&(S.liq>=10)]); common=set(D.sym)&set(G.sym); print('coins D',D.sym.nunique(),'G',G.sym.nunique(),'common',len(common))
print('within common coins: D',round(D[D.sym.isin(common)].y.mean(),2),len(D[D.sym.isin(common)]),' G',round(G[G.sym.isin(common)].y.mean(),2),len(G[G.sym.isin(common)]))
# permutation: shuffle liq labels among short-in-loss events (within coin) -> distribution of D_ORIG-like mean
L=S[(S.wpnl<0)].copy(); rng=np.random.default_rng(0); obs=D.y.mean(); null=[]
for k in range(500):
    L['lp']=L.groupby('sym').liq.transform(lambda v: rng.permutation(v.values))
    null.append(dd(L[L.lp<5]).y.mean())
null=np.array(null); print('perm within coin: obs',round(obs,2),'null mean',round(null.mean(),2),'p',(null>=obs).mean())
# monthly/weekly
print(D.groupby('week').y.agg(['count','mean']).round(2).T)
