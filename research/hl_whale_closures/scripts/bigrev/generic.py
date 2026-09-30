import sys; sys.path.insert(0,'lib'); from hl import *
V=pd.read_csv('work/bigrev/daily_qv.csv',index_col=0).iloc[:,0]
BIG=sorted(V[V>=150e6].index)
kb=kl('BTCUSDT'); btc=kb.c
out=[]
for s in BIG:
    k=kl(s); k=k[k.index>=pd.Timestamp('2026-03-01').value//10**6]
    if len(k)<10000: continue
    c=k.c.values; o=k.o.values; t=k.index.values
    bc=btc.reindex(t).ffill().values
    for N in (1,3,5,15):
        mv=np.full(len(c),np.nan); mv[N:]=100*(c[N:]/c[:-N]-1)
        idx=np.where(np.abs(mv)>=2)[0]; last=-10**9
        for i in idx:
            if i-last<60 or i+241>=len(c): continue
            last=i; side=-np.sign(mv[i])
            en=o[i+1]*(1+side*0.0003)          # next-minute open + 0.03% slippage
            for H in (5,15,30,60,120,240):
                ex=c[i+H]*(1-side*0.0003)
                g=100*side*(ex/en-1); b=100*side*(bc[i+H]/bc[i]-1)
                out.append((s,t[i],N,H,mv[i],g,b))
O=pd.DataFrame(out,columns=['sym','t','N','H','move','gross','btc'])
O['month']=pd.to_datetime(O.t,unit='ms').dt.strftime('%Y-%m'); O['per']=np.where(O.month<='2026-06','TRAIN','TEST')
O['net']=O.gross-0.10; O['net_mn']=O.gross-O.btc-0.10
O['dir']=np.where(O.move<0,'dump->LONG','pump->SHORT')
O.to_parquet('work/bigrev/generic_trades.parquet')
tab=O.groupby(['N','H','per']).agg(n=('net','size'),net=('net','mean'),med=('net','median'),mn=('net_mn','mean')).round(3).unstack('per')
print(tab.to_string())
print(); print('by month, N=3:'); print(O[O.N==3].pivot_table(index='month',columns='H',values='net',aggfunc='mean').round(2).to_string())
print(); print('dumps vs pumps, N=3, TEST:'); print(O[(O.N==3)&(O.per=='TEST')].groupby(['dir','H']).net.agg(['size','mean','median']).round(3).to_string())
print(); print('per coin N=3 H=60 all:'); print(O[(O.N==3)&(O.H==60)].groupby('sym').net.agg(['size','mean']).round(2).sort_values('mean').to_string())
