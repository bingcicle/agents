# Live recording (30.09, 2h, 35 coins): how sweeps consume the book. For each taker sweep (same hash & taker & coin),
# volume distribution along its price range; volume traded 0.8-1.2% from fair; compare with linear-walk assumption.
import pandas as pd, numpy as np
L='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/live/'
T=pd.read_parquet(L+'hl_trades_fair.parquet')
T=T[T.fair.notna()].copy()
T['wd']=np.where(T.side=='B',1,-1)
T['dev_w']=T.wd*T.dev   # + = beyond fair in taker direction
g=T.groupby(['coin','hash','taker'])
S=g.agg(t=('time','first'),wd=('wd','first'),usd=('usd','sum'),dmin=('dev_w','min'),dmax=('dev_w','max'),n=('tid','size'),whale=('taker_whale','first')).reset_index()
S['rng']=S.dmax-S.dmin
print('sweeps',len(S),'| range>=0.3%',(S.rng>=0.3).sum(),'| reaching >=0.8% beyond fair',(S.dmax>=0.8).sum(),'| >=1%',(S.dmax>=1).sum())
big=S[S.rng>=0.3]
rows=[]
for r in big.itertuples():
    x=T[(T.coin==r.coin)&(T.hash==r.hash)&(T.taker==r.taker)]
    q=(x.dev_w-r.dmin)/r.rng
    for qq in (0.25,0.5,0.75,0.9):
        rows.append((r.coin,r.hash,qq,x.usd[q>qq].sum()/r.usd, 1-qq))
Q=pd.DataFrame(rows,columns=['coin','hash','q','share_beyond','linear'])
print('\nshare of sweep USD beyond relative price position q (median / mean) vs linear 1-q, n sweeps=%d'%big.shape[0])
print(Q.groupby('q')[['share_beyond','linear']].agg(['median','mean']).round(3))
# USD printed in the 0.8-1.2% band beyond fair by sweeps reaching >=1.2%, and beyond 1%
deep=S[S.dmax>=1.0]
out=[]
for r in deep.itertuples():
    x=T[(T.coin==r.coin)&(T.hash==r.hash)&(T.taker==r.taker)]
    out.append((r.coin,r.t,r.usd,r.dmax,x.usd[(x.dev_w>=0.8)&(x.dev_w<=1.2)].sum(),x.usd[x.dev_w>1.0].sum(),r.whale))
D=pd.DataFrame(out,columns=['coin','t','usd','dmax','usd_band_08_12','usd_beyond_1','whale'])
print('\nsweeps reaching >=1% beyond fair:'); print(D.round(2).to_string())
D.to_csv('live_deep_sweeps.csv',index=False); Q.to_csv('live_sweep_shape.csv',index=False)
