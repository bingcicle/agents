"""Independent anchor construction (skeptic). Episodes: same addr+coin+wdir, chain gap<=10 s over txs>=$1k.
rel24 = anchor tx_usd / (Binance quote vol of the 1440 fully-closed 1m candles before ts / 1440)."""
import numpy as np, pandas as pd, os, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT=SP+'work/skeptic-impulse-latency/'
t=pd.read_parquet(SP+'data/tx_matrix.parquet')
print('raw',len(t), 'days', t.day.min(), t.day.max())
t=t[(t.day<='2026-09-29')&t.symbol.notna()&(t.tx_usd>=1000)].copy()
# sanity: tx_usd vs sz*px ; wdir vs dir
print('tx_usd/(sz*px) med', (t.tx_usd/(t.sz*t.px)).median(), 'dir x wdir', pd.crosstab(t.dir,t.wdir).to_dict())
t=t.sort_values(['addr','coin','wdir','ts','h']).reset_index(drop=True)
new=(t.addr!=t.addr.shift())|(t.coin!=t.coin.shift())|(t.wdir!=t.wdir.shift())|(t.ts-t.ts.shift()>10000)
t['ep']=new.cumsum()
g=t.groupby('ep')
t['ep_n']=g.ts.transform('size'); t['ep_usd']=g.tx_usd.transform('sum')
A=t[new.values].copy()
# if several txs share the anchor ts (same block), aggregate their usd as anchor size known at that moment
same=t[t.ts==t.groupby('ep').ts.transform('min')].groupby('ep').tx_usd.sum()
A['anc_usd']=A.ep.map(same)
print('txs',len(t),'episodes',len(A),'dup',len(t)/len(A))
# rel24 from k1m
K=SP+'data/k1m/'
rel=np.full(len(A),np.nan); qv24=np.full(len(A),np.nan)
for sym,idx in A.groupby('symbol').groups.items():
    d=K+sym+'/'
    if not os.path.isdir(d): continue
    parts=[]
    for fn in sorted(os.listdir(d)):
        z=np.load(d+fn); parts.append(pd.DataFrame({'ot':z['ot'],'qv':z['qv']}))
    k=pd.concat(parts).drop_duplicates('ot').sort_values('ot')
    ot=k.ot.values; cq=np.concatenate([[0],np.cumsum(k.qv.values)])
    ts=A.loc[idx,'ts'].values
    j=np.searchsorted(ot+60000, ts, side='right')      # candles closed by ts: ot+60000<=ts
    t0=ts-86400000
    i0=np.searchsorted(ot, t0, side='left')
    ncand=j-i0
    s=cq[j]-cq[i0]
    v=np.where(ncand>=1000, s/1440.0, np.nan)
    qv24[A.index.get_indexer(idx)]=v
A['qv24m']=qv24
A['rel24']=A.tx_usd/A.qv24m
A['td']=A.tx_usd/A.depth_usd
A['per']=np.where(A.day<='2026-09-21','D','T')
A.to_parquet(OUT+'anchors.parquet')
t[['addr','coin','symbol','ts','wdir','tx_usd','ep','day']].to_parquet(OUT+'txs1k.parquet')
for thr in (1,2,4):
    m=A.rel24>=thr
    print(f'rel24>={thr}: n={m.sum()} D={((A.per=="D")&m).sum()} T={((A.per=="T")&m).sum()} symdays={A[m].groupby(["symbol","day"]).ngroups} whale_sells={(A[m].wdir<0).mean():.2f}')
m=A.td>=0.3; print('td>=0.3', m.sum(), A[m].groupby(['symbol','day']).ngroups)
m=(A.rel24>=2)|(A.td>=0.3); print('union', m.sum(), A[m].groupby(['symbol','day']).ngroups, A[m].symbol.value_counts().head(8).to_dict())
