# Snapshot HL l2Book with coarse aggregation (nSigFigs 3 and 2) to see resting size at 0.8-1.2% from mid (full-precision
# l2Book 20 levels only spans ~0.1-0.5%). 2 rounds over the A2 coin list.
import json, time, urllib.request, pandas as pd, numpy as np, os, ssl
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
f=pd.read_parquet(SP+'work/skeptic-hl-dislocation/s_a2_exit.parquet')
coins=sorted(set(f.coin)|set(pd.read_parquet(SP+'work/live/hl_book.parquet',columns=['coin']).coin))
ctx=ssl.create_default_context(cafile='/root/.ccr/ca-bundle.crt')
def post(body):
    r=urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    return json.loads(urllib.request.urlopen(r,timeout=10,context=ctx).read())
rows=[]
for rnd in range(2):
    for c in coins:
        for nsf in (5,3,2):
            try: d=post({'type':'l2Book','coin':c,'nSigFigs':nsf} if nsf!=5 else {'type':'l2Book','coin':c})
            except Exception as e: print(c,nsf,e); continue
            b,a=d['levels']
            if not b or not a: continue
            bp=np.array([float(x['px']) for x in b]); bs=np.array([float(x['sz']) for x in b])
            ap=np.array([float(x['px']) for x in a]); as_=np.array([float(x['sz']) for x in a])
            mid=(bp[0]+ap[0])/2
            db=100*(1-bp/mid); da=100*(ap/mid-1)
            rows.append(dict(rnd=rnd,coin=c,nsf=nsf,mid=mid,span_b=db[-1],span_a=da[-1],
              bid_to_1=float((bp*bs)[db<=1.0].sum()), bid_band=float((bp*bs)[(db>=0.8)&(db<=1.2)].sum()),
              ask_to_1=float((ap*as_)[da<=1.0].sum()), ask_band=float((ap*as_)[(da>=0.8)&(da<=1.2)].sum()),
              step_pct=100*np.median(np.diff(-bp))/mid if len(bp)>1 else np.nan))
        time.sleep(0.05)
D=pd.DataFrame(rows); D.to_csv('depth_snap.csv',index=False)
# use the finest aggregation whose span covers 1.2% on the bid side
ok=D[D.span_b>=1.2].sort_values('nsf',ascending=False).drop_duplicates(['rnd','coin'])
print('coins covered',ok.coin.nunique(),'of',len(coins),'| nSigFigs used',ok.nsf.value_counts().to_dict(),'| median bucket step %',ok.step_pct.median().round(3))
print('bid resting USD within 0.8-1.2%% of mid: p25/p50/p75 = %s'%ok.bid_band.quantile([.25,.5,.75]).round(0).tolist())
print('bid USD within 1%%: p25/p50/p75 = %s'%ok.bid_to_1.quantile([.25,.5,.75]).round(0).tolist())
print(ok.groupby('coin')[['bid_band','bid_to_1','step_pct']].median().round(2).to_string())
