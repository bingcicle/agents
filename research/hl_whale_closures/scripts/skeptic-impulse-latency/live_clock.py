import gzip, json, numpy as np, pandas as pd, collections
L='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/data/live/'
def rd(fn):
    out=[]
    try:
        with gzip.open(L+fn,'rt') as f:
            for line in f:
                try: out.append(json.loads(line))
                except Exception: break
    except Exception as e: print('trunc',fn,e)
    return out
hl=[]; bn=[]; chans=collections.Counter(); bstreams=collections.Counter()
for h in ('20','21'):
    for o in rd(f'hl_20260930_{h}.jsonl.gz'):
        m=o['m']; chans[m.get('channel')]+=1
        if m.get('channel')=='trades':
            for t in m['data']:
                hl.append((o['r'],t['time'],t['coin'],t['side'],float(t['px']),float(t['sz']),t['hash'],t['users'][0],t['users'][1]))
    for o in rd(f'bn_20260930_{h}.jsonl.gz'):
        d=o['m']['data']; s=o['m']['stream']; bstreams[s.split('@')[1]]+=1
        if d.get('e')=='bookTicker':
            bn.append((o['r'],d['T'],d['E'],d['s'],float(d['b']),float(d['a'])))
        elif d.get('e')=='aggTrade':
            bn.append((o['r'],d['T'],d['E'],d['s'],float(d['p']),-float(d['p']) if d['m'] else float(d['p'])))
print(chans, bstreams)
H=pd.DataFrame(hl,columns=['r','t','coin','side','px','sz','hash','u0','u1'])
B=pd.DataFrame(bn,columns=['r','T','E','s','b','a'])
print(len(H),len(B))
H['lag']=H.r-H.t; B['lag']=B.r-B.E; B['lagT']=B.r-B['T']
q=[0,0.01,0.05,0.25,0.5,0.75,0.95]
print('HL r-time quantiles', H.lag.quantile(q).round(0).to_dict())
print('BN r-E quantiles', B.lag.quantile(q).round(0).to_dict())
print('BN r-T quantiles', B.lagT.quantile(q).round(0).to_dict())
# per-minute min lag to see drift
H['min']=H.t//60000; B['min']=B.E//60000
m=pd.concat([H.groupby('min').lag.min().rename('hl_min'),B.groupby('min').lag.min().rename('bn_min'),H.groupby('min').lag.median().rename('hl_med')],axis=1)
print(m.describe().round(0))
H.to_parquet('live_hl_trades.parquet'); B.to_parquet('live_bn.parquet')
