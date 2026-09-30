# For every event, fetch the whale's own fills around the event tx from the HL public API (userFillsByTime) and derive
# the whale's entry price from closedPnl: entry = px - closedPnl/(sz*side) -> whale PnL % at the close (price return from entry).
# Only the 10k most recent fills per wallet are served, so heavy wallets come back empty (coverage bias noted).
import json, time, urllib.request, os, numpy as np, pandas as pd, requests
SESS=requests.Session()
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state/'
ev=pd.read_parquet(W+'events_state.parquet')
out_fn=W+'pnl_api.jsonl'
done=set()
if os.path.exists(out_fn):
    for l in open(out_fn): done.add(json.loads(l)['h'])
def post(d):
    t=time.time(); r=SESS.post('https://api.hyperliquid.xyz/info',json=d,timeout=30)
    if r.status_code!=200: print('http',r.status_code,flush=True); raise Exception(r.status_code)
    TT.append(time.time()-t); return r.json()
TT=[]
f=open(out_fn,'a')
for _,r in ev.sort_values('ts').iterrows():
    if r.h in done: continue
    for attempt in range(4):
        try:
            fills=post({'type':'userFillsByTime','user':r.addr,'startTime':int(r.ts-3000),'endTime':int(r.ts+3000)}); break
        except Exception as e:
            time.sleep(5*(attempt+1)); fills=None
    rec={'h':r.h,'addr':r.addr,'coin':r.coin,'ts':int(r.ts),'n':None}
    if fills is not None:
        fl=[x for x in fills if x['coin']==r.coin and x['dir'] in ('Close Long','Close Short','Long > Short','Short > Long')]
        rec['n']=len(fl)
        if fl:
            # prefer fills of the same order time as the event tx
            fl.sort(key=lambda x: abs(x['time']-r.ts))
            t_best=fl[0]['time']; sel=[x for x in fl if x['time']==t_best]
            sz=sum(float(x['sz']) for x in sel); cp=sum(float(x['closedPnl']) for x in sel); vw=sum(float(x['sz'])*float(x['px']) for x in sel)/sz
            side=1 if sel[0]['dir'] in ('Close Long','Long > Short') else -1
            entry=vw-cp/(sz*side)
            rec.update(dict(t_fill=t_best, px=vw, sz=sz, closedPnl=cp, entry=entry, pnl_pct=100*side*(vw/entry-1), side=side,
                            start_pos=float(sel[0]['startPosition']), liq=any('liquidation' in x for x in sel)))
    f.write(json.dumps(rec)+'\n'); f.flush()
    time.sleep(1.0)
    if len(TT)%50==0: print(len(TT),'mean req s',np.mean(TT[-50:]),flush=True)
print('done')
