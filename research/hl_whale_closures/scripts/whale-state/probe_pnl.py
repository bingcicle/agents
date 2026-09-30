# Probe the meaning of sim_trades.whale_pnl_pct / liq_dist by comparing with HL fills (closedPnl) for a few recent events.
import pandas as pd, numpy as np, json, time, urllib.request
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
s=pd.read_csv(SP+'hl_whale_export_20260930/02_bot_data/sim_trades.csv')
s['t_open_ms']=((pd.to_datetime(s.date_open)-pd.Timedelta(hours=2))-pd.Timestamp('1970-01-01'))//pd.Timedelta('1ms')
tx=pd.read_parquet(SP+'data/whale_txs.parquet',columns=['addr','coin','ts','px','sz','sp','dir','tx_usd','bot_ts'])
r=s[(s.t_open_ms>1790553600000)].sample(12,random_state=1)
def post(d):
    req=urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps(d).encode(),headers={'Content-Type':'application/json'})
    return json.loads(urllib.request.urlopen(req,timeout=20).read())
for _,row in r.iterrows():
    g=tx[(tx.addr==row.whale_addr)&(tx.coin==row.coin)&(abs(tx.bot_ts*1000-row.t_open_ms)<30000)]
    f=post({'type':'userFillsByTime','user':row.whale_addr,'startTime':int(row.t_open_ms-120000),'endTime':int(row.t_open_ms+5000)})
    f=[x for x in f if x['coin']==row.coin]
    time.sleep(0.5)
    if not f: print(row.coin,'no fills', len(g)); continue
    x=f[-1]; px=float(x['px']); sz=float(x['sz']); cp=float(x['closedPnl']); side=1 if 'Long' in x['dir'].split('>')[0] or x['dir']=='Close Long' else -1
    entry=px-cp/(sz*side) if sz else np.nan
    print(row.coin, row.our_side, x['dir'], 'px',px,'entry',round(entry,6),'ret%',round(100*side*(px/entry-1),3),'sim whale_pnl',row.whale_pnl_pct,'liq_dist',row.liq_dist,'nfills',len(f),'ntx',len(g))
