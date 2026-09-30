# fetch HL hourly funding 2026-08-20 .. 2026-09-30 for all HL coins with a Binance symbol (proxy for Binance funding)
import json, time, urllib.request, os
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/long-horizon/funding/'
sym=json.load(open(SP+'infra/symmap.json'))
t_start=1787184000000  # 2026-08-20 00:00 UTC
t_end=1790812800000    # 2026-10-01
for coin,s in sym.items():
    if not s: continue
    fn=W+coin+'.json'
    if os.path.exists(fn): continue
    out=[]; t=t_start
    for _ in range(6):
        req=urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps({'type':'fundingHistory','coin':coin,'startTime':t}).encode(),headers={'Content-Type':'application/json'})
        try:
            r=json.load(urllib.request.urlopen(req,timeout=30))
        except Exception as e:
            print(coin,'err',e); time.sleep(2); continue
        if not r: break
        out+=r; t=r[-1]['time']+1
        if len(r)<500 or t>t_end: break
        time.sleep(0.3)
    json.dump(out,open(fn,'w'))
    print(coin,len(out),flush=True)
    time.sleep(0.3)
