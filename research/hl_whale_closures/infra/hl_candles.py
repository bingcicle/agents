# Fetch Hyperliquid candles (1m/5m/15m, max 5000 most recent each) + funding history for all coins.
import json, urllib.request, time, os
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT=SP+'data/hl/'; os.makedirs(OUT,exist_ok=True)
coins=json.load(open(SP+'all_coins.json'))
def post(body):
    for i in range(2):
        try:
            r=urllib.request.urlopen(urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'}),timeout=60)
            return json.loads(r.read())
        except Exception as e:
            time.sleep(2**i)
    return None
END=int(time.time()*1000)
res={}
for c in coins:
    for iv,mins in [('1m',1),('5m',5),('15m',15)]:
        fn=f'{OUT}{c}_{iv}.json'
        if os.path.exists(fn): continue
        start=END-5000*mins*60*1000
        d=post({'type':'candleSnapshot','req':{'coin':c,'interval':iv,'startTime':start,'endTime':END}})
        json.dump(d,open(fn,'w')); time.sleep(0.25)
    fn=f'{OUT}{c}_funding.json'
    if not os.path.exists(fn):
        d=post({'type':'fundingHistory','coin':c,'startTime':END-40*86400*1000}); json.dump(d,open(fn,'w')); time.sleep(0.25)
    print(c, flush=True)
print('done')
