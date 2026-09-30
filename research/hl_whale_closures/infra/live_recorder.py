# Live recorder: HL l2Book + trades (with users) and Binance futures bookTicker + aggTrade for whale coins,
# plus periodic REST snapshots of whale positions (clearinghouseState). Writes gzip jsonl rotated hourly.
# Each line: {"r": receive_ms_local, "src": "hl"|"bn"|"pos", "m": <raw message>}
import websocket, json, time, os, gzip, threading, urllib.request, sys
from urllib.parse import urlparse
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT=SP+'data/live/'
P=urlparse(os.environ['HTTPS_PROXY'])
SYM=json.load(open(SP+'infra/symmap.json'))
COINS=['PONS','LIT','NIL','XPL','CHIP','USELESS','VVV','MON','NEAR','ZRO','STRK','FARTCOIN','SKR','CC','MET','MINA','XMR','HYPE',
       'PUMP','AZTEC','SAGA','GOAT','2Z','ETHFI','JUP','GRASS','INJ','ZETA','SPX','STBL','ZEC','HEMI','ENA','TAO','WLD']
COINS=[c for c in COINS if SYM.get(c)]
lock=threading.Lock(); files={}
def write(src, m):
    r=int(time.time()*1000); hour=time.strftime('%Y%m%d_%H', time.gmtime(r/1000))
    key=(src,hour)
    with lock:
        f=files.get(key)
        if f is None:
            for k in [k for k in files if k[0]==src]: files.pop(k).close()
            f=files[key]=gzip.open(f'{OUT}{src}_{hour}.jsonl.gz','at')
        f.write(json.dumps({'r':r,'src':src,'m':m},separators=(',',':'))+'\n')
def log(*a):
    print(time.strftime('%H:%M:%S'),*a,flush=True)
def run_ws(name, url, subs, src):
    while True:
        try:
            ws=websocket.WebSocket(sslopt={'ca_certs':'/root/.ccr/ca-bundle.crt'})
            ws.connect(url, http_proxy_host=P.hostname, http_proxy_port=P.port, proxy_type='http', timeout=30)
            for s in subs: ws.send(json.dumps(s))
            log(name,'connected',len(subs))
            last_ping=time.time(); n=0
            while True:
                m=ws.recv()
                if not m: continue
                n+=1
                write(src, json.loads(m))
                if src=='hl' and time.time()-last_ping>30:
                    ws.send(json.dumps({'method':'ping'})); last_ping=time.time()
        except Exception as e:
            log(name,'error',repr(e)[:200]); time.sleep(3)
def hl_subs():
    s=[]
    for c in COINS:
        s.append({'method':'subscribe','subscription':{'type':'l2Book','coin':c}})
        s.append({'method':'subscribe','subscription':{'type':'trades','coin':c}})
    return s
def bn_url():
    st='/'.join(f'{SYM[c].lower()}@bookTicker/{SYM[c].lower()}@aggTrade' for c in COINS)
    return 'wss://fstream.binance.com/stream?streams='+st
def pos_poller():
    import pandas as pd
    b=pd.read_parquet(SP+'data/close_batches.parquet')
    b=b[b.coin.isin(COINS)]
    top=b.groupby('addr').old_val.max().sort_values(ascending=False)
    addrs=list(top.index[:120])
    log('pos poller addrs',len(addrs))
    while True:
        t0=time.time()
        for a in addrs:
            try:
                req=urllib.request.Request('https://api.hyperliquid.xyz/info',data=json.dumps({'type':'clearinghouseState','user':a}).encode(),headers={'Content-Type':'application/json'})
                d=json.loads(urllib.request.urlopen(req,timeout=20).read())
                pos=[x['position'] for x in d.get('assetPositions',[]) if x['position']['coin'] in COINS]
                write('pos',{'user':a,'time':d.get('time'),'pos':pos})
            except Exception as e:
                log('pos err',repr(e)[:100])
            time.sleep(0.6)
        time.sleep(max(0, 300-(time.time()-t0)))
if __name__=='__main__':
    log('coins',len(COINS),COINS)
    th=[threading.Thread(target=run_ws,args=('hl','wss://api.hyperliquid.xyz/ws',hl_subs(),'hl'),daemon=True),
        threading.Thread(target=run_ws,args=('bn',bn_url(),[],'bn'),daemon=True),
        threading.Thread(target=pos_poller,daemon=True)]
    for t in th: t.start()
    while True:
        time.sleep(60)
        with lock:
            for f in files.values(): f.flush()
