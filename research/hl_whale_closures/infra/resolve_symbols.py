# Resolve HL coin -> Binance USDT-M futures symbol by probing 1m kline daily files on data.binance.vision
import json, urllib.request, concurrent.futures as cf
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
known=json.load(open(SP+'symmap_known.json'))
coins=json.load(open(SP+'all_coins.json'))
def exists(sym, day):
    u=f'https://data.binance.vision/data/futures/um/daily/klines/{sym}/1m/{sym}-1m-{day}.zip'
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,method='HEAD'),timeout=30); return r.status==200
    except Exception: return False
def cands(c):
    out=[]
    if c in known: out.append(known[c])
    base=c[1:] if c.startswith('k') and c[1:].isupper() else c
    for s in [base+'USDT','1000'+base+'USDT','1000000'+base+'USDT', c+'USDT']:
        if s not in out: out.append(s)
    return out
def resolve(c):
    for s in cands(c):
        for d in ['2026-09-28','2026-09-20','2026-09-10','2026-09-01']:
            if exists(s,d): return c,s
    return c,None
res={}
with cf.ThreadPoolExecutor(16) as ex:
    for c,s in ex.map(resolve, coins): res[c]=s
json.dump(res,open(SP+'infra/symmap.json','w'),indent=0)
print('resolved',sum(v is not None for v in res.values()),'/',len(res))
print('missing',[c for c,v in res.items() if v is None])
