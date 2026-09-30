import os, io, zipfile, urllib.request, pandas as pd, concurrent.futures as cf
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-squeeze/funding/'
syms=sorted(os.listdir(SP+'data/k1m'))
months=['2026-0%d'%m for m in range(3,10)]
def get(sym):
    fn=W+sym+'.csv'
    if os.path.exists(fn): return sym,'cached'
    parts=[]
    for m in months:
        u=f'https://data.binance.vision/data/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{m}.zip'
        try:
            b=urllib.request.urlopen(u,timeout=30).read()
            z=zipfile.ZipFile(io.BytesIO(b)); parts.append(pd.read_csv(z.open(z.namelist()[0])))
        except Exception as e:
            pass
    if parts:
        pd.concat(parts).to_csv(fn,index=False); return sym,len(parts)
    return sym,0
with cf.ThreadPoolExecutor(8) as ex:
    for r in ex.map(get,syms): print(r,flush=True)
