# Binance USDT-M daily metrics (5-min open interest, long/short ratios, taker vol ratio) for all symbols 2026-03-01..2026-09-29
import json, os, io, zipfile, urllib.request, time, concurrent.futures as cf
import pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT=SP+'data/metrics/'
def fetch(u):
    for i in range(4):
        try:
            with urllib.request.urlopen(u, timeout=60) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: return None
            time.sleep(2**i)
        except Exception: time.sleep(2**i)
    return None
def job(p):
    s,d=p; fn=f'{OUT}{s}/{d}.csv'
    if os.path.exists(fn): return 'skip'
    b=fetch(f'https://data.binance.vision/data/futures/um/daily/metrics/{s}/{s}-metrics-{d}.zip')
    if b is None: return 'missing'
    z=zipfile.ZipFile(io.BytesIO(b)); os.makedirs(OUT+s,exist_ok=True)
    open(fn,'wb').write(z.read(z.namelist()[0])); return 'ok'
sym=json.load(open(SP+'infra/symmap.json'))
syms=sorted(set(v for v in sym.values() if v)|{'BTCUSDT','ETHUSDT','SOLUSDT'})
days=[d.strftime('%Y-%m-%d') for d in pd.date_range('2026-03-01','2026-09-29')]
jobs=[(s,d) for s in syms for d in days]
st={}; t0=time.time()
with cf.ThreadPoolExecutor(24) as ex:
    for i,r in enumerate(ex.map(job,jobs)):
        st[r]=st.get(r,0)+1
        if i%2000==0: print(i,len(jobs),st,round(time.time()-t0),flush=True)
print('done',st,round(time.time()-t0),flush=True)
