import io, zipfile, time, urllib.request, os, numpy as np, pandas as pd
from concurrent.futures import ThreadPoolExecutor
OUT='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-impulse-latency/'
A=pd.read_parquet(OUT+'anchors.parquet')
m=(A.rel24>=2)|(A.td>=0.3)
sds=sorted(set(map(tuple,A[m][['symbol','day']].values)))
def job(sd):
    sym,day=sd
    fn=f'{OUT}agg/{sym}_{day}.npz'
    if os.path.exists(fn): return 'skip'
    url=f'https://data.binance.vision/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{day}.zip'
    for a in range(4):
        try:
            with urllib.request.urlopen(url,timeout=180) as r: raw=r.read(); break
        except Exception as e:
            err=e; time.sleep(5*(a+1))
    else:
        return f'FAIL {sym} {day} {err}'
    z=zipfile.ZipFile(io.BytesIO(raw)); nm=z.namelist()[0]
    with z.open(nm) as f: head=f.read(100).decode()
    with z.open(nm) as f:
        if head.startswith('agg_trade_id'):
            df=pd.read_csv(f)
        else:
            df=pd.read_csv(f,header=None,names=['agg_trade_id','price','quantity','first_trade_id','last_trade_id','transact_time','is_buyer_maker'])
    bm=df.is_buyer_maker.astype(str).str.lower().isin(['true','1']).values
    o=np.argsort(df.transact_time.values,kind='stable')
    np.savez(fn,t=df.transact_time.values[o].astype(np.int64),p=df.price.values[o].astype(np.float64),
             q=df.quantity.values[o].astype(np.float32),bm=bm[o],id=df.agg_trade_id.values[o].astype(np.int64))
    return f'ok {sym} {day} {len(df)}'
with ThreadPoolExecutor(5) as ex:
    for i,r in enumerate(ex.map(job,sds)):
        if not r.startswith('ok') or i%25==0: print(i,r,flush=True)
print('done',len(sds))
