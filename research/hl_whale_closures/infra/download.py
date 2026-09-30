# Download Binance USDT-M aggTrades -> sparse 1-second bars (npz), and 1m klines for all symbols.
import json, os, io, zipfile, urllib.request, time, sys, concurrent.futures as cf
import numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT1S=SP+'data/bars1s/'; OUTK=SP+'data/k1m/'
def fetch(u, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(u, timeout=180) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: return None
            time.sleep(2**i)
        except Exception:
            time.sleep(2**i)
    return None
def do_agg(p):
    c,s,d=p
    fn=f'{OUT1S}{s}/{d}.npz'
    if os.path.exists(fn): return p,'skip'
    b=fetch(f'https://data.binance.vision/data/futures/um/daily/aggTrades/{s}/{s}-aggTrades-{d}.zip')
    if b is None: return p,'missing'
    z=zipfile.ZipFile(io.BytesIO(b)); name=z.namelist()[0]
    raw=z.read(name)
    first=raw[:200].decode(errors='ignore')
    hdr=0 if first.startswith('agg_trade_id') else None
    df=pd.read_csv(io.BytesIO(raw), header=hdr, usecols=[1,2,5,6] if hdr is None else ['price','quantity','transact_time','is_buyer_maker'])
    df.columns=['price','qty','ts','ibm']
    if df.ibm.dtype==object: df['ibm']=df.ibm.astype(str).str.lower().eq('true')
    df['sec']=(df.ts//1000).astype(np.int64)
    df['quote']=df.price*df.qty
    df['bq']=np.where(df.ibm, 0.0, df.quote)
    g=df.groupby('sec',sort=True)
    agg=pd.DataFrame({'o':g.price.first(),'h':g.price.max(),'l':g.price.min(),'c':g.price.last(),
                      'qty':g.qty.sum(),'quote':g.quote.sum(),'bq':g.bq.sum(),'n':g.price.size()})
    os.makedirs(OUT1S+s,exist_ok=True)
    np.savez_compressed(fn, sec=agg.index.values.astype(np.int64), o=agg.o.values, h=agg.h.values, l=agg.l.values, c=agg.c.values,
                        qty=agg.qty.values.astype(np.float64), quote=agg.quote.values.astype(np.float64), bq=agg.bq.values.astype(np.float64), n=agg.n.values.astype(np.int32))
    return p,'ok'
def do_k(p):
    s,d=p
    fn=f'{OUTK}{s}/{d}.npz'
    if os.path.exists(fn): return p,'skip'
    b=fetch(f'https://data.binance.vision/data/futures/um/daily/klines/{s}/1m/{s}-1m-{d}.zip')
    if b is None: return p,'missing'
    z=zipfile.ZipFile(io.BytesIO(b)); raw=z.read(z.namelist()[0])
    first=raw[:100].decode(errors='ignore')
    df=pd.read_csv(io.BytesIO(raw), header=0 if first.startswith('open_time') else None)
    df=df.iloc[:,:11]; df.columns=['ot','o','h','l','c','v','ct','qv','n','tbv','tbqv']
    os.makedirs(OUTK+s,exist_ok=True)
    np.savez_compressed(fn, ot=df.ot.values.astype(np.int64), o=df.o.values.astype(float), h=df.h.values.astype(float), l=df.l.values.astype(float), c=df.c.values.astype(float), qv=df.qv.values.astype(float), tbqv=df.tbqv.values.astype(float), n=df.n.values.astype(np.int64))
    return p,'ok'
if __name__=='__main__':
    mode=sys.argv[1]
    if mode=='k':
        sym=json.load(open(SP+'infra/symmap.json'))
        syms=set(v for v in sym.values() if v)|{'BTCUSDT','ETHUSDT','SOLUSDT'}
        days=[d.strftime('%Y-%m-%d') for d in pd.date_range('2026-08-20','2026-09-29')]
        jobs=[(s,d) for s in sorted(syms) for d in days]; fn=do_k; W=16
    else:
        jobs=[tuple(p) for p in json.load(open(SP+'infra/needed_pairs.json'))]
        # biggest first so tail is short
        sizes={tuple(p):z for p,z in json.load(open(SP+'infra/pair_sizes.json'))}
        jobs.sort(key=lambda p:-sizes.get(p,0)); fn=do_agg; W=6
    t0=time.time(); st={}
    with cf.ProcessPoolExecutor(W) as ex:
        for i,(p,r) in enumerate(ex.map(fn,jobs)):
            st[r]=st.get(r,0)+1
            if i%100==0: print(i,len(jobs),st,round(time.time()-t0),flush=True)
    print('done',st,round(time.time()-t0),flush=True)
