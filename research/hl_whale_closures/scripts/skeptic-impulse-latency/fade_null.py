"""Null: capitulation pattern WITHOUT a whale tx (>=600 s from any >=$1k whale tx in the coin): price fell >=a% over
[s-61,s-1] and kept falling >=b% to s+120 -> LONG at s+300 worst-3s, exit s+3600 worst-3s; one open position per coin."""
from common_s import *
from s1 import dense
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
T=pd.read_parquet(OUT+'txs1k.parquet')
A=pd.read_parquet(OUT+'anchors.parquet')
sds=A[['symbol','day']].drop_duplicates().values
rows=[]
for sym,day in sds:
    if not os.path.exists(f'{SP}data/bars1s/{sym}/{day}.npz'): continue
    base,N,cf,hw,lw=dense(sym,day)
    D0=int(pd.Timestamp(day).timestamp())
    lc=np.log(cf)*100
    s=np.arange(D0,D0+86400); i=s-base
    pre=lc[i-61]-lc[i-1]          # fall over 60 s (positive = fell)
    imp=lc[i-1]-lc[i+120]         # continued fall
    ws=T[(T.symbol==sym)&(T.ts>=(D0-4000)*1000)&(T.ts<(D0+90000)*1000)].ts.values//1000
    near=np.zeros(len(s),bool)
    for x in np.unique(ws):
        a=max(0,x-D0-600); b=min(len(s),x-D0+601)
        if b>a: near[a:b]=True
    for a_,b_ in ((0.5,0.3),(1.0,0.3)):
        ok=np.where((pre>=a_)&(imp>=b_)&~near)[0]
        busy=-1
        for k in ok:
            if s[k]<busy: continue
            e=hw[i[k]+300]; x=lw[i[k]+3600]
            if not(np.isfinite(e) and np.isfinite(x)): continue
            rows.append(dict(symbol=sym,day=day,coin=sym,sec=s[k],a=a_,g=(np.log(x)-np.log(e))*100,pre=pre[k],imp=imp[k]))
            busy=s[k]+3600
Nl=pd.DataFrame(rows); Nl['per']=np.where(Nl.day<='2026-09-21','D','T'); Nl['wdir']=-1
Nl['s1']=Nl.sec+300; Nl['s2']=Nl.sec+3600
Nl=add_alt(Nl,'s1','s2','alt'); Nl['g_alt']=Nl.g-Nl.alt
Nl.to_parquet(OUT+'fade_null.parquet')
print(Nl.groupby(['a','per']).agg(n=('g','size'),gross=('g','mean'),med=('g','median'),gross_alt=('g_alt','mean'),alt=('alt','mean'),pre=('pre','mean')).round(3))
