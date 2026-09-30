from common_s import *
A=pd.read_parquet(OUT+'anchors.parquet'); M=pd.read_parquet(OUT+'ms_s.parquet')
M=M.merge(A[['ep','addr','coin','symbol','day','per','td','rel24','tx_usd','detect_src','bot_ts','ts']].rename(columns={'ep':'key'}),on='key')
EV=M[M.kind=='ev'].copy(); PL=M[M.kind!='ev'].copy()
print('rel24>=4 anchors with ms data:',(EV.rel24>=4).sum(),'of',(A.rel24>=4).sum(), ' D',((EV.rel24>=4)&(EV.per=='D')).sum(),'T',((EV.rel24>=4)&(EV.per=='T')).sum())
COST=0.146
def mk(d,L,H,ent='a',ex='x',dedupe=True):
    d=d.copy(); d['g']=d[f'{ex}{H}']-d[f'{ent}{L}']; d=d[d.g.notna()].sort_values('t0')
    if dedupe:
        keep=[];busy={}
        for r in d.itertuples():
            if busy.get(r.coin,0)>r.t0: continue
            keep.append(r.Index); busy[r.coin]=r.t0+H*1000
        d=d.loc[keep]
    return d
pd.set_option('display.width',250)
for L in (300,1000,2000):
    d=mk(EV[EV.rel24>=4],L,300)
    d['s1']=(d.t0+L)//1000; d['s2']=(d.t0+300000)//1000
    d=add_alt(d,'s1','s2','alt'); d['btc']=btc_ret(d.t0+L,d.t0+300000.)
    d['net']=d.g-COST; d['net_alt']=d.g-d.wdir*d.alt-COST; d['net_btc']=d.g-d.wdir*d.btc-COST
    for per in ('D','T'):
        x=d[d.per==per]
        print(battery(x,'net',f'FOLLOW rel24>=4 L={L}ms H=300 touch->touch dedupe {per}'))
        pl=PL[PL.key.isin(x.key)]; pg=pl.x300-pl[f'a{L}']
        print(f'   gross {x.g.mean():+.3f} | placebo gross {pg.mean():+.3f} (n={pg.notna().sum()}) | alt in our dir {(x.wdir*x.alt).mean():+.3f} net_alt {x.net_alt.mean():+.3f} (med {x.net_alt.median():+.3f}) | net_btc {x.net_btc.mean():+.3f}')
        print(f'   whale sells (we SHORT) n={(x.wdir<0).sum()} net {x[x.wdir<0].net.mean():+.3f} | whale buys (we LONG) n={(x.wdir>0).sum()} net {x[x.wdir>0].net.mean():+.3f}')
        for fee in (0.10,0.04): print(f'   fee {fee}: net {x.g.mean()-fee:+.3f}')
        if L==300: d.to_parquet(OUT+'cand_L300.parquet')
