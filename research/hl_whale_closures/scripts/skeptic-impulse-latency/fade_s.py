from common_s import *
A=pd.read_parquet(OUT+'anchors.parquet'); R=pd.read_parquet(OUT+'s1_anchor.parquet')
X=A.merge(R,on='ep'); X['sec']=X.ts//1000
X['pre60']=-X['m-60']
X['lag']=X.bot_ts-X.ts/1000
COST=0.146
def trades(m,K=300,H=3600,dedupe=True):
    d=X[m].copy(); d['g']=d[f'ae{K}']-d[f'ax{H}']; d=d[d.g.notna()].sort_values('ts')
    if dedupe:
        keep=[];busy={}
        for r in d.itertuples():
            if busy.get(r.coin,0)>r.ts: continue
            keep.append(r.Index); busy[r.coin]=r.ts+H*1000
        d=d.loc[keep]
    d['s1']=d.sec+K; d['s2']=d.sec+H
    d=add_alt(d,'s1','s2','alt')
    d['btc']=btc_ret(d.s1.values*1000.,d.s2.values*1000.)
    # fade position is AGAINST whale: our dir = -wdir. index return in our dir = -wdir*alt
    d['g_alt']=d.g-(-d.wdir*d.alt); d['g_btc']=d.g-(-d.wdir*d.btc)
    d['net']=d.g-COST; d['net_alt']=d.g_alt-COST; d['net_btc']=d.g_btc-COST
    return d
pd.set_option('display.width',250)
for nm,sz in (('td>=0.2',X.td>=0.2),('rel24>=1',X.rel24>=1)):
    for lo in (False,True):
        m=sz&(X.pre60>=0.5)&(X.m120>=0.3)
        if lo: m&=X.wdir<0
        d=trades(m)
        for per in ('D','T'):
            x=d[d.per==per]
            print(battery(x,'net',f'FADE {nm} pre60>=0.5 m120>=0.3 +300->+3600 {"LONGonly" if lo else "both"} {per}'))
            print(f'   alt-neutral net mean {x.net_alt.mean():+.3f} med {x.net_alt.median():+.3f} | BTC-neutral {x.net_btc.mean():+.3f} | alt move in our dir {(-x.wdir*x.alt).mean():+.3f} | LONG n={(x.wdir<0).sum()} net {x[x.wdir<0].net.mean():+.3f}; SHORT n={(x.wdir>0).sum()} net {x[x.wdir>0].net.mean():+.3f}')
            xl=x[x.lag<=290]; print(f'   known by +290s (bot_ts): n={len(xl)} net {xl.net.mean():+.3f}; detect_src {x.detect_src.value_counts().to_dict()}')
        if lo:
            print(battery(d[d.per=='T'],'net_alt','   T alt-neutral battery'))
d.to_parquet(OUT+'fade_rel24_long.parquet')
