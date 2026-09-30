import numpy as np, pandas as pd
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/p2-skeptic-fastmove/'
d=pd.read_parquet(W+'ev.parquet')
dt=pd.to_datetime(d.ts,unit='ms'); d['mon']=dt.dt.month; d['day']=dt.dt.strftime('%m-%d'); d['wk']=dt.dt.strftime('%V')
d['per']=np.where(d.mon<=6,'TR','TE')
for e in ('e1','e_c1','e_o2'): d['g_'+e]=100*(d.x/d[e]-1)
d['bounce']=100*(d.e1/d.c0-1)
d['neu']=d.g_e1-d.idx
def cci(x,cl,B=2000):
    x=np.asarray(x,float); ok=~np.isnan(x); x=x[ok]; cl=pd.factorize(np.asarray(cl)[ok])[0]; k=cl.max()+1
    S=np.bincount(cl,x,k); n=np.bincount(cl,minlength=k); w=np.random.default_rng(0).multinomial(k,np.ones(k)/k,B)
    m=(w@S)/(w@n); return np.round(np.percentile(m,[5,95]),3)
def rep(g,col,lab):
    x=g[col]; 
    print(f'{lab:28s} n={x.notna().sum():5d} mean={x.mean():+.3f} med={x.median():+.3f} ciCoin={cci(x,g.sym)} ciDay={cci(x,g.day)} ciWk={cci(x,g.wk)}')
pd.set_option('display.width',220)
for (X,N) in ((1.5,3),(2,5),(3,3)):
    g=d[(d.X==X)&(d.N==N)]
    print(f'\n===== DUMP X{X} N{N}: n={len(g)}, NaN exit={g.x.isna().sum()}, NaN entry={g.e1.isna().sum()}, zero-trade entry min={(g.nt1==0).sum()}')
    for p in ('TR','TE'):
        gp=g[g.per==p]
        for col in ('g_e1','g_e_c1','g_e_o2','neu','bounce'): rep(gp,col,f'{p} {col}')
    print(g.pivot_table(index='mon',values=['g_e1','g_e_c1','neu','bounce'],aggfunc='mean').round(3).T.to_string())
    # OIDN variants
    for lag in (0,5,10,15):
        f=g[g[f'oi{lag}']<=-0.5]
        print(f'  OIDN lag{lag}: TR n={ (f.per=="TR").sum()} g={f[f.per=="TR"].g_e1.mean():+.3f} c1={f[f.per=="TR"].g_e_c1.mean():+.3f} | TE n={(f.per=="TE").sum()} g={f[f.per=="TE"].g_e1.mean():+.3f} c1={f[f.per=="TE"].g_e_c1.mean():+.3f}  TE w/o 08-22 {f[(f.per=="TE")&(f.day!="08-22")].g_e1.mean():+.3f}')
    print('  corr(oi5,g)',g[['oi5','oi_fut','g_e1']].corr().round(3).iloc[:,-1].to_dict())
