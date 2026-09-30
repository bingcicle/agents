import sys; sys.path.insert(0,SPW:='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-whale-state')
from s0_load import load_sim
from common import *
from stats import cboot
import statsmodels.formula.api as smf
import bt
SYMMAP=json.load(open(SP+'infra/symmap.json')); SI={s:i for i,s in enumerate(SYMS)}
s=load_sim(); s['symbol']=s.coin.map(SYMMAP); s=s[s.symbol.isin(SI)].copy()
s['i']=s.symbol.map(SI).astype(int)
s=s.sort_values(['whale_addr','coin','wtd','open_ms'])
gap=s.groupby(['whale_addr','coin','wtd']).open_ms.diff(); s=s[gap.isna()|(gap>30*60000)].copy()
s=s[s.sw==-1]   # whale SHORT being closed
END=int(pd.Timestamp('2026-09-29 23:59').value//10**6)
def feats(i,dec):
    m=(dec-T0)//60000-1         # last full minute closed before decision
    k5=(dec-600000-T0)//300000  # OI bucket starting <= dec-10min
    out={}
    for Lh in (1,4,12,24):
        r=Cf[:,m]/Cf[:,m-Lh*60]-1; ex=r[i]-np.nanmean(np.delete(r[ALT],np.flatnonzero(ALT==i)))
        out[f'ex{Lh}']=100*ex; out[f'doi{Lh}']=100*(OIf[i,k5]/OIf[i,k5-Lh*12]-1)
    return out
def outc(i,me,Hh,side=-1):
    mx=me+Hh*60
    if mx>=NM: return np.nan,np.nan
    pe,px=Cf[i,me],Cf[i,mx]; alt=Cf[ALT,mx]/Cf[ALT,me]-1; msk=np.isfinite(alt)&(ALT!=i)
    y=100*side*((px/pe-1)-alt[msk].mean())
    fc=bt.fund_cum(i); f=0.0
    te=T0+(me+1)*60000; tx=T0+(mx+1)*60000
    if fc is not None and fc[0][0]<=te:
        k0=np.searchsorted(fc[0],te,'right')-1; k1=np.searchsorted(fc[0],tx,'right')-1; f=100*side*(fc[1][k1]-fc[1][k0])
    return y-0.246-f, f
s=s[(s.open_ms+24*3600000+120000)<=END]
rows=[]
for r in s.itertuples():
    me=int(np.ceil(r.open_ms/60000))+1-T0//60000-1   # minute closing at ceil(dec)+60s
    d=dict(sym=r.symbol,i=r.i,dec=r.open_ms,addr=r.whale_addr,wpnl=r.wpnl,liq=r.liq_dist,me=me)
    for Hh in (4,12,24):
        d[f'y{Hh}'],d[f'f{Hh}']=outc(r.i,me,Hh)
    d.update(feats(r.i,r.open_ms)); rows.append(d)
E=pd.DataFrame(rows); E=E[(E.dec+24*3600000+120000)<=END]
E['day']=pd.to_datetime(E.dec,unit='ms').dt.strftime('%m-%d'); E['week']=pd.to_datetime(E.dec,unit='ms').dt.strftime('%V')
VAR={'D_ORIG':(E.wpnl<0)&(E.liq<5),'D_LEV':(E.wpnl<0)&(E.liq<1.0),'D_NEAR':(E.wpnl<0)&(E.liq<0.3),'D_ANY':E.wpnl<0}
def dedupe(x,hours=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+hours*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
# generic hourly coin-hours over the same window
h0=(int(pd.Timestamp('2026-08-27').value//10**6)-T0)//3600000; h1=(END-T0)//3600000-25
G=[]
for h in range(h0,h1):
    dec=T0+h*3600000
    for i in range(NS):
        if i==IB: continue
        me=(dec-T0)//60000+1
        if not np.isfinite(Cf[i,me]): continue
        G.append((i,dec,me))
G=pd.DataFrame(G,columns=['i','dec','me'])
# vectorized features & outcomes for G
m=(G.dec.values-T0)//60000-1; k5=(G.dec.values-600000-T0)//300000; ii=G.i.values
for Lh in (1,4,12,24):
    r=Cf[:,m]/Cf[:,m-Lh*60]-1; altm=np.nanmean(r[ALT],axis=0)
    G[f'ex{Lh}']=100*(r[ii,np.arange(len(G))]-altm)
    G[f'doi{Lh}']=100*(OIf[ii,k5]/OIf[ii,k5-Lh*12]-1)
for Hh in (4,12,24):
    me=G.me.values; mx=me+Hh*60
    rc=Cf[ii,mx]/Cf[ii,me]-1; ra=np.nanmean(Cf[ALT][:,mx]/Cf[ALT][:,me]-1,axis=0)
    ys=[]; fs=[]
    G[f'y{Hh}']=100*-(rc-ra)-0.246
G['sym']=[SYMS[i] for i in G.i]; G['day']=pd.to_datetime(G.dec,unit='ms').dt.strftime('%m-%d'); G['week']=pd.to_datetime(G.dec,unit='ms').dt.strftime('%V')
G=G[np.isfinite(G.y24)]
# funding approx for G: skip (sensitivity) -> report whale y without funding too
E['y24nf']=E.y24+E.f24
G.to_parquet(W+'partB_generic.parquet'); E.to_parquet(W+'partB_whale.parquet')
print('generic coin-hours',len(G),' generic mean y24 (short hedged, no fund)',G.y24.mean().round(3))
def sqz_any(x):
    return ((x.ex1>=3)&(x.doi1<=-1))|((x.ex4>=5)&(x.doi4<=-2))|((x.ex12>=8)&(x.doi12<=-4))|((x.ex24>=12)&(x.doi24<=-5))
G['sqz']=sqz_any(G); E['sqz']=sqz_any(E)
print('generic SQZ-any coin-hours',G.sqz.sum(),'mean y24',G[G.sqz].y24.mean().round(2), ' y24 w/o sqz',G[~G.sqz].y24.mean().round(3))
out=[]
from sklearn.neighbors import NearestNeighbors
fc=['ex4','ex24','doi4','doi24']
for v,msk in VAR.items():
    x=dedupe(E[msk&np.isfinite(E.y24)])
    res=dict(var=v,n=len(x),ncoin=x.sym.nunique(),nwal=x.addr.nunique(),y24=round(x.y24.mean(),2),med=round(x.y24.median(),2),
             ciC=cboot(x.y24,x.sym),ciD=cboot(x.y24,x.day),y24_nofund=round(x.y24nf.mean(),2),y12=round(x.y12.mean(),2),y4=round(x.y4.mean(),2),
             in_sqz=int(x.sqz.sum()),ex24_med=round(x.ex24.median(),1),doi24_med=round(x.doi24.median(),1),
             top=x.groupby('sym').y24.sum().sort_values().tail(2).round(1).to_dict())
    # (1) same coin +-12h control (generic hourly entries, no funding) compare nofund
    d1=[]
    for r in x.itertuples():
        g=G[(G.i==r.i)&(np.abs(G.dec-r.dec)<=12*3600000)&(np.abs(G.dec-r.dec)>=3600000)]
        if len(g): d1.append(r.y24nf-g.y24.mean())
        else: d1.append(np.nan)
    x=x.assign(d1=d1); z=x[np.isfinite(x.d1)]
    res['d_samecoin12h']=round(z.d1.mean(),2); res['ci_d1C']=cboot(z.d1,z.sym)
    # (2) NN on features, other coins, same week, no whale event within 24h in that coin
    wev=E[['i','dec']].values
    dd=[]
    for r in x.itertuples():
        g=G[(G.week==r.week)&(G.i!=r.i)].dropna(subset=fc)
        if len(g)<50: dd.append(np.nan); continue
        sc=g[fc].std().values; nn=NearestNeighbors(n_neighbors=10).fit(g[fc].values/sc)
        q=np.array([[getattr(r,c) for c in fc]])/sc
        if not np.all(np.isfinite(q)): dd.append(np.nan); continue
        _,ix=nn.kneighbors(q); dd.append(r.y24nf-g.y24.values[ix[0]].mean())
    x=x.assign(d2=dd); z=x[np.isfinite(x.d2)]
    res['d_nn']=round(z.d2.mean(),2); res['ci_d2C']=cboot(z.d2,z.sym); res['ci_d2D']=cboot(z.d2,z.day)
    # (3) OLS stacked: generic (w=0) + whale events (w=1)
    st=pd.concat([G[['sym','day','y24']+fc].assign(w=0), x[['sym','day']+fc].assign(y24=x.y24nf,w=1)]).dropna()
    mod=smf.ols('y24 ~ w + ex4 + ex24 + doi4 + doi24',data=st).fit(cov_type='cluster',cov_kwds={'groups':pd.factorize(st.sym)[0]})
    res['ols_w']=round(mod.params['w'],2); res['ols_t']=round(mod.tvalues['w'],2); res['ols_ex24']=round(mod.params['ex24'],3); res['ols_t_ex24']=round(mod.tvalues['ex24'],2); res['ols_doi24_t']=round(mod.tvalues['doi24'],2)
    out.append(res); print(res,flush=True)
pd.DataFrame(out).to_csv(W+'partB_results.csv',index=False)
