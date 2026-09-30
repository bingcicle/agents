from mybt import *; from st2 import *
import warnings; warnings.filterwarnings('ignore')
exec(open('b1.py').read().split("rows=[]")[0].split("# TZ check")[0])  # load s
E=pd.read_parquet('simE.parquet'); E=E[E.dec<=pd.Timestamp('2026-09-28 23:57').value//10**6]
def dd(x,h=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+h*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
S=E[E.side=='LONG']
def prof(D,tag):
    res={}
    for off in (-24,-12,-6,-3,-1,0,1,3,6,12,24):
        v=[]
        for r in D.itertuples():
            m0=int(np.ceil((r.dec+60000-T0)/60000))-1+off*60; mx=m0+1440
            if mx>=NM or m0<1: v.append(np.nan); continue
            ra=C[:,mx]/C[:,m0]-1; msk=np.isfinite(ra); msk[IB]=False; msk[r.i]=False
            v.append(-100*(ra[r.i]-np.nanmean(ra[msk]))-0.246)
        v=np.array(v); ok=np.isfinite(v); res[off]=(round(np.nanmean(v),2),cb(v[ok],D.sym.values[ok]))
    print(tag,res)
prof(dd(S[(S.wpnl<0)&(S.liq<5)]),'D_ORIG (no funding) offset h ->')
prof(dd(S[(S.wpnl<0)&(S.liq>=10)]),'loss liq>=10')
prof(dd(S[(S.wpnl>=0)&(S.liq<5)]),'profit liq<5')
