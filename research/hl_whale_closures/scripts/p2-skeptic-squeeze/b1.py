from mybt import *; from st2 import *
import warnings; warnings.filterwarnings('ignore')
s=pd.read_csv(SP+'hl_whale_export_20260930/02_bot_data/sim_trades.csv')
s['sym']=s.coin.map(SYMMAP); SI={x:i for i,x in enumerate(SY)}; s=s[s.sym.isin(SI)].copy(); s['i']=s.sym.map(SI)
# TZ check: entry_px vs Binance close at various shifts, only after 13.09 (Binance priced)
for sh in (0,1,2,3):
    t=((pd.to_datetime(s.date_open)-pd.Timedelta(hours=sh)).astype('datetime64[ms]').astype('int64'))
    m=((t-T0)//60000).values; ok=(pd.to_datetime(s.date_open)>'2026-09-14').values&(m<NM)
    b=C[s.i.values[ok],m[ok]]; e=np.abs(np.log(s.entry_px.values[ok]/b)); print('shift',sh,'median |dev| %',round(100*np.nanmedian(e),3))
s['dec']=(pd.to_datetime(s.date_open)-pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
END=T0+NM*60000
def outcome(i,dec,Hh=24,delay_s=60):
    me=int(np.ceil((dec+delay_s*1000-T0)/60000))-1   # minute whose close >= dec+delay
    mx=me+Hh*60
    if mx>=NM: return None
    pe=C[:,me]; px_=C[:,mx]
    if not (np.isfinite(pe[i]) and np.isfinite(px_[i])): return None
    ra=px_/pe-1; msk=np.isfinite(ra); msk[IB]=False; msk[i]=False
    fl=fund_long(i,T0+(me+1)*60000,T0+(mx+1)*60000)
    rc=ra[i]; ew=np.nanmean(ra[msk])
    return dict(y=-100*(rc-ew)-0.246+(0 if np.isnan(fl) else fl), y_btc=-100*(rc-ra[IB])-0.246+(0 if np.isnan(fl) else fl),
                y_raw=-100*rc-0.146+(0 if np.isnan(fl) else fl), y_md=-100*(rc-np.nanmedian(ra[msk]))-0.246+(0 if np.isnan(fl) else fl), fl=fl)
def pre(i,dec,Lh):
    m=int((dec-T0)//60000)-1
    a=C[:,m]; b=C[:,m-Lh*60]; r=a/b-1; msk=np.isfinite(r); msk[IB]=False; msk[i]=False
    return 100*(r[i]-np.nanmean(r[msk]))
rows=[]
for r in s.itertuples():
    o=outcome(r.i,r.dec)
    if o is None: continue
    o.update(sym=r.sym,i=r.i,t=r.dec,dec=r.dec,side=r.our_side,wpnl=r.whale_pnl_pct,liq=r.liq_dist,addr=r.whale_addr,ex24=pre(r.i,r.dec,24),ex4=pre(r.i,r.dec,4))
    rows.append(o)
E=add(pd.DataFrame(rows)); E.to_parquet(W+'simE.parquet'); print('events',len(E),E.side.value_counts().to_dict())
