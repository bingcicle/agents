import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from myload import *; from mystats import *
t=twap()
t['net']=t.s_net_official_pct; t['gross']=t.s_gross_tape_pct; t['cost']=t.s_costs_pct
t['hd']=(t.s_status=='verified')&t.net.notna()&~t.s_flags.fillna('').str.split(';').apply(lambda L:'partial' in L)&(t.s_exit_reason_tape!='cancelled')&(t.s_entry_src!='none')&(t.s_exit_src!='none')
t['day']=t['loc'].dt.strftime('%m-%d'); t['sign']=np.where(t.our_side=='LONG',1,-1)
REG=pd.Timestamp('2026-09-17 10:32'); END=pd.Timestamp('2026-09-30 21:07:06'); days=(END-REG).total_seconds()/86400
t['t0']=t.s_entry_ts_ms.fillna(t.entry_ts_ms); t['t1']=t.s_exit_ts_ms.fillna(t.t0+3600000)
rows=[]
for i,r in t[(t['loc']>=REG)&t.hd].drop_duplicates('twap_id').iterrows():
    sym=SYM.get(r.coin)
    if not sym: rows.append((r.twap_id,np.nan,np.nan,np.nan,np.nan)); continue
    c=ret(sym,r.t0,r.t1,r.sign); b=ret('BTCUSDT',r.t0,r.t1,r.sign); a=altidx(r.t0,r.t1,r.sign)
    # placebo: all 60-min windows (our side) on same coin within +-24h of entry, gross
    k=k1m(sym); pl=np.nan
    if k is not None:
        m0=(int(r.t0)//60000)*60000; w=k.o.loc[m0-86400000:m0+86400000+3600000]
        v=w.values; h=60
        if len(v)>h+100: pl=np.nanmean(r.sign*100*(v[h:]/v[:-h]-1))
    rows.append((r.twap_id,c,b,a,pl))
D=pd.DataFrame(rows,columns=['twap_id','coin_ret','btc','alt','plc']).set_index('twap_id')
t=t.join(D,on='twap_id')
def rep(d,name):
    d=d.sort_values('t0'); x=d.net.values; n=len(x)
    if n==0: print(name,'n=0'); return
    k=max(1,int(round(n*0.1))); xs=np.sort(x)[::-1]
    print(f'{name}: n={n} w={d.whale_addr.nunique()} days={d.day.nunique()} coins={d.coin.nunique()} med={np.median(x):+.3f} mean={x.mean():+.3f} win={np.mean(x>0):.2f} sum={x.sum():+.2f} $/mo={x.sum()*10/days*30:+.0f} woTop10={xs[k:].sum():+.2f} L/S={sum(d.our_side=="LONG")}/{sum(d.our_side=="SHORT")}')
    if n>=3:
        for c in ['whale_addr','day','coin']:
            lo,hi,p=cboot(x,d[c].values,np.mean); lo2,hi2,p2=cboot(x,d[c].values,np.median)
            print(f'   CI90 by {c[:6]}: mean [{lo:+.3f},{hi:+.3f}] med [{lo2:+.3f},{hi2:+.3f}]')
        h=n//2; print(f'   halves mean {x[:h].mean():+.3f}/{x[h:].mean():+.3f} med {np.median(x[:h]):+.3f}/{np.median(x[h:]):+.3f}')
        for c in ['whale_addr','day','coin']:
            g=d.groupby(c).net.sum(); print(f'   max share of sum by {c[:6]}: {g.max()/x.sum() if x.sum()>0 else np.nan:.2f} ({str(g.idxmax())[:10]} {g.max():+.2f})')
        print('   signflip P (gross, by wallet):', signflip(d.gross.values,d.whale_addr.values,d.cost.values), ' by day:', signflip(d.gross.values,d.day.values,d.cost.values))
        for col in ['coin_ret','btc','alt','plc']:
            y=d[col]; print(f'   {col}: mean {y.mean():+.3f} (n={y.notna().sum()})', end='')
        print()
        e=(d.gross-d.plc).dropna(); print(f'   gross-placebo(same coin +-24h, all 60m windows): mean {e.mean():+.3f} med {e.median():+.3f}', 'CI day', np.round(cboot(e.values,d.loc[e.index,'day'].values)[:2],3))
        e=(d.gross-d.alt).dropna(); print(f'   gross-altEW: mean {e.mean():+.3f}', 'CI day', np.round(cboot(e.values,d.loc[e.index,'day'].values)[:2],3))
        e=(d.net-d.btc).dropna(); print(f'   net-BTC: mean {e.mean():+.3f}')
for card in ['T1_твап_відкриття','T1_твап_відкриття_15','T1_твап_відкриття_20','T2_твап_скорочення','T2_твап_скорочення_15','T2_твап_скорочення_20']:
    rep(t[(t.strategy==card)&(t['loc']>=REG)&t.hd],card+' OOS')
# dose within T1: move bins
d=t[(t.strategy=='T1_твап_відкриття')&(t['loc']>=REG)&t.hd]
d['bin']=pd.cut(d.move_pct,[0,1.25,1.5,2,3,10])
print(d.groupby('bin').net.agg(['size','mean','median','sum']))
d2=t[(t.strategy=='T2_твап_скорочення')&(t['loc']>=REG)&t.hd]
d2['bin']=pd.cut(d2.move_pct,[0,1.25,1.5,2,3,10])
print(d2.groupby('bin').net.agg(['size','mean','median','sum']))
# symmetric "fade TWAP move>=1.5" (T1_15 u T2_15)
u=t[(t.strategy.isin(['T1_твап_відкриття_15','T2_твап_скорочення_15']))&(t['loc']>=REG)&t.hd]
rep(u,'FADE>=1.5 (T1_15+T2_15)')
u=t[(t.strategy.isin(['T1_твап_відкриття','T2_твап_скорочення']))&(t['loc']>=REG)&t.hd]
rep(u,'FADE any (T1+T2)')
t.to_pickle('t.pkl')
print(t[(t.strategy=='T1_твап_відкриття_15')&(t['loc']>=REG)&t.hd][['date_entry','coin','whale_addr','move_pct','net','gross','coin_ret','btc','alt','plc']].assign(whale_addr=lambda x:x.whale_addr.str[:10]).round(3).to_string())
