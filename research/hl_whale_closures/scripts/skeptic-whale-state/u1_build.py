# Independent rebuild of universe U from whale_txs + outcomes on my minute matrix + whale PnL from raw HL API fills.
import sys; sys.path.insert(0,'.')
from sk import *
tx=pd.read_parquet(SP+'data/whale_txs.parquet',columns=['addr','coin','h','ts','px','sz','sp','dir','liq','bot_ts','batch_ratio','depth_usd','tx_usd','detect_src'])
tx=tx[tx.dir.isin(['Close Short','Close Long'])&(tx.liq==0)].copy()
tx['wdir']=np.where(tx.dir=='Close Short',1,-1)
tx=tx.sort_values(['addr','coin','wdir','ts']).reset_index(drop=True)
# rolling 60 s window (ts-60s, ts] per addr/coin/wdir
r_sz=np.zeros(len(tx)); r_usd=np.zeros(len(tx)); p0=np.zeros(len(tx))
for key,g in tx.groupby(['addr','coin','wdir']).indices.items():
    t=tx.ts.values[g]; sz=tx.sz.values[g]; usd=tx.tx_usd.values[g]; sp=np.abs(tx.sp.values[g])
    cs=np.concatenate([[0],np.cumsum(sz)]); cu=np.concatenate([[0],np.cumsum(usd)])
    i0=np.searchsorted(t,t-60000,'right')
    idx=np.arange(len(g))
    r_sz[g]=cs[idx+1]-cs[i0]; r_usd[g]=cu[idx+1]-cu[i0]; p0[g]=sp[i0]
tx['r_pct']=100*r_sz/np.where(p0>0,p0,np.nan); tx['r_usd']=r_usd
tx['symbol']=tx.coin.map(SYM)
g=tx[(tx.r_pct>=5)&(tx.r_usd>=5000)&(tx.batch_ratio>=1)&tx.symbol.notna()].copy()
g=g.sort_values(['addr','coin','wdir','ts'])
gap=g.groupby(['addr','coin','wdir']).ts.diff()
ev=g[gap.isna()|(gap>30*60000)].copy()
ev['dec_ms']=np.maximum(ev.ts+1500,(ev.bot_ts*1000).astype(np.int64))
ev['day']=pd.to_datetime(ev.ts,unit='ms').dt.strftime('%Y-%m-%d')
ev=ev[ev.day<='2026-09-29']
print('U events',len(ev),'wallets',ev.addr.nunique(),'coins',ev.coin.nunique(), ev.day.min(), ev.day.max(), 'DISC',(ev.day<='2026-09-21').sum())
# whale pnl from raw API fills recorded by the lens (recompute from px, sz, closedPnl, side)
p=pd.read_json(SP+'work/whale-state/pnl_api.jsonl',lines=True)
p=p[p.px.notna()&(p.sz>0)].copy()
p['entry2']=p.px-p.closedPnl/(p.sz*p.side); p['wpnl2']=100*p.side*(p.px/p.entry2-1)
print('api pnl recompute max abs diff', float((p.wpnl2-p.pnl_pct).abs().max()))
# fill time must be at/before decision: use only fills with |t_fill - ts| <= 3 s
p=p[(p.t_fill-p.ts).abs()<=3000]
ev=ev.merge(p[['addr','coin','ts','wpnl2','t_fill']],on=['addr','coin','ts'],how='left')
print('wpnl coverage',ev.wpnl2.notna().mean().round(3))
out=[]
for lab,side_mult,hours in (('fad24',-1,24),('fol8',1,8),('fad8',-1,8),('fol24',1,24)):
    O=outcomes(ev, side_mult*ev.wdir.values, hours)
    ev[lab+'_ew']=O.ew; ev[lab+'_raw']=O.raw; ev[lab+'_btc']=O.btc; ev[lab+'_fund']=O.fund
ev.to_parquet('u_events.parquet'); print('saved')
