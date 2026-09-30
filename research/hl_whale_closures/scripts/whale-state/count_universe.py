# Count candidate event universes (NO outcomes looked at): rolling-60s cumulative close crossing thresholds.
import sys, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
sys.path.insert(0,SP+'lib'); from hl import SYM
tx=pd.read_parquet(SP+'data/whale_txs.parquet')
tx['wdir']=np.where(tx.dir.isin(['Close Short','Short > Long','Open Long']),1,-1)
tx=tx[(tx.liq==0)&tx.coin.map(SYM).notna()].sort_values(['addr','coin','wdir','ts']).reset_index(drop=True)
out=[]
for k,a in tx.groupby(['addr','coin','wdir'],sort=False):
    ts=a.ts.values; usd=a.tx_usd.values; sz=a.sz.values; sp=np.abs(a.sp.values)
    cu=np.cumsum(usd); cs=np.cumsum(sz)
    j0=np.searchsorted(ts, ts-60000,'left')
    r_usd=cu-np.concatenate([[0],cu])[j0]
    r_sz=cs-np.concatenate([[0],cs])[j0]
    r_pct=100*r_sz/np.where(sp[j0]>0,sp[j0],np.nan)
    out.append(pd.DataFrame({'i':a.index.values,'r_usd':r_usd,'r_pct':r_pct}))
R=pd.concat(out).set_index('i'); tx=tx.join(R)
def ep(m,gapm=30):
    g=tx[m].sort_values(['addr','coin','wdir','ts'])
    gap=g.groupby(['addr','coin','wdir']).ts.diff()
    e=g[gap.isna()|(gap>gapm*60000)]
    return e
for name,m in [('tx gate r>=2',(tx.tx_usd>=5000)&(tx.tx_pct>=5)&(tx.batch_ratio>=2)),
               ('tx gate any r',(tx.tx_usd>=5000)&(tx.tx_pct>=5)),
               ('roll60 5%&5k r>=2',(tx.r_usd>=5000)&(tx.r_pct>=5)&(tx.batch_ratio>=2)),
               ('roll60 5%&5k any r',(tx.r_usd>=5000)&(tx.r_pct>=5)),
               ('roll60 2%&10k r>=2',(tx.r_usd>=10000)&(tx.r_pct>=2)&(tx.batch_ratio>=2)),
               ('roll60 20k r>=2',(tx.r_usd>=20000)&(tx.batch_ratio>=2)),
               ('roll60 20k r>=1',(tx.r_usd>=20000)&(tx.batch_ratio>=1))]:
    e=ep(m); e=e.assign(day=pd.to_datetime(e.ts,unit='ms').dt.strftime('%m-%d'))
    print(f'{name:22s} txs={m.sum():6d} episodes={len(e):5d} disc(<=09-21)={(e.day<="09-21").sum():5d} test={(e.day>="09-22").sum():5d} wallets={e.addr.nunique()} coins={e.coin.nunique()} whale-buy share={(e.wdir==1).mean():.2f}')
tx.to_parquet(SP+'work/whale-state/txs_roll.parquet')
