import sys; sys.path.insert(0,'lib'); from hl import *
V=pd.read_csv('work/bigrev/daily_qv.csv',index_col=0).iloc[:,0]
BIG=set(V[V>=150e6].index)
tx=pd.read_parquet(DATA+'whale_txs.parquet')
tx['symbol']=tx.coin.map(SYM); tx=tx[tx.symbol.isin(BIG)&(tx.liq==0)&(tx.tx_usd>=5000)].copy()
tx['wdir']=np.where(tx.dir.isin(['Close Short','Short > Long','Open Long']),1,-1)
tx=tx[tx.ts<pd.Timestamp('2026-09-30').value//10**6].sort_values('ts')
print('whale close txs >=5k in big coins:',len(tx), tx.coin.value_counts().to_dict())
rows=[]
for r in tx.itertuples():
    s=int(r.ts//1000); b=bars(r.symbol,(s-400)*1000,(s+7300)*1000)
    if b is None: continue
    dec=s+2   # decision ~1.5-2 s after the fill (WS)
    p_dec=last_px(b,dec)
    for W in (60,180):
        p0=b.cf.loc[s-W:s-1].dropna()
        if len(p0)==0: continue
        # max move in whale direction from any point in the window before tx to decision
        ref=p0.min() if r.wdir>0 else p0.max()
        mv=100*r.wdir*(p_dec/ref-1)
        rows.append(dict(ts=r.ts,coin=r.coin,addr=r.addr,wdir=r.wdir,usd=r.tx_usd,W=W,move=mv,sec=dec,symbol=r.symbol))
E=pd.DataFrame(rows)
E=E[E.move>=2].copy()
print('events move>=2%:', E.groupby('W').size().to_dict())
out=[]
for W,g in E.groupby('W'):
    g=g.sort_values('ts'); last={}
    for r in g.itertuples():
        if r.ts-last.get(r.coin,-1e18)<3600*1000: continue     # one trade per coin per hour
        last[r.coin]=r.ts
        b=bars(r.symbol,(r.sec-10)*1000,(r.sec+7300)*1000); side=-r.wdir
        for lag in (0,5,30):
            en=worst_px(b,r.sec+lag,side)
            for H in (5,15,30,60,120):
                ex=worst_px(b,r.sec+lag+H*60,-side)
                g_=100*side*(ex/en-1)
                bt=ret_min('BTCUSDT',(r.sec+lag)*1000,(r.sec+lag+H*60)*1000)
                out.append(dict(W=W,coin=r.coin,day=day_of(r.ts),addr=r.addr,move=r.move,lag=lag,H=H,gross=g_,btc=side*bt,side='LONG' if side>0 else 'SHORT'))
O=pd.DataFrame(out); O.to_parquet('work/bigrev/whale_trades.parquet')
for (W,lag,H),g in O.groupby(['W','lag','H']):
    if lag==0 or H in (15,60):
        net=g.gross-0.10
        print(f'W={W}s lag={lag}s H={H}m n={len(g)} gross mean={g.gross.mean():+.3f} med={g.gross.median():+.3f} net(0.10) mean={net.mean():+.3f} win={100*(net>0).mean():.0f}% | minus BTC {(g.gross-g.btc).mean():+.3f}')
g=O[(O.W==180)&(O.lag==0)&(O.H==60)]
print(g[['coin','day','side','move','gross','btc']].round(2).to_string())
