import sys; sys.path.insert(0,'lib'); from hl import *
O=SP+'work/live/'
T=pd.read_parquet(O+'hl_trades.parquet'); BT=pd.read_parquet(O+'bn_book.parquet')
tx=pd.read_parquet(DATA+'whale_txs.parquet'); whales=set(tx.addr)
print('HL WS delivery lag ms (r-time):', T.eval('r-time').describe(percentiles=[.1,.5,.9]).round(0).to_dict())
res=[]
for coin,g in T.groupby('coin'):
    s=SYM.get(coin); b=BT[BT.s==s].sort_values('T')
    if len(b)<100: continue
    bm=(b.bid.values+b.ask.values)/2; bT=b['T'].values
    g=g.sort_values('time')
    i=np.searchsorted(bT, g.time.values, side='right')-1; ok=i>=0
    g=g[ok].copy(); g['bmid']=bm[i[ok]]
    g['basis']=(g.px/g.bmid-1)*100
    # rolling basis from past 10 min median (strict past)
    g['bas_ref']=g.basis.rolling(400,min_periods=50).median().shift(1)
    g['fair']=g.bmid*(1+g.bas_ref/100)
    g['dev']=(g.px/g.fair-1)*100
    # fair 60 s later
    j=np.searchsorted(bT, g.time.values+60000, side='right')-1
    g['bmid60']=bm[np.clip(j,0,len(bm)-1)]
    g['fair60']=g.bmid60*(1+g.bas_ref/100)
    g['taker']=np.where(g.side=='A',g.seller,g.buyer)
    g['taker_whale']=g.taker.isin(whales)
    g['usd']=g.px*g.sz
    res.append(g)
R=pd.concat(res)
print('basis per coin median (HL vs Binance mid %):'); print(R.groupby('coin').basis.median().round(3).describe())
for X in (0.5,1.0,1.5,2.0):
    # sell-aggressor prints at <= fair*(1-X) fill a resting bid at that level; buy-aggressor >= fair*(1+X) fill an ask
    lo=R[(R.side=='A')&(R.dev<=-X)]; hi=R[(R.side=='B')&(R.dev>=X)]
    for nm,d,sg in (('bid',lo,1),('ask',hi,-1)):
        if len(d)==0: print(X,nm,'n=0'); continue
        lvl=d.fair*(1-sg*X/100)
        pnl=sg*(d.fair60/lvl-1)*100-0.03
        ev=d.groupby(['coin',(d.time//60000)]).agg(n=('px','size'),usd=('usd','sum'),wh=('taker_whale','max'),pnl=('px','size'))
        print(f'X={X} {nm}: prints={len(d)} usd={d.usd.sum():.0f} coin-minutes={len(ev)} whale-taker share(prints)={d.taker_whale.mean():.2f} '
              f'pnl60 at level mean={pnl.mean():+.3f} med={pnl.median():+.3f} coins={d.coin.nunique()}')
R.to_parquet(O+'hl_trades_fair.parquet')
