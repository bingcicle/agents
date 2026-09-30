import sys, json; sys.path.insert(0,'.')
from common import *
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
sym=json.load(open(SP+'infra/symmap.json'))
w=pd.read_parquet(SP+'data/whale_txs.parquet',columns=['addr','coin','ts','dir','tx_usd','liq','depth_usd'])
w['sym']=w.coin.map(sym); w=w.dropna(subset=['sym'])
w0=w.ts.min(); print('whale data from',pd.Timestamp(w0,unit='ms'), 'coins with bn symbol',w.sym.nunique())
d=load(); d=d[(d.ts>=w0+3600e3)]
wcoins=set(w.sym)
res=[]
for c in ['A_DUMP_X1.5_N3','A_DUMP_X1.5_N1','A_DUMP_X2_N5','A_DUMP_X3_N3','A_DUMP_X1_N3','A_PUMP_X1.5_N3','A_PUMP_X2_N5','A_PUMP_X3_N3']:
    g=d[(d.cell==c)].copy(); N=int(c.split('_N')[1]); dump=('DUMP' in c)
    wd=w[(w.dir==('Close Long' if dump else 'Close Short'))&(w.tx_usd>=5000)]
    flag=np.zeros(len(g),bool); usd=np.zeros(len(g))
    for s,gg in g.groupby('sym'):
        ws=wd[wd.sym==s].sort_values('ts')
        if len(ws)==0: continue
        cs=np.concatenate([[0],np.cumsum(ws.tx_usd.values)])
        a=np.searchsorted(ws.ts.values,gg.ts.values-N*60000); b=np.searchsorted(ws.ts.values,gg.ts.values)
        pos=g.index.get_indexer(gg.index); flag[pos]=b>a; usd[pos]=cs[b]-cs[a]
    g['whale']=flag; g['wusd']=usd; g['wcoin']=g.sym.isin(wcoins)
    for lab,sub in [('whale',g[g.whale]),('no_whale_same_coins',g[~g.whale&g.wcoin]),('no_whale_other_coins',g[~g.wcoin])]:
        if len(sub)<5: res.append(dict(cell=c,grp=lab,n=len(sub))); continue
        lo,hi=cci(sub.net,sub.sym)
        res.append(dict(cell=c,grp=lab,n=len(sub),ncoins=sub.sym.nunique(),gross=sub.gross.mean(),net=sub.net.mean(),med=sub.net.median(),neut=sub.neutral.mean(),ci_coin=(round(lo,3),round(hi,3)),move=sub.move.mean()))
    # within-coin difference (coin FE): mean over coins having both
    gg=g[g.wcoin]; m=gg.groupby(['sym','whale']).net.mean().unstack()
    m=m.dropna()
    if len(m)>2: res.append(dict(cell=c,grp='within_coin_diff(whale-no)',n=len(m),net=(m[True]-m[False]).mean(),med=(m[True]-m[False]).median()))
    # big whale: wusd >= 50k
    big=g[g.wusd>=50000]
    if len(big)>=5: res.append(dict(cell=c,grp='whale_usd>=50k',n=len(big),ncoins=big.sym.nunique(),gross=big.gross.mean(),net=big.net.mean(),med=big.net.median(),ci_coin=cci(big.net,big.sym)))
R=pd.DataFrame(res); pd.set_option('display.width',250)
print(R.to_string(float_format=lambda v:f'{v:+.3f}'))
R.to_csv(W+'whale_addon.csv',index=False)
