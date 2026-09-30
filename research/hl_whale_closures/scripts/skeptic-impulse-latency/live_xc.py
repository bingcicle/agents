"""Lead-lag between Binance bookTicker (exchange E) and HL trades (HL time) on 30.09 live recording.
For each Binance mid jump >= 8 bp within <=200 ms, find the first HL trade in the same coin in the jump direction whose
price moved >= 4 bp vs the HL trade before the jump window; report lag = HL time - Binance jump E."""
import numpy as np, pandas as pd, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
sm=json.load(open(SP+'infra/symmap.json'))
H=pd.read_parquet('live_hl_trades.parquet'); B=pd.read_parquet('live_bn.parquet')
inv={v:k for k,v in sm.items() if v}
B['coin']=B.s.map(inv); B=B[B.coin.notna()]
B['mid']=(B.b+B.a)/2
out=[]
for coin,b in B.groupby('coin'):
    h=H[H.coin==coin].sort_values('t')
    if len(h)<200: continue
    b=b.sort_values('E'); E=b.E.values; lm=np.log(b.mid.values)
    ht=h.t.values; hp=np.log(h.px.values); hs=h.side.values
    # jumps: mid change over 200 ms window >= 8bp; take first E where |lm - lm(E-200)|>=8bp, then skip 5 s
    j0=np.searchsorted(E,E-200,'left')
    d=(lm-lm[j0])*1e4
    last=-1e18
    for i in np.where(np.abs(d)>=8)[0]:
        if E[i]<last+5000: continue
        last=E[i]; sgn=np.sign(d[i]); tj=E[i]
        k0=np.searchsorted(ht,tj-3000,'left'); k1=np.searchsorted(ht,tj+3000,'right')
        if k1<=k0: continue
        # reference HL px: last HL trade before tj-300
        kr=np.searchsorted(ht,tj-300,'left')-1
        if kr<0: continue
        ref=hp[kr]
        seg=sgn*(hp[kr+1:k1]-ref)*1e4
        hit=np.where(seg>=0.5*abs(d[i]))[0]
        lag=ht[kr+1+hit[0]]-tj if len(hit) else np.nan
        out.append(dict(coin=coin,E=tj,jump=d[i],lag=lag))
O=pd.DataFrame(out); print(len(O), O.coin.nunique()); print(O.lag.describe()); print(O.lag.quantile([.05,.1,.25,.5,.75,.9]).round(0).to_dict())
print('share HL move before BN jump E (lag<0):', (O.lag<0).mean().round(2), ' within [-300,0]:', ((O.lag>=-300)&(O.lag<0)).mean().round(2))
# and HL big aggressive prints -> Binance response relative to HL time
