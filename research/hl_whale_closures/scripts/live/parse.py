# Parse live recording into compact parquet tables: HL trades (with users), HL book top/levels summary, Binance bookTicker, Binance aggTrades.
import gzip, json, glob, pandas as pd, numpy as np
def lines(f):
    try:
        for l in gzip.open(f,'rt'): yield l
    except (EOFError, OSError): return
D='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/data/live/'
O='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/live/'
tr=[]; bk=[]
for f in sorted(glob.glob(D+'hl_*.jsonl.gz')):
    for line in lines(f):
        try: e=json.loads(line)
        except Exception: continue
        m=e['m']; ch=m.get('channel')
        if ch=='trades':
            for t in m['data']:
                tr.append((e['r'],t['coin'],t['side'],float(t['px']),float(t['sz']),t['time'],t['hash'],t['tid'],t['users'][0],t['users'][1]))
        elif ch=='l2Book':
            d=m['data']; bids,asks=d['levels']
            if not bids or not asks: continue
            bp=np.array([float(x['px']) for x in bids]); bs=np.array([float(x['sz']) for x in bids])
            ap=np.array([float(x['px']) for x in asks]); as_=np.array([float(x['sz']) for x in asks])
            mid=(bp[0]+ap[0])/2
            bk.append((e['r'],d['coin'],d['time'],bp[0],ap[0],bs[0],as_[0],
                       float((bp*bs).sum()),float((ap*as_).sum()),100*(1-bp[-1]/mid),100*(ap[-1]/mid-1),
                       json.dumps([bp.tolist(),bs.tolist(),ap.tolist(),as_.tolist()])))
T=pd.DataFrame(tr,columns=['r','coin','side','px','sz','time','hash','tid','buyer','seller']).drop_duplicates('tid')
B=pd.DataFrame(bk,columns=['r','coin','time','bid','ask','bsz','asz','bid_usd20','ask_usd20','bid_span_pct','ask_span_pct','levels'])
T.to_parquet(O+'hl_trades.parquet'); B.to_parquet(O+'hl_book.parquet')
print('hl trades',len(T),'book snaps',len(B), T.coin.nunique())
bt=[]; at=[]
for f in sorted(glob.glob(D+'bn_*.jsonl.gz')):
    for line in lines(f):
        try: e=json.loads(line)
        except Exception: continue
        d=e['m'].get('data',{})
        if d.get('e')=='bookTicker': bt.append((e['r'],d['s'],d['T'],float(d['b']),float(d['a']),float(d['B']),float(d['A'])))
        elif d.get('e')=='aggTrade': at.append((e['r'],d['s'],d['T'],float(d['p']),float(d['q']),d['m']))
BT=pd.DataFrame(bt,columns=['r','s','T','bid','ask','bq','aq']); AT=pd.DataFrame(at,columns=['r','s','T','p','q','m'])
BT.to_parquet(O+'bn_book.parquet'); AT.to_parquet(O+'bn_trades.parquet')
print('bn book',len(BT),'bn trades',len(AT))
P=[]
for f in sorted(glob.glob(D+'pos_*.jsonl.gz')):
    for line in lines(f):
        e=json.loads(line); m=e['m']
        for p in m['pos']:
            P.append((e['r'],m['user'],p['coin'],float(p['szi']),float(p['positionValue']),float(p.get('entryPx') or 'nan'),
                      float(p['liquidationPx']) if p.get('liquidationPx') else np.nan,float(p['unrealizedPnl'])))
pd.DataFrame(P,columns=['r','user','coin','szi','pos_usd','entry','liq_px','upnl']).to_parquet(O+'positions.parquet')
print('positions',len(P))
