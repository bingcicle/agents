import pandas as pd, json, glob, collections, datetime as dt
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
D=SP+'hl_whale_export_20260930/02_bot_data/'
sym=json.load(open(SP+'infra/symmap.json'))
need=collections.defaultdict(set)   # coin -> set of UTC dates
def add(coin, ts_ms_start, ts_ms_end=None):
    if coin not in sym or sym[coin] is None: return
    a=pd.Timestamp(ts_ms_start,unit='ms'); b=pd.Timestamp(ts_ms_end if ts_ms_end else ts_ms_start,unit='ms')+pd.Timedelta(minutes=65)
    for d in pd.date_range(a.normalize(), b.normalize(), freq='D'): need[coin].add(d.strftime('%Y-%m-%d'))
cnt=0
for f in sorted(glob.glob(D+'events-*.jsonl')):
    for line in open(f):
        e=json.loads(line)
        if e['kind']=='close_batch':
            ts=max(t['ts'] for t in e['txs']) if e['txs'] else e['ts']*1000
            add(e['coin'], ts); cnt+=1
s=pd.read_csv(D+'settlements.csv',low_memory=False)
for c,t in zip(s.coin,s.entry_ts_ms): add(c,t)
tw=pd.read_csv(D+'twap_signals.csv',low_memory=False)
for c,a,b in zip(tw.coin,tw.start,tw.end):
    try:
        ta=pd.Timestamp(a).value//10**6; tb=pd.Timestamp(b).value//10**6
        # cap very long twaps at 2 days
        tb=min(tb, ta+2*86400*1000)
        add(c,ta,tb)
    except Exception: pass
for fn,col in [('sim_trades.csv','date_open'),('fc_trades.csv','date')]:
    x=pd.read_csv(D+fn,low_memory=False)
    for c,t in zip(x.coin,x[col]):
        add(c, pd.Timestamp(t).value//10**6)   # local time string ~ CEST; +-2h ok for day coverage
    # also add previous day to be safe for tz
for fn in glob.glob(D+'follow_trades.csv*')+glob.glob(D+'rev_trades.csv*'):
    x=pd.read_csv(fn,low_memory=False)
    col='date_open' if 'date_open' in x.columns else 'date'
    for c,t in zip(x.coin,x[col]): add(c, pd.Timestamp(t).value//10**6-2*3600*1000)
out=[]
lim_lo,lim_hi='2026-08-25','2026-09-29'
for c,ds in need.items():
    for d in sorted(ds):
        if lim_lo<=d<=lim_hi: out.append((c,sym[c],d))
print('pairs',len(out),'coins',len(need))
json.dump(out,open(SP+'infra/needed_pairs.json','w'))
