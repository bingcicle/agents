import sys; sys.path.insert(0,'.')
from common import *
import json
meta=json.load(open(W+'meta.json')); I=np.load(W+'index_I.npy'); btc=np.load(W+'btc_c.npy')
t=pd.to_datetime(meta['t0']+np.arange(meta['T'])*60000,unit='ms')
s=pd.Series(I,index=t); b=pd.Series(np.log(btc),index=t).ffill()
m=s.resample('MS').last(); mb=b.resample('MS').last()
print('alt index monthly % :',(100*(np.exp(m.diff().fillna(m.iloc[0]-s.iloc[0]))-1)).round(1).to_dict())
print('BTC monthly % :',(100*(np.exp(mb.diff().fillna(mb.iloc[0]-b.iloc[0]))-1)).round(1).to_dict())
wk=s.resample('W-SUN').last().diff(); 
d=load()
d['wkret']=d.ts.map(lambda x: np.nan)  # placeholder
wki=pd.to_datetime(d.ts,unit='ms').dt.to_period('W-SUN').dt.end_time.dt.normalize()
wkv=100*wk; wkv.index=wkv.index.normalize()
d['wk_up']=wki.map(wkv)>0
d['tr7_up']=d.tr7>0
key=['A_DUMP_X1.5_N3','A_DUMP_X2_N5','A_DUMP_X3_N3','A_DUMP_X5_N5','A_PUMP_X1.5_N3','A_PUMP_X2_N5','A_PUMP_X3_N3','A_PUMP_X5_N5','B_DUMP_VOLHI','B_DUMP_OIDN']
for c in key:
    g=d[d.cell==c]
    r=g.groupby(['per','tr7_up']).agg(n=('gross','size'),gross=('gross','mean'),neut=('neutral','mean')).round(3)
    r2=g.groupby(['per','wk_up']).agg(n=('gross','size'),gross=('gross','mean'),neut=('neutral','mean')).round(3)
    print('\n==',c,'| trailing-7d index up? (known ex ante)'); print(r.to_string()); print('  calendar week index up? (ex post)'); print(r2.to_string())
d[['cell','sym','ts','tr7_up','wk_up']].to_parquet(W+'regime_flags.parquet')
