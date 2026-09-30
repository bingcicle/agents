import sys; sys.path.insert(0,'.')
from common import *
import json
d=load()
M=json.load(open(W+'meta.json')); QV=np.load(W+'M_qv.npy',mmap_mode='r')
sp=int((SPLIT-M['t0'])//60000)
liq=pd.Series(np.nanmedian(np.asarray(QV[:sp]),axis=0)*1440,index=M['syms'])
terc=pd.qcut(liq.rank(method='first'),3,labels=['illiq','mid','liq'])
d['liqt']=d.sym.map(terc)
ex=d[d.day!='08-22']
t=pd.DataFrame({'gTR':d[d.per=='TRAIN'].groupby('cell').gross.mean(),'gTE':d[d.per=='TEST'].groupby('cell').gross.mean(),
  'gTE_wo0822':ex[ex.per=='TEST'].groupby('cell').gross.mean(),'nTR_low':d[d.per=='TRAIN'].groupby('cell').gross.mean()-COST_LO,
  'nTE_low_wo0822':ex[ex.per=='TEST'].groupby('cell').gross.mean()-COST_LO,
  'neuTR':d[d.per=='TRAIN'].groupby('cell').neutral.mean(),'neuTE_wo0822':ex[ex.per=='TEST'].groupby('cell').neutral.mean()})
pd.set_option('display.width',250); pd.set_option('display.max_rows',100)
print(t.round(3).to_string())
print('\ncells with LOW-cost net >0 in TRAIN and TEST(w/o 08-22):',list(t[(t.nTR_low>0)&(t.nTE_low_wo0822>0)].index))
print('\nliquidity tercile gross (TRAIN / TEST):')
for c in ['A_DUMP_X1.5_N3','A_DUMP_X2_N5','A_DUMP_X3_N3','A_PUMP_X2_N5','A_PUMP_X3_N3']:
    print(c, d[d.cell==c].pivot_table(index='liqt',columns='per',values='gross',aggfunc=['mean','size'],observed=True).round(3).to_dict())
print('\ntails net (all period):')
for c in ['A_DUMP_X1.5_N3','A_DUMP_X2_N5','A_DUMP_X3_N3','A_DUMP_X5_N5','A_PUMP_X2_N5','A_PUMP_X5_N5','B_DUMP_TP1','B_DUMP_H240']:
    s=stats(d[d.cell==c]); print(c,{k:(round(v,3) if isinstance(v,float) else v) for k,v in s.items() if k in('n','mean','med','win','q1','q5','mn','mx','sum_wo_top10','maxshare_coin','ci_coin','ci_week')})
print('\n08-22 index path: ')
I=np.load(W+'index_I.npy'); t0=M['t0']
a=int((pd.Timestamp('2026-08-22').value//10**6-t0)//60000)
seg=pd.Series(100*(np.exp(I[a:a+1440]-I[a])-1),index=pd.to_datetime(t0+np.arange(a,a+1440)*60000,unit='ms'))
print(seg.resample('60min').agg(['min','max','last']).round(1).to_string())
