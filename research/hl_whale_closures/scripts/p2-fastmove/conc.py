import sys; sys.path.insert(0,'.')
from common import *
d=load()
for c in ['A_DUMP_X1.5_N3','A_DUMP_X2_N1','A_DUMP_X2_N5','A_DUMP_X3_N1','A_DUMP_X3_N3','A_DUMP_X3_N15','A_DUMP_X5_N3','A_DUMP_X5_N5','B_DUMP_VOLHI','B_DUMP_OIDN']:
    g=d[(d.cell==c)&(d.per=='TEST')]
    byday=g.groupby('day').net.agg(['sum','size']).sort_values('sum',ascending=False)
    top=byday.head(3)
    wo=g[~g.day.isin(top.index)]
    noaug=g[g.month!='08']
    print(f"{c:16s} n={len(g)} mean={g.net.mean():+.3f} | top3 days {list(top.index)} sums {top['sum'].round(1).tolist()} n={top['size'].tolist()} share_of_total={top['sum'].sum()/g.net.sum():.2f} | w/o top3 days mean={wo.net.mean():+.3f} ci_coin={cci(wo.net,wo.sym)} | w/o Aug mean={noaug.net.mean():+.3f} ci={cci(noaug.net,noaug.sym)} | Sep only {g[g.month=='09'].net.mean():+.3f} n={ (g.month=='09').sum()}")
