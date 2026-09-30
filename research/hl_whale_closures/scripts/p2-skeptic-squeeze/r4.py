from mybt import *; from st2 import *
import warnings; warnings.filterwarnings('ignore')
for L_,H_ in ((24,24),(12,24),(4,24),(24,48)):
    X={1:.03,4:.05,12:.08,24:.12}[L_]; Y={1:.01,4:.02,12:.04,24:.05}[L_]
    D=add(run(L=L_,X=X,Y=Y,Hh=H_,side=-1,dirn='pump'))
    print('SQZ',L_,H_,[(p,sm(x)['n'],sm(x)['mean'],sm(x)['med'],sm(x)['ciC']) for p,x in D.groupby('per')])
