from mybt import *; from st2 import *
import warnings; warnings.filterwarnings('ignore')
out=[]
def rep(tag,D):
    D=add(D)
    for p in ('TRAIN','TEST'):
        x=D[D.per==p]; s=sm(x); s.update(tag=tag,per=p,btc=sm(x,'y_btc').get('mean'),md=sm(x,'y_md').get('mean')); out.append(s)
    print(tag,[ (o['per'],o.get('n'),o.get('mean'),o.get('ciC'),o.get('ciD'),o.get('wo10'),o.get('btc'),o.get('md')) for o in out[-2:]],flush=True)
for X in (.08,.10,.12,.15,.20):
    for Y in (.03,.05,.08,.12):
        rep(f'X{X}Y{Y}',run(X=X,Y=Y))
for Lh in (4,12,48):
    rep(f'L{Lh}',run(L=Lh))
for Hh in (4,12,48,72):
    rep(f'H{Hh}',run(Hh=Hh))
for dl in (10,60):
    rep(f'delay{dl}',run(delay=dl))
rep('oilag6',run(oilag=6))
rep('noOI',run(Y=-10))  # doi<=10 always -> pure dump follow
rep('OIup',run(Y=-10)) 
pd.DataFrame(out).to_csv(W+'liqdump_robust.csv',index=False)
