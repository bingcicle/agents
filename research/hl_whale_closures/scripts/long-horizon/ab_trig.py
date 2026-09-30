import numpy as np, pandas as pd
from lh import *; from evalx import *
out=W+'out_AB_trig.txt'; open(out,'w').write('Trigger-based episodes from bot follow trades 30.08-29.09 (early=30.08-12.09 legacy HL-era, disc=12.09-21.09, test=22.09-29.09)\n')
A=attach(pd.read_parquet(W+'trig_A.parquet')); A.to_parquet(W+'trig_A_ev.parquet')
compact(A,'A-trig: follow whale dir from first trigger after 6h silence of the pair',out=out)
compact(A[A.ratio>=5],'A-trig ratio>=5',out=out)
B=attach(pd.read_parquet(W+'trig_B.parquet')); B.to_parquet(W+'trig_B_ev.parquet')
compact(B,'B-trig: FADE whale dir after bot-seen full close',out=out)
compact(B[B.ratio>=3],'B-trig ratio>=3',out=out)
