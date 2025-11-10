import random

f=open('primefinder2_snapshot/data/test1000.txt','w')

[f.write(str(random.randint(2,10**7))+'\n') for _ in range(1000)]