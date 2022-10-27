import pyspark
import sys
import json
import random
from itertools import combinations
import time
import re
from operator import add
import math


start = time.time()

def compute_cos(x):
    b_f = []
    u_f = []
    try:
        b_f = business_r[x[0]]
    except KeyError:
        b_f = []
    try:
        u_f = user_r[x[1]]
    except KeyError:
        u_f = []

    dot_pro = len(set(b_f).intersection(u_f))
    e_d = math.sqrt(len(b_f) * len(u_f))
    if dot_pro == 0:
        final = 0
    else:
        final = dot_pro / e_d
    if final >= 0.01:
        return {"user_id":x[1],"business_id":x[0],"sim":final}
    else:
        return {}






sc = pyspark.SparkContext("local[*]","task21")
#file = sys.argv[1]
f = open(sys.argv[2])
data = json.load(f)
f.close()
print(1)
business_r = data['business']


user_r = data['user_feature']


file2 = sys.argv[1]
rdd2 = sc.textFile(file2)
test = rdd2.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],x['user_id']))
print(2)
cos_sim = test.map(lambda x:compute_cos(x))#.filter(lambda x:x['sim'] >= 0.01)
output = cos_sim.collect()
#jobj = json.dumps(cos_sim.collect())
with open(sys.argv[3],'w') as f_j:
    for i in output:
        if i!= {}:
            f_j.writelines(json.dumps(i))
            f_j.writelines('\n')

print("Duration: "+str(time.time()-start))