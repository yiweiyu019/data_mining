import pyspark
import sys
import json
import random
from itertools import combinations
import time

start = time.time()
hash_size = 30
bond = 30
def convert(b_info,u_list):
    result = []
    for i in range(0,len(u_list)):
        if u_list[i] in b_info:
            result.append(i)

    return result



def hash2(x,m,a,b):
    hash_list = []
    for i in range(hash_size):
        min = 9999999999
        #a = random.randint(0,m)
        #b = random.randint(0,m)
        for j in x:
            value = ((a[i] * j + b[i])) % m
            if value < min:
                min = value
        hash_list.append(min)
    return hash_list


def split_bond(x):
    x_l = len(x)
    r = x_l //bond
    result = []
    for i in range(0,x_l,r):
        result.append(x[i:i+r])
    return result


# def lsh(x,n):
#     band_output = {}
#     value = hash(str(x[1][n]))
#     if value in band_output:
#         band_output[value].append(x[0])
#     else:
#         band_output[value] = [x[0]]
#     return band_output

def Jaccard(x,y):
    a = set(x[1])
    b = set(y[1])
    inter = a.intersection(b)
    union = a.union(b)
    out = sorted((x[0],y[0]))
    return ((out[0],out[1]),float(len(inter))/float(len(union)))


sc = pyspark.SparkContext("local[*]","task11")
file = sys.argv[1]
rdd = sc.textFile(file)

file_j = rdd.map(lambda x:json.loads(x))
b_u = file_j.map(lambda x:(x['business_id'],x['user_id']))
all_user = b_u.map(lambda x:x[1]).distinct().collect()

m = b_u.map(lambda x:x[1]).distinct().count()
group_b = b_u.groupByKey().mapValues(list)
#print(group_b.first())
#print(40)

feature_m = group_b.map(lambda x:(x[0],convert(x[1],all_user))).persist()
#print(feature_m.first())
##print(feature_m.collect())
a_list = []
b_list = []
for i in range(hash_size):
    a = random.randint(0,m)
    b = random.randint(0,m)
    a_list.append(a)
    b_list.append(b)
compress_m = feature_m.map(lambda x:(x[0],x[1],hash2(x[1],m,a_list,b_list))).map(lambda x:(x[0],x[1],split_bond(x[2]))).persist()

final = []

candidate_list = []


def expand_c(x):
    result = []
    j_output = []
    if len(x) > 2:
        for j in combinations(x,2):
            j_s = Jaccard(j[0],j[1])
            result.append(j_s)
    else:
        x = sorted(x)
        j_s = Jaccard(x[0],x[1])
        result.append(j_s)
    return result

def merge(x):
    result = []
    for i in x:
        result.extend(i)
    return result

def lsh(x):
    cand = []
    for i in range(0,bond):
        cand.append((hash((tuple(x[2][i]),i)),(x[0],x[1])))
    return cand

candidate = compress_m.flatMap(lambda x:lsh(x)).groupByKey().mapValues(list).filter(lambda x:len(x[1])>1).map(lambda x:x[1])
frequent = candidate.flatMap(lambda x:expand_c(x)).filter(lambda x:x[1] >= 0.05).distinct()
#print(candidate.take(1))
output = frequent.collect()

f = open(sys.argv[2],'w')
for i in output:
    f.writelines(str({"b1":i[0][0],"b2":i[0][1],"sim":i[1]}).replace("'",'"'))
    f.write('\n')

f.close()

print("Duration: "+str(time.time()-start))


