import pyspark
import sys
import json
import random
from itertools import combinations
import time
import re
from operator import add
import math
import os

start = time.time()

input_path = sys.argv[1]  #folder cotaining the files of datapoints
n_cluster = int(sys.argv[2])   #K
#out_file1 = sys.argv[3]   #cluster results
#out_file2 = sys.argv[4]   #intermediate results

sc = pyspark.SparkContext("local[*]","task11")
path_list = []
for i in os.listdir(input_path):
    path_list.append(input_path + '/' + i)

def cfloat(x):
    result = []
    for i in x:
        result.append(float(i))
    return tuple(result)

def distance_e(x,y):
    result = 0
    for i in range(0,len(x)):
        result += (x[i] - y[i])**2
    result = math.sqrt(result)
    return result

def distance_e2(x,y):
    result = 0
    for i in range(0,len(x[1])):
        result += (x[1][i] - y[1][i])**2
    result = math.sqrt(result)
    return (result,(x[0],y[0]))

def distance_e3(x,y):
    x1 = info[x]
    result = 0
    for i in range(0,len(x1)):
        result += (x1[i] - y[1][i])**2
    result = math.sqrt(result)
    return (result,(x,y[0]))

def compute(x,y):
    output = []
    for j in y:
        temp = distance_e2(x,j)
        output.append(temp)
    reorder = sorted(output)[0]

    return (reorder[1][1],reorder[1][0])

def compute2(x,y):
    output = []
    for j in y:
        temp = distance_e3(x,j)
        output.append(temp)
    reorder = sorted(output)[0]

    return (str(reorder[1][1]),reorder[1][0])

data = sc.textFile(path_list[0])
data = data.map(lambda x:x.split(',')).map(lambda x:(int(x[0]),cfloat(x[1:])))
info = data.collectAsMap()
DS = []
CS = []
RS = []

#ind = data.map(lambda x:x[0])

#print(data.first())
dimension = len(data.first())
threshold = 2*math.sqrt(dimension)

size = data.count()
#run K-Means on a random subset
sampleall = data.takeSample(False,int(size*0.1))   #first take a sample
  #all data
#initial_center = random.sample(sample,n_cluster)



sample = sc.parallelize(sampleall)
initial_center = sample.takeSample(False,n_cluster)  #pick some k from the sample
sample = sample.filter(lambda x:x not in initial_center)   #filter the k center

for i in range(0,len(initial_center)):
    initial_center[i] = (str(i),initial_center[i][1])

print(2)

dis = sample.map(lambda x:compute(x,initial_center)).groupByKey().mapValues(list)#.filter(lambda x:len(x[1])<=10)

def compt_stat(x):
    sum = {}
    sumq = {}
    n = 0
    for i in x[1]:
        n += 1
        for j in range(0,len(info[i])):
            if j in sum:
                sum[j] += info[i][j]
            else:
                sum[j] = info[i][j]
            if j in sumq:
                sumq[j] += info[i][j] ** 2
            else:
                sumq[j] = info[i][j] ** 2
    center = {}
    std = {}
    for i in sum:
        center[i] = sum[i]/n
        std[i] = math.sqrt(sumq[i]/n - (sum[i]/n)**2)


    return x[0],(center,std,n)


def compute_maha2(x,d):

    dist = 0
    c = d[1][0]
    s = d[1][1]

    for i in range(0,len(x[1])):
        dist += ((x[1][i]-c[i])/s[i])**2
    dist = math.sqrt(dist)
    return (dist,x[0],d[0])

def compute_maha(x,d):
    result = []
    for i in d:
        result.append(compute_maha2(x,i))
    result = sorted(result)[0]
    if result[0] < threshold:
        return (result[2],result[1])
    else:
        return ('-1',result[1])
    #return result



summarization = dis.map(lambda x:compt_stat(x)).collect()

data = data.filter(lambda x:x not in sampleall)
print(789)
ma_dis = data.map(lambda x:compute_maha(x,summarization)).groupByKey().mapValues(list) #remaining -->ds+(points)
ds_s = ma_dis.filter(lambda x:x[0]!='-1').map(lambda x:compt_stat(x))
print('DS-D0')
print(ds_s.collect())
#ds = ma_dis.filter(lambda x:x[0]!='-1').flatMap(lambda x:x[1]).collect()
#print(data.first())
print('what')
#print(ds.first())
#discard_data = data.filter(lambda x:x not in ds)
rs_cs = ma_dis.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1])#.collect()
initial_center2 = rs_cs.takeSample(False,3 * n_cluster)
initial_info = []

for i in range(0,len(initial_center2)):
    initial_info.append((i,info[initial_center2[i]]))

rs_cs = rs_cs.filter(lambda x:x not in initial_center2)

#
# #print(initial_center2[0])
dis2 = rs_cs.map(lambda x:compute2(x,initial_info)).groupByKey().mapValues(list)
csinfo = dis2.filter(lambda x:len(x[1])>1).collectAsMap()

cs = dis2.filter(lambda x:len(x[1])>1).map(lambda x:compt_stat(x))
rs = dis2.filter(lambda x:len(x[1])<=1)    #RS first(data0)
print('RS-D0')
print(rs.collect())
cs2 = cs.collect()
final_cs = []

def merge(x,d):
    dist = 0
    c = d[1][0]
    s = d[1][1]

    for i in range(0,len(x[1][0])):
        dist += ((x[1][0][i]-c[i])/s[i])**2
    dist = math.sqrt(dist)
    if dist < threshold:
        return (x[0],d[0])
    else:
        return 0

counter = []
all_merge = []
all_f = []
for i in combinations(cs2,2):
    all_f.append(i[0][0])
    all_f.append(i[1][0])
    temp = merge(i[0],i[1])
    if temp != 0:
        all_merge.append(temp)

nn = 0
all_f = set(all_f)
for i in all_merge:
    if i[0] in all_f and i[1] in all_f:
        final_cs.append((nn,csinfo[i[0]]+csinfo[i[1]]))
        all_f.remove(i[0])
        all_f.remove(i[1])
        nn += 1
print(all_f)
for i in all_f:
    final_cs.append((nn,csinfo[i]))
final_CS = {}      #CS initial round (first data0)
for i in final_cs:
    result = compt_stat(i)
    final_CS[result[0]] = result[1]
print('CS-D0')
print(final_CS)

########rest files

#DS
#CS
#RS

#1.load all new points, compare points to ds summarization, assign to nearser
# cluster if the maha distance < threshold
#2. those don't have any <threshold, compare them to each of the cs summariztion, assign to
#nearser cs cluster if the maha distance <threshold,
#3. those don't assign to ds or cs, add to RS(last round)
#4. run the clustering algorithm on the total RS with 5*K center, generate CS summerization(with more
#than one data) based on euclidean ....use the remaining points as new RS
#5.merge CS clusters that have a Maha dist <threshold














#lower = data.first()
#upper = data.top(1)


#first_center = random.randint(lower[0],upper[0][0])
#print(first_center)
#initial1 = data.filter(lambda x:x[0] == first_center).collect()
#print(initial1)
#initial2 = data.filter(lambda x:x[0] == first_center).first()

#print(initial2)
# data = data.filter(lambda x:x!=initial1)
#
# print(4)
# comp = data.map(lambda x:distance_e2(x,initial1)).sortByKey(False)
# print(comp.take(2))





