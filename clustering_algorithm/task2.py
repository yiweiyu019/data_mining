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
#n_cluster = sys.argv[2]   #K
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

def compute(x,y):
    output = []
    for j in y:
        temp = distance_e2(x,j)
        output.append(temp)

    return sorted(output)[0]


data = sc.textFile(path_list[0])
data = data.map(lambda x:x.split(',')).map(lambda x:(int(x[0]),cfloat(x[1:])))
#ind = data.map(lambda x:x[0])
#print(data.first())
initial = data.takeSample(False,10)

data = data.filter(lambda x:x not in initial)

for i in range(0,len(initial)):
    initial[i] = (str(i),initial[i][1])


dis = data.map(lambda x:compute(x,initial))
print(dis.take(10))













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





