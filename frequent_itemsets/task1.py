import pyspark
import sys
from operator import add
from itertools import combinations
import time
import math
st = time.time()
output_candidate = {}
output_frequent = {}

def get_combination(x,n):
    out = []
    for i in combinations(x,n):
        out.append(i)
    #print("candidate:")
    #output_candidate[n] = out
    #print(out)
    return out
count = {}

def frequent_v(itemset,candidate,t,k):
    count = {}
    real_f = []
    if k == 1:
        for i in itemset:
            for j in i[1]:
                if j in count:
                    count[j] += 1
                else:
                    count[j] = 1
    else:
        for i in candidate:
            for j in itemset:
                if set(i) <= set(j[1]):
                    if i in count:
                        count[i] += 1
                    else:
                        count[i] = 1

    for i in count:
        if count[i] >= t:
            real_f.append(i)

    return real_f

def frequent_v3(itemset,candidate,k):
    for i in candidate:
        for j in itemset:
            if k == 1:
                if set((i,)) <= set(j[1]):
                    yield (i,1)
                else:
                    yield (i,0)
            else:
                if set(i) <= set(j[1]):
                    yield (i,1)
                else:
                    yield (i,0)

#
#
#
# def frequent_v2(itemset,candidate):
#     count = {}
#     real = []
#     for i in candidate:
#         for j in itemset:
#             if set(i) <= set(j[1]):
#                 if i in count:
#                     count[i] += 1
#                 else:
#                     count[i] = 1
#     for i in count:
#         real.append((i,count[i]))
#     return real

sc = pyspark.SparkContext("local[*]","task1")
support = int(sys.argv[2])
rdd = sc.textFile(sys.argv[3])
case = int(sys.argv[1])
header = rdd.first()
#print(111)

if case == 1:
    groupdata = rdd.filter(lambda x:x!=header).map(lambda x:x.split(',')).groupByKey().mapValues(list)
    all_data = rdd.filter(lambda x:x!=header).map(lambda x:x.split(',')[1])
    #print(all_data.distinct().collect())
    freq_single = all_data.map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:x[1]>=support).map(lambda x:x[0])

    k = len(freq_single.collect())
    partitions = groupdata.getNumPartitions()
    data = freq_single.collect()
    #print("frequent")
    #print(data)
    #output_frequent[1] = freq_single.map(lambda x:(x,'this is just for output')).collect()


    #bb = 0
    #print(freq.collect())
    for i in range(1,k+1):
        if k>=2:
            candidate = get_combination(data,i)    #find all candidates
        else:
            candidate = []

        freq = groupdata.mapPartitions(lambda x:frequent_v(list(x),candidate,support/partitions,i)).distinct()#reduce(lambda a,b:a)
        #broad_c = sc.broadcast(freq.collect())
        bb = freq.collect()
        if i == 1:
            output_candidate[i] = freq.map(lambda x:(x,'this is just for output')).collect()
        else:
            output_candidate[i] = freq.map(lambda x:tuple(sorted(x))).collect()
        rf = groupdata.mapPartitions(lambda x:frequent_v3(list(x),bb,i)).reduceByKey(add).filter(lambda x:x[1]>=support)
        #print("test")
        #print(freq.collect())
        data = rf.map(lambda x:x[0]).collect()
        if data == []:
            break
        #print("frequent:")
        #print(data)
        if i == 1:
            output_frequent[i] = rf.map(lambda x:x[0]).map(lambda x:(x,'this is just for output')).collect()
        else:
            output_frequent[i] = rf.map(lambda x:x[0]).map(lambda x:tuple(sorted(x))).collect()
        if i != 1:
            flat_data = []
            for i in data:
                for j in i:
                    if j not in flat_data:
                        flat_data.append(j)
            data = flat_data
else:
    groupdata = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x:(x[1],x[0])).groupByKey().mapValues(list)
    all_data = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')[0])
    #print(groupdata.collect())
    #print(all_data.collect())
    freq_single = all_data.map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:x[1]>=support).map(lambda x:x[0])

    k = len(freq_single.collect())
    partitions = groupdata.getNumPartitions()
    data = freq_single.collect()
    #print("frequent")
    #print(data)




    for i in range(1,k+1):
        #candidate = get_combination(data,i)    #find all candidates
        if k>=2:
            candidate = get_combination(data,i)    #find all candidates
        else:
            candidate = []
        freq = groupdata.mapPartitions(lambda x:frequent_v(list(x),candidate,support/partitions,i)).distinct().persist()#reduce(lambda a,b:a)
        #broad_c = sc.broadcast(freq.collect())
        #print(broad_c.value)
        bb = freq.collect()
        if i == 1:
            output_candidate[i] = freq.map(lambda x:(x,'this is just for output')).collect()
        else:
            output_candidate[i] = freq.map(lambda x:tuple(sorted(x))).collect()
        #rf = groupdata.mapPartitions(lambda x:frequent_v2(list(x),broad_c.value)).filter(lambda x:x[1]>=support)
        #rf = groupdata.mapPartitions(lambda x:frequent_v2(list(x),bb)).filter(lambda x:x[1]>= support)
        rf = groupdata.mapPartitions(lambda x: frequent_v3(list(x), bb,i)).reduceByKey(add).filter(lambda x: x[1] >= support)


        data = rf.map(lambda x:x[0]).collect()
        if data == []:
            break
        if i == 1:
            output_frequent[i] = rf.map(lambda x:x[0]).map(lambda x:(x,'this is just for output')).collect()
        else:
            output_frequent[i] = rf.map(lambda x:x[0]).map(lambda x:tuple(sorted(x))).collect()
        if i != 1:
            flat_data = []
            for i in data:
                for j in i:
                    if j not in flat_data:
                        flat_data.append(j)
            data = flat_data
#print(output_candidate)
#print(output_frequent)
f = open(sys.argv[4],'w')
f.write('Candidates:\n')
for i in output_candidate:
    f.writelines(str(sorted(output_candidate[i])).replace(", 'this is just for output'",'').replace('[','').replace(']','')+'\n')
    f.write('\n')
f.write('Frequent Itemsets:\n')
for i in output_frequent:
    f.writelines(str(sorted(output_frequent[i])).replace(", 'this is just for output'",'').replace('[','').replace(']','')+'\n')
    f.write('\n')
f.close()
print("Duration: "+str(time.time()-st))












