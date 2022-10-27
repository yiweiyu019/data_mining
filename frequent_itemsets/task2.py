import pyspark
import sys
from operator import add
from itertools import combinations
import time
st = time.time()

output_candidate = {}
output_frequent = {}
#the first stage, find all candidates combinations that larger than the threshold in the certain partition
#partial frequent
def frequent_v(data,kfrequent,k,t):
    set_one_basket = []
    count_comb = {}
    for i in data:
        one_basket = []
        #for each basket, find all frequent combinations in that certain basket
        for j in kfrequent:
            if k >= 2:
                #determine frequent data appeared in each basket
                if set((j,)) <= set(i[1]):
                    one_basket.append(j)
        #increment possible combinations basket by basket
        #it is important to use the basket to generate the combinations, or the time complexity will be an issue
        for c in combinations(one_basket,k):
            if c in count_comb:
                count_comb[c] += 1
            else:
                count_comb[c] = 1
    #filtering
    #get the local frequent pair
    real_comb_t = []
    for i in count_comb:
        if count_comb[i] >= t:
            real_comb_t.append(i)
    return real_comb_t

#the second stage, to check whether the combination from stage one is the
#true positive in the entire context
def frequent_v3(itemset,candidate):
    count = {}
    real = []
    for i in candidate:
        for j in itemset:
            if set(i) <= set(j[1]):
                yield (i,1)
            else:
                yield (i,0)


sc = pyspark.SparkContext("local[*]","task2.yiwei")
threshold = int(sys.argv[1])
support = int(sys.argv[2])
rdd = sc.textFile(sys.argv[3])

header = rdd.first()

groupdata = rdd.filter(lambda x:x!=header).map(lambda x:x.split(',')).groupByKey().map(lambda x:(x[0],set(x[1]))).filter(lambda x:len(x[1])>threshold)
all_data = groupdata.flatMap(lambda x:x[1])
output_candidate[1] = all_data.distinct().map(lambda x:(x,'this is just for output')).collect()
freq_single = all_data.map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:x[1]>=support).map(lambda x:x[0])

k_single = freq_single.collect()
k_mid = freq_single.map(lambda x:(x,'this is just for output')).collect()
output_frequent[1] = k_mid
partition = groupdata.getNumPartitions()
k = len(freq_single.collect())
for i in range(2, k+1):
    freq = groupdata.mapPartitions(lambda x: frequent_v(list(x),k_single,i, support//partition)).distinct().persist()  # reduce(lambda a,b:a)

    bb = freq.collect()
    output_candidate[i] = freq.map(lambda x:tuple(sorted(x))).collect()

    rf = groupdata.mapPartitions(lambda x: frequent_v3(list(x), bb)).reduceByKey(add).filter(lambda x: x[1] >= support)
    #print('8-5')
    k_single = rf.flatMap(lambda x: x[0]).distinct().collect()
    k_single2 = rf.map(lambda x: x[0]).collect()
    set_single = []

    #print(sorted(k_single2))
    if k_single == []:
        break
    output_frequent[i] = rf.map(lambda x:x[0]).map(lambda x:tuple(sorted(x))).collect()

#print(output_candidate)
f = open(sys.argv[4],'w')
f.write('Candidates:\n')
for i in output_candidate:
    f.writelines(str(sorted(output_candidate[i])).replace(", 'this is just for output'",'').replace('[','').replace(']','')+'\n')
    f.write('\n')
f.write('Frequent Itemsets:\n')
output2 = 0
for i in output_frequent:
    output2 += len(output_frequent[i])
    f.writelines(str(sorted(output_frequent[i])).replace(", 'this is just for output'",'').replace('[','').replace(']','').replace('), ','),')+'\n')
    f.write('\n')
f.close()
print(2)
#####this part is for the comparation in task3(to compare with the result of fp-growth)
f = open('this_is_for_intersection.txt','w')
f.write(str(output2))
f.close()
print("Duration: "+str(time.time()-st))