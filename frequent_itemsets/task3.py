import pyspark
from pyspark.mllib.fpm import FPGrowth
import sys
import time
st = time.time()

sc = pyspark.SparkContext("local[*]","task3.yiwei")
threshold = int(sys.argv[1])
support = int(sys.argv[2])
rdd = sc.textFile(sys.argv[3])

header = rdd.first()
#print(33)
groupdata = rdd.filter(lambda x:x!=header).map(lambda x:x.split(',')).groupByKey().map(lambda x:set(x[1])).filter(lambda x:len(x)>threshold).cache()
#groupdata2 = rdd.filter(lambda x:x!=header).map(lambda x:x.split(',')).groupByKey().mapValues(list).map(lambda x:x[1])
#print(groupdata.take(2))
num = groupdata.count()
num2 = groupdata.count()
fpGrowth = FPGrowth.train(groupdata,minSupport = support/num,numPartitions = 12)
#fpGrowth = FPGrowth.train(groupdata2,minSupport = support/num2,numPartitions = 2)
result = fpGrowth.freqItemsets().collect()
output = {}
for i,j in result:
    if len(i) in output:
        output[len(i)].append(i)
    else:
        output[len(i)] = [i]
fp_count = 0
for i in output:
    fp_count += len(output[i])

f = open('this_is_for_intersection.txt','r')
apriori_count = f.readlines()
apriori_count = int(apriori_count[0])
f.close()

f = open(sys.argv[4],'w')
f.write("Task2," + str(apriori_count)+'\n')
f.write("Task3," + str(fp_count)+'\n')
f.write("Intersection," + str(apriori_count-fp_count)+'\n')
f.close()

print("Duration: "+str(time.time()-st))