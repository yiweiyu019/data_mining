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

sc = pyspark.SparkContext("local[*]","task11")
file = sys.argv[1]
rdd = sc.textFile(file)


stop_word = []
with open(sys.argv[3],'r') as f:
    for i in f.readlines():
        i = i.strip()
        stop_word.append(i)

def remove_c(x):
    result = []
    for i in x:
        i = re.sub('[^a-zA-Z]','',i)
        if i.lower() not in stop_word and i != '':
            result.append(i)
    return result

def count(x):
    result = {}
    for i in x:
        if i in result:
            result[i] += 1
        else:
            result[i] = 1
    result2 = sorted(result.items(),key = lambda x: x[1], reverse = True)
    final = []
    for i in result2:
        tf = i[1]/result2[0][1]
        #if tf >= 0.000001:
        final.append((i[0],tf))
    return final

def comb(x):
    result = []
    for i in x[1]:
        result.append((i[0],(x[0],i[1])))
    return result

def compute(x):
    result = []
    for i in x:
        for j in idf:
            if i[0] == j[0]:
                result.append((i[0],i[1] * j[1]))

    return sorted(result,key = lambda x:x[1],reverse = True)[0:100]

def compute2(x):
    result = []
    for i in x[1]:
        tf_idf = i[1] * math.log(total/x[2],2)
        result.append((i[0],(x[0],tf_idf)))
    return result

def comput3(x,y):
    if type(x) == list:
        x = x
    else:
        x = [x]
    if type(y) == list:
        y = y
    else:
        y = [y]
    return x+y

def only_word(x):
    result = []
    for i in x:
        result.append(i[0])
    return result



file_j = rdd.map(lambda x:json.loads(x))
info = file_j.map(lambda x:(x['business_id'],(x['text'],x['user_id'])))
info = info.reduceByKey(lambda x,y:((x[0]+y[0]),comput3(x[1],y[1])))
user_review = file_j.map(lambda x:(x['business_id'],x['user_id'])).reduceByKey(lambda x,y:comput3(x,y))


total = info.count()

business_review = info.map(lambda x:(x[0],re.split('\W|\_|\d',x[1][0]),x[1][1])).map(lambda x:(x[0],remove_c(x[1]),x[2]))


tf = business_review.map(lambda x:(x[0],count(x[1]))).map(lambda x:comb(x))



all_c = tf.flatMap(lambda x:x).map(lambda x:x[0]).count()
#idf = business_review.map(lambda x:set(x[1])).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(add).map(lambda x:(x[0],math.log(total/x[1],2))).collect()
idf = tf.flatMap(lambda x:x).reduceByKey(lambda x,y:comput3(x,y)).map(lambda x:(x[0],x[1],len(x[1]))).filter(lambda x:x[2] >= all_c * 0.000001)

tf_idf = idf.map(lambda x:compute2(x)).flatMap(lambda x:x).reduceByKey(lambda x,y:comput3(x,y))
feature = tf_idf.map(lambda x:(x[0],sorted(x[1],reverse=True)[:200])).map(lambda x:(x[0],only_word(x[1])))

def user_word(x):
    result = []
    for i in x[1][1]:
        result.append((i,x[1][0]))
    return result



user_feature = feature.join(user_review).flatMap(lambda x:user_word(x)).reduceByKey(lambda x,y:comput3(x,y)).map(lambda x:{x[0]:tuple(set(x[1]))})

print(user_review.first())
print(feature.first())


resultd = feature.map(lambda x:{x[0]:x[1]}).collect()
resultu = user_feature.collect()
result_all = {'business':resultd,'user_feature':resultu}
#resultu = {'user':resultu}
business_r = {}
for i in result_all['business']:
    business_r.update(i)
user_r = {}
for i in result_all['user_feature']:
    user_r.update(i)
result_all2 = {'business':business_r,'user_feature':user_r}
jsobj = json.dumps(result_all2)
#jsobj2 = json.dumps(resultu)
with open(sys.argv[2],'w') as f_j:
    f_j.write(jsobj)


print("Duration: "+str(time.time()-start))