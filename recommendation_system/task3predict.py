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

model = sys.argv[5]

if model == 'item_based':

    sc = pyspark.SparkContext("local[*]","task21")
    def fil(x):
        return sorted(x,reverse= True)

    def func4(x):
        ll = []
        if x[1][0][1] is None:
            return {"user_id":x[0],"business_id":x[1][0][0],"stars":3.823989}   #if business_id not exist in the train_review, output the total average score
        if x[1][0][2] is None:
            num = 0
            for i in x[1][0][1]:
                num += i[1]
            result = num / len(x[1][0][1])
            return {"user_id": x[0], "business_id": x[1][0][0], "stars": result}
        k = 0
        for i in x[1][0][2]:   #i == (weight, business_id)
            for j in x[1][1]:     #x[1][0] is a list of business id comment by the target user_id [(business_id,star),(business_id,star)...]
                if j[0] == i[1]:
                    ll.append((j[0], j[1], i[0]))#(business_id,score,weight)
                    k += 1
            if k == 3:
                break

        if len(ll)>0:
            part1 = 0
            part2 = 0
            for j in ll:
                part1 += j[1] * j[2]
                part2 += abs(j[2])
            return {"user_id": x[0], "business_id": x[1][0][0], "stars": part1/part2}
        else:
            num = 0
            for i in x[1][0][1]:
                num += i[1]
            result = num / len(x[1][0][1])
            return {"user_id": x[0], "business_id": x[1][0][0], "stars": result}

    #group by user_id
    start = time.time()
    file3 = sys.argv[1]  #train_review
    file2 = sys.argv[2]  #test_review
    file = sys.argv[3]   #model, output from train
    file4= sys.argv[4]
    rdd3 = sc.textFile(file3)
    origin = rdd3.map(lambda x:json.loads(x)).map(lambda x:(x['user_id'],(x['business_id'],x['stars']))).groupByKey()
    origin2 = rdd3.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],(x['user_id'],x['stars']))).groupByKey()
    rdd = sc.textFile(file)
    data1 = rdd.map(lambda x:json.loads(x)).filter(lambda x:x['sim'] > 0).map(lambda x:(x['b1'],(x['sim'],x['b2'])))
    data2 = rdd.map(lambda x:json.loads(x)).filter(lambda x:x['sim'] > 0).map(lambda x:(x['b2'],(x['sim'],x['b1'])))
    data=data1.union(data2).groupByKey().map(lambda x:(x[0],fil(x[1])))
    rdd2 = sc.textFile(file2)
    test1 = rdd2.map(lambda x:json.loads(x)).map(lambda x:(x['business_id'],x['user_id']))
    temp1=test1.leftOuterJoin(origin2).leftOuterJoin(data).map(lambda x:(x[1][0][0],(x[0],x[1][0][1],x[1][1]))).leftOuterJoin(origin)
    temp2=temp1.map(lambda x:func4(x)).collect()
    with open(file4,'w') as f_j:
        for i in temp2:
            if i!= {}:
                f_j.writelines(json.dumps(i))
                f_j.writelines('\n')
elif model == 'user_based':


    sc = pyspark.SparkContext("local[*]", "task21")


    def fil(x):
        return sorted(x, reverse=True)

    def func4(x):
        ll = []
        if x[1][0][1] is None:
            return {"user_id": x[1][0][0], "business_id": x[0],
                    "stars": 3.823989}  # if business_id not exist in the train_review, output the total average score
        if x[1][0][2] is None:
            num = 0
            for i in x[1][0][1]:
                num += i[1]
            result = num / len(x[1][0][1])
            return {"user_id": x[1][0][0], "business_id": x[0], "stars": result}
        k = 0
        if x[1][1] is None:
            num = 0
            for i in x[1][0][1]:
                num += i[1]
            result = num / len(x[1][0][1])
            return {"user_id": x[1][0][0], "business_id": x[0], "stars": result}
        for i in x[1][0][2]:  # i == (weight, business_id)
            for j in x[1][1]:  # x[1][0] is a list of business id comment by the target user_id [(business_id,star),(business_id,star)...]
                if j[0] == i[1]:
                    ll.append((j[0], j[1], i[0]))  # (business_id,score,weight)
                    k += 1
            if k == 5:
                break

        if len(ll) > 0:
            part1 = 0
            part2 = 0
            for j in ll:
                avg_t = avg_dict[j[0]]
                avg = (avg_t[0] - j[1]) / (avg_t[1] - 1)
                part1 += (j[1] - avg) * j[2]
                part2 += abs(j[2])
                try:
                    base_avg = avg_dict[x[1][0][0]][0] / avg_dict[x[1][0][0]][1]
                except:
                    base_avg = 3.823989
            return {"user_id": x[1][0][0], "business_id": x[0], "stars": base_avg + part1 / part2}
        else:
            num = 0
            for i in x[1][0][1]:
                num += i[1]
            result = num / len(x[1][0][1])
            return {"user_id": x[1][0][0], "business_id": x[0], "stars": result}


    # group by user_id
    start = time.time()
    file3 = sys.argv[1]  # train_review
    file2 = sys.argv[2]  # test_review
    file = sys.argv[3]  # model, output from train
    file4 = sys.argv[4]


    def compute_avg(x):
        star = 0
        for i in x[1]:
            star += i[1]
        return {x[0]: (star, len(x[1]))}


    rdd3 = sc.textFile(file3)
    origin = rdd3.map(lambda x: json.loads(x)).map(
        lambda x: (x['business_id'], (x['user_id'], x['stars']))).groupByKey()
    origin2 = rdd3.map(lambda x: json.loads(x)).map(
        lambda x: (x['user_id'], (x['business_id'], x['stars']))).groupByKey()
    avg = origin2.map(compute_avg).collect()
    avg_dict = {}
    for i in avg:
        avg_dict.update(i)

    rdd = sc.textFile(file)
    data1 = rdd.map(lambda x: json.loads(x)).filter(lambda x: x['sim'] > 0).map(
        lambda x: (x['u1'], (x['sim'], x['u2'])))
    data2 = rdd.map(lambda x: json.loads(x)).filter(lambda x: x['sim'] > 0).map(
        lambda x: (x['u2'], (x['sim'], x['u1'])))
    data = data1.union(data2).groupByKey().map(lambda x: (x[0], fil(x[1])))

    rdd2 = sc.textFile(file2)
    test1 = rdd2.map(lambda x: json.loads(x)).map(lambda x: (x['user_id'], x['business_id']))
    temp1 = test1.leftOuterJoin(origin2).leftOuterJoin(data).map(
        lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))).leftOuterJoin(origin)

    temp2 = temp1.map(lambda x: func4(x)).collect()
    with open(file4, 'w') as f_j:
        for i in temp2:
            if i != {}:
                f_j.writelines(json.dumps(i))
                f_j.writelines('\n')

print("Duration: "+str(time.time()-start))