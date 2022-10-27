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

N = 3

model = sys.argv[3]



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

#da = [('a',[123]),('b',[234]),('c',[678])]

def combination(x):
    result = []
    for i in range(0,total):

        if da[i] == x:
            while i+1 <= total-1:
                result.append((x,da[i+1]))
                i = i+1
    return result

def inter(x):
    a = []
    b = []
    if isinstance(x[0][1], list):
        for i in x[0][1]:
            a.append(i[0])
    if isinstance(x[1][1],list):
        for j in x[1][1]:
            b.append(j[0])
    if len(set(a).intersection(b)) >= 3:
        return True
    else:
        return False

def inter3(x):
    a = []
    b = []
    for i in x[0][1]:
        a.append(i[0])
    for j in x[1][1]:
        b.append(j[0])
    both = set(a).intersection(b)
    both_l = len(both)
    output1 = {}
    output2 = {}
    inter_sum1 = 0
    inter_sum2 = 0
    for i in x[0][1]:
        #inter_sum1 += i[1]
        if i[0] in both:
            output1[i[0]] = i[1]
            #inter_sum1 += i[1]
    for i in output1:
        inter_sum1 += output1[i]
    for j in x[1][1]:
        #inter_sum2 += j[1]
        if j[0] in both:
            output2[j[0]] = j[1]
            #inter_sum2 += j[1]
    for j in output2:
        inter_sum2 += output2[j]

    avg1 = inter_sum1/both_l
    avg2 = inter_sum2/both_l
    part1 = 0
    part2_1 = 0
    part2_2 = 0
    for i in output1:
        part1 += (output1[i]-avg1) * (output2[i]-avg2)
        part2_1 += (output1[i]-avg1) ** 2
        part2_2 += (output2[i]-avg2) ** 2

    if part2_2 == 0 or part2_1 == 0:
        output = 0
    else:
        output = part1/(math.sqrt(part2_1) * math.sqrt(part2_2))

    return {"b1":x[0][0],"b2":x[1][0],"sim":output}


if model == 'item_based':
    sc = pyspark.SparkContext("local[*]","task11")
    #ex = sc.parallelize([i for i in range(10)]).flatMap(lambda x: [(x)])# for i in range(x+1, 10)])
    #print(ex.collect())
    file = sys.argv[1]
    # rdd = sc.parallelize(da)
    # ans = rdd.flatMap(lambda x:combination(x))
    # print(ans.collect())
    rdd = sc.textFile(file)

    file_j = rdd.map(lambda x:json.loads(x))
    info = file_j.map(lambda x:(x['business_id'],(x['user_id'],x['stars'])))
    group_info = info.reduceByKey(lambda x,y:comput3(x,y))
    da = group_info.collect()
    total = group_info.count()
    #pair_business = group_info.flatMap(lambda x:combination(x)).filter(lambda x:inter(x)).map(lambda x:inter2(x))
    pair_business = group_info.flatMap(lambda x:combination(x)).filter(lambda x:inter(x)).map(lambda x:inter3(x))#.filter(lambda x:x['sim']>=0)


    ddd = pair_business.collect()


    #jsobj2 = json.dumps(resultu)
    with open(sys.argv[2],'w') as f_j:
        for i in ddd:
            f_j.writelines(json.dumps(i))
            f_j.writelines('\n')
elif model == 'user_based':
    hash_size = 29
    bond = 29


    def convert(b_info, u_list):
        result = []
        for i in range(0, len(u_list)):
            if u_list[i] in b_info:
                result.append(i)

        return result


    def hash2(x, m, a, b):
        hash_list = []
        for i in range(hash_size):
            min = 9999999999
            # a = random.randint(0,m)
            # b = random.randint(0,m)
            for j in x:
                value = ((a[i] * j + b[i])) % m
                if value < min:
                    min = value
            hash_list.append(min)
        return hash_list


    def split_bond(x):
        x_l = len(x)
        r = x_l // bond
        result = []
        for i in range(0, x_l, r):
            result.append(x[i:i + r])
        return result


    def Jaccard(x, y):
        a = set(x[1])
        b = set(y[1])
        inter = a.intersection(b)
        union = a.union(b)
        out = sorted(((x[0], x[2]), (y[0], y[2])))
        return ((out[0], out[1]), float(len(inter)) / float(len(union)))


    sc = pyspark.SparkContext("local[*]", "task11")
    file = sys.argv[1]
    rdd = sc.textFile(file)

    file_j = rdd.map(lambda x: json.loads(x))
    user_info = file_j.map(lambda x: (x['user_id'], (x['business_id'], x['stars']))).groupByKey().mapValues(list)
    user_info2 = file_j.map(lambda x: (x['user_id'], x['business_id'])).groupByKey().mapValues(list)
    user_join = user_info.join(user_info2)

    all_business = file_j.map(lambda x: (x['user_id'], (x['business_id'], x['stars']))).map(
        lambda x: x[1][0]).distinct().collect()

    m = user_info.map(lambda x: x[1][0]).distinct().count()

    feature_m = user_join.map(lambda x: (x[0], convert(x[1][1], all_business), x[1][0])).persist()

    a_list = []
    b_list = []
    for i in range(hash_size):
        a = random.randint(0, m)
        b = random.randint(0, m)
        a_list.append(a)
        b_list.append(b)
    compress_m = feature_m.map(lambda x: (x[0], x[1], hash2(x[1], m, a_list, b_list), x[2])).map(
        lambda x: (x[0], x[1], split_bond(x[2]), x[3])).persist()

    def expand_c(x):
        result = []
        j_output = []
        if len(x) > 2:
            for j in combinations(x, 2):
                j_s = Jaccard(j[0], j[1])
                result.append(j_s)
        else:
            x = sorted(x)
            j_s = Jaccard(x[0], x[1])
            result.append(j_s)
        return result


    def merge(x):
        result = []
        for i in x:
            result.extend(i)
        return result


    def lsh(x):
        cand = []
        for i in range(0, bond):
            cand.append((hash((tuple(x[2][i]), i)), (x[0], x[1], x[3])))
        return cand


    candidate = compress_m.flatMap(lambda x: lsh(x)).groupByKey().mapValues(list).filter(lambda x: len(x[1]) > 1).map(
        lambda x: x[1])
    frequent = candidate.flatMap(lambda x: expand_c(x)).filter(lambda x: x[1] >= 0.01).map(lambda x: x[0])

    def inter(x):
        a = []
        b = []
        if isinstance(x[0][1], list):
            for i in x[0][1]:
                a.append(i[0])
        if isinstance(x[1][1], list):
            for j in x[1][1]:
                b.append(j[0])
        if len(set(a).intersection(b)) >= 3:
            return True
        else:
            return False


    def inter3(x):
        a = []
        b = []
        for i in x[0][1]:
            a.append(i[0])
        for j in x[1][1]:
            b.append(j[0])
        both = set(a).intersection(b)
        both_l = len(both)
        output1 = {}
        output2 = {}
        inter_sum1 = 0
        inter_sum2 = 0
        for i in x[0][1]:
            # inter_sum1 += i[1]
            if i[0] in both:
                output1[i[0]] = i[1]
                # inter_sum1 += i[1]
        for i in output1:
            inter_sum1 += output1[i]
        for j in x[1][1]:
            # inter_sum2 += j[1]
            if j[0] in both:
                output2[j[0]] = j[1]
                # inter_sum2 += j[1]
        for j in output2:
            inter_sum2 += output2[j]

        avg1 = inter_sum1 / both_l
        avg2 = inter_sum2 / both_l
        part1 = 0
        part2_1 = 0
        part2_2 = 0
        for i in output1:
            part1 += (output1[i] - avg1) * (output2[i] - avg2)
            part2_1 += (output1[i] - avg1) ** 2
            part2_2 += (output2[i] - avg2) ** 2

        if part2_2 == 0 or part2_1 == 0:
            output = 0
        else:
            output = part1 / (math.sqrt(part2_1) * math.sqrt(part2_2))

        return (x[0][0], x[1][0], output)


    output2 = frequent.filter(lambda x: inter(x)).map(lambda x: inter3(x)).distinct().map(lambda x: {'u1': x[0], 'u2': x[1], 'sim': x[2]}).collect()

    with open(sys.argv[2], 'w') as f_j:
        for i in output2:
            f_j.writelines(json.dumps(i))
            f_j.writelines('\n')

print("Duration: "+str(time.time()-start))