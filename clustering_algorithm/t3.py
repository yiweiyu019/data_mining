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
intermediate = []
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

##compute euclidean distance
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


    return x[0],(center,std,n,sum,sumq)


def compute_maha2(x,d):

    dist = 0
    c = d[1][0]
    s = d[1][1]

    for i in range(0,len(x[1])):
        if s[i] == 0:
            s[i] = 0.00000001
        dist += ((x[1][i]-c[i])/s[i])**2
    dist = math.sqrt(dist)
    return (dist,x[0],d[0])

def compute_maha(x,d):
    result = []
    for i in d:
        result.append(compute_maha2(x,(i,d[i])))
    result = sorted(result)[0]
    if result[0] < threshold:
        return (result[2],result[1])
    else:
        return ('-1',result[1])

def merge_cluster(x,y):
    sum1 = x[1][3]
    sum2 = y[1][3]
    sumq1 = x[1][4]
    sumq2 = y[1][4]
    sum = {}
    sumq = {}
    for i in sum1:
        sum[i] = sum1[i] + sum2[i]
        sumq[i] = sumq1[i] + sumq2[i]
    n = x[1][2] + y[1][2]
    center = {}
    std = {}
    for i in sum:
        center[i] = sum[i]/n
        std[i] = math.sqrt(sumq[i]/n - (sum[i]/n)**2)
    # avg1 = x[1][0]
    # avg2 = y[1][0]
    # std1 = x[1][1]
    # std2 = y[1][1]
    # final_avg = {}
    # final_std = {}
    # for i in avg1:
    #     final_avg[i] = (avg1[i]+avg2[i])/2
    #     final_std[i] = (std1[i]+std2[i])/2
    return (center,std,n,sum,sumq)

def merge(x,d):
    dist = 0
    c = d[1][0]
    s = d[1][1]

    for i in range(0,len(x[1][0])):
        dist += ((x[1][0][i]-c[i])/s[i])**2
    dist = math.sqrt(dist)
    if dist < threshold:
        mergeresult = merge_cluster(x,d)
        return (x[0],d[0],mergeresult)
    else:
        return 0


#--------------------------------Implementation-------------------------------------------------------
DS = {}
CS = {}
RS = {}

DS_data = {}
CS_data = {}


##load data0.txt
data = sc.textFile(path_list[0])

#data = [(0,(x,y,z,...)),(1,(x,y,z...))]
data = data.map(lambda x:x.split(',')).map(lambda x:(int(x[0]),cfloat(x[1:])))
#originial file
#info = {0:(x,y,z...),1:(x,y,z...)}
info = data.collectAsMap()



#compute threshold for maha dist
ind = data.map(lambda x:x[0])
dimension = len(data.first()[1])
print('dimension9')
print(data.first())
threshold = 3 * math.sqrt(dimension)
#threshold2 = 2*math.sqrt(dimension)

#1.pick 1/10 sample data from data0.txt
size = data.count()

#sampleall = [(3,(x,y,z,...)),(17,(x,y,z...))]
sampleall = data.takeSample(False,int(size*0.2))
#sampleall = data.takeSample(False,10000)
sample = sc.parallelize(sampleall)
initial_center = sample.takeSample(False,n_cluster)
initial_centero = []
for i in initial_center:
    initial_centero.append(i[0])
#sample = sample.filter(lambda x:x[0] not in initial_centero)
ds_da = {}
#--->initial_center = [('0',(x,y,z...)),('1',(x,y,z...))]
for i in range(0,len(initial_center)):
    #ds_da[str(i)] = [initial_center[i][0]]
    initial_center[i] = (str(i),initial_center[i][1])
    #ds_da[str(i)] = [initial_center[i][0]]

#1.2 in sample exclude center, computer the euclidean distance from each point to the center(K),cluster them
central = initial_center
#dis = [('4',[rid1,rid2...]),...]
for i in range(0,5):
    sample_cluster = sample.map(lambda x:compute(x,central)).groupByKey().mapValues(list)#.filter(lambda x:len(x[1])<=10)
    ds_d1 = sample_cluster.collectAsMap()
    ds_da = ds_d1
#for i in ds_d1:
#    ds_da[i].extend(ds_d1[i])
#summarization = [('4',({0:-39,1:83...},{0:160,1:100},3180)]
    summarization = sample_cluster.map(lambda x:compt_stat(x)).map(lambda x:(x[0],list((x[1][0]).values()))).collect()
    central = summarization
summarization = sample_cluster.map(lambda x:compt_stat(x)).collectAsMap()
print(DS_data.keys())
ds_n = 0
for i in ds_da:
    ds_n += len(ds_da[i])

print(ds_n)
DS_data = ds_da
DS = summarization
#2. finish initial ds cluster summarization, use originial data(exclude all sample)
#compare each point to ds, compute maha, <threshold ds, >=threshold rs+cs
data = data.filter(lambda x:x not in sampleall)
#ma_dis = [('4',[0,2,9...]),]
# ma_dis = data.map(lambda x:compute_maha(x,summarization)).groupByKey().mapValues(list) #remaining -->ds+(points)
# DS_data = ma_dis.filter(lambda x:x[0]!='-1').collectAsMap()
# for i in DS_data:
#     ds_da[i].extend(DS_data[i])
# DS_data = ds_da
# print('DSDATA')
# print(DS_data)
# ds_s = ma_dis.filter(lambda x:x[0]!='-1').map(lambda x:compt_stat(x))
# ###ds_da (all data points)
# print('DS-D0')
# ds_s0 = ds_s.collectAsMap()
#
# for i in ds_s0:
#     DS[i] = merge_cluster((i,ds_s0[i]),(i,summarization[i]))
# #DS = merge_cluster(ds_s0,summarization)
# print(DS)
# print(DS_data.keys())
# ds_n = 0
# for i in DS_data:
#     ds_n += len(DS_data[i])
#
# print(ds_n)
# # 3. for the remaining points, >=threshold,
# #rs_cs = [37,89,285...]
# rs_cs = ma_dis.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1])#.collect()
# #3.1 choose a large k, rebuild 3*k clusters
# #initial_center2 = [24,85...]
data = data.map(lambda x:x[0])
initial_center2 = data.takeSample(False,3 * n_cluster)
initial_info = []
cs_da = {}
#initial_info = [(0,(x,y,z...))....]
for i in range(0,len(initial_center2)):
    #cs_da[str(i)] = [initial_center2[i]]
    initial_info.append((i,info[initial_center2[i]]))

#exclude initial center from rs_cs
#rs_cs = rs_cs.filter(lambda x:x not in initial_center2)
#calculating initial cs


for i in range(0,5):
    dis2 = data.map(lambda x:compute2(x,initial_info)).groupByKey().mapValues(list)
    #ds_d1 = dis2.collectAsMap()
    summarization2 = dis2.map(lambda x: compt_stat(x)).map(lambda x: (x[0], list((x[1][0]).values()))).collect()
    initial_info = summarization2
#more than one data point in cluster ---cs
CS_data = dis2.filter(lambda x:len(x[1])>1).collectAsMap()

#for i in CS_data:
#    CS_data[i].extend(cs_da[i])

#generate CS summarization
cs = dis2.filter(lambda x:len(x[1])>1).map(lambda x:compt_stat(x))
#only one point,rs,single point
rs = dis2.filter(lambda x:len(x[1])<=1)    #RS first(data0)


print('RS-D0')
rsd0 = rs.flatMap(lambda x:x[1]).collect()
print(rsd0)
for i in rsd0:
    RS[i] = info[i]
print(RS)
cs2 = cs.collectAsMap()
final_cs = {}
print(CS_data.keys())
print(DS_data.keys())
cs_n  = 0
ds_n = 0
for i in CS_data:
    cs_n += len(CS_data[i])
for i in DS_data:
    ds_n += len(DS_data[i])
print(cs_n)
print(ds_n)

counter = []
all_merge = []
all_f = []

for i in combinations(cs2,2):
    all_f.append(i[0])
    all_f.append(i[1])
    temp = merge((i[0],cs2[i[0]]),(i[1],cs2[i[1]]))
    if temp != 0:
        all_merge.append(temp)

nn = 0
all_f = set(all_f)
final_cssum = {}
for i in all_merge:
    if i[0] in all_f and i[1] in all_f:
        final_cs[str(nn)] = CS_data[i[0]]+CS_data[i[1]]
        final_cssum[str(nn)] = i[2]
        all_f.remove(i[0])
        all_f.remove(i[1])
        nn += 1
#print(all_f)
for i in all_f:
    final_cs[str(nn)] = CS_data[i]
    final_cssum[str(nn)] = cs2[i]
    nn+=1

print('CS-D0')
CS_data = final_cs
CS = final_cssum
print(CS)

print(CS_data.keys())
print(DS_data.keys())
cs_n  = 0
ds_n = 0
for i in CS_data:
    cs_n += len(CS_data[i])
for i in DS_data:
    ds_n += len(DS_data[i])
print(cs_n)
print(ds_n)
intermediate.append((1,len(DS_data.keys()),ds_n,len(CS_data.keys()),cs_n,len(RS)))
print('heshi')
print(DS_data)
org = {}
for i in DS_data:
    for j in DS_data[i]:
        org[j] = int(i)
org = sorted(org.items())
final_org = {}
for i in org:
    final_org[str(i[0])] = i[1]
outputj = json.dumps(final_org)
with open('out.json','w') as f_j:
    f_j.write(outputj)
#this

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





######second file------------------------------------------------------------
for pp in range(1,len(path_list)):
    print('this is file')
    print(pp)

    data_2 = sc.textFile(path_list[pp])
    data_2 = data_2.map(lambda x:x.split(',')).map(lambda x:(int(x[0]),cfloat(x[1:])))
    #originial file
    #info = {0:(x,y,z...),1:(x,y,z...)}
    info = data_2.collectAsMap()

    print('neww')

    summarization = DS

    ma_dis = data_2.map(lambda x:compute_maha(x,summarization)).groupByKey().mapValues(list) #remaining -->ds+(points)

    new_inds = ma_dis.filter(lambda x:x[0]!='-1').collectAsMap()
    ds_temp = []
    for i in new_inds:
        DS_data[i].extend(new_inds[i])



    ds = ma_dis.filter(lambda x:x[0]!='-1').map(lambda x:compt_stat(x))
    ds = ds.collectAsMap()
    for i in ds:
        DS[i] = merge_cluster((i,ds[i]),(i,DS[i]))
    print('DS-D1')

    print(DS)

    now_out = ma_dis.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1]).count()
    ds_n = 0
    for i in new_inds:
        ds_n += len(new_inds[i])
    print('second_this')
    print(ds_n)
    #print(len(new_inds))
    print(now_out)

    # 3. for the remaining points, compare to existing cs
    #rs_cs = [37,89,285...]
    if CS!= {}:
        rs_cs = ma_dis.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1]).map(lambda x:(x,info[x]))#.collect()
        csnew = rs_cs.map(lambda x:compute_maha(x,CS)).groupByKey().mapValues(list)
        new_incs = csnew.filter(lambda x:x[0]!='-1').collectAsMap()
        ds_temp = []
        for i in new_incs:
            CS_data[i].extend(new_incs[i])

        # print(CS_data.keys())
        # print(DS_data.keys())
        # cs_n = 0
        # ds_n = 0
        # for i in CS_data:
        #     cs_n += len(CS_data[i])
        # for i in DS_data:
        #     ds_n += len(DS_data[i])
        # print(cs_n)
        # print(ds_n)
        csnn = csnew.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1]).count()
        print(csnn)
        print('try')

        cst = csnew.filter(lambda x:x[0]!='-1').map(lambda x:compt_stat(x))
        csd = csnew
        cs = cst.collectAsMap()
        ctemp = {}

        for i in cs:
            ctemp[i] = merge_cluster((i,cs[i]),(i,CS[i]))
            #CS_data[i].extend(cs[i])

        CS.update(ctemp)
        print('CS-D1')
        print(CS)

        #csreal = csnew.filter(lambda x:x[0]!='-1').map(lambda x:compt_stat(x))
        rsnew = csnew.filter(lambda x:x[0]=='-1').flatMap(lambda x:x[1])
    #RS.append(rsnew)
    #3.1 choose a large k, rebuild 3*k clusters
    #initial_center2 = [24,85...]

    initial_center2 = rsnew.takeSample(False,3 * n_cluster)
    initial_info = []

    #initial_info = [(0,(x,y,z...))....]
    initial_n = len(CS)
    cc = 0

    for i in range(initial_n,initial_n+len(initial_center2)):
        initial_info.append((i,info[initial_center2[cc]]))
        CS_data[str(i)] = []
        cc+=1

    #exclude initial center from rs_cs
    #rsnew = rsnew.filter(lambda x:x not in initial_center2)
    #calculating initial cs
    dis2 = rsnew.map(lambda x:compute2(x,initial_info)).groupByKey().mapValues(list)



    #more than one data point in cluster ---cs
    cscluster = dis2.filter(lambda x:len(x[1])>1).collectAsMap()

    for i in cscluster:
        CS_data[i].extend(cscluster[i])
    #generate CS summarization
    #
    # print('uuui')
    # cs_n = 0
    # for i in cscluster:
    #     cs_n += len(cscluster[i])
    # cs_n2 = 0
    # for i in CS_data:
    #     cs_n2 += len(CS_data[i])
    #
    # print(cs_n)
    # print(cs_n2)
    cs = dis2.filter(lambda x:len(x[1])>1).map(lambda x:compt_stat(x))
    #only one point,rs,single point
    rs = dis2.filter(lambda x:len(x[1])<=1)    #RS first(data0)
    print('RS-D1')
    rrr = rs.flatMap(lambda x:x[1]).collect()
    for i in rrr:
        RS[i] = info[i]
    print(RS)

    cs2 = cs.collectAsMap()
    print('second')
    # CS.extend(cs2)
    # print('datapoint')
    # print(CS_data)
    # print('summarization')
    # print(CS)

    #cs_map_t= cs.collectAsMap()
    #cs_map.update(cs_map_t)
    CS.update(cs2)

    final_cs = {}


    counter = []
    all_merge = []
    all_f = []
    for i in combinations(CS,2):
        all_f.append(i[0])
        all_f.append(i[1])
        temp = merge((i[0], CS[i[0]]), (i[1], CS[i[1]]))
        if temp != 0:
            all_merge.append(temp)
    print('af')
    print(CS.keys())
    print(all_f)
    nn = 0
    all_f = set(all_f)
    final_cssum = {}



    for i in all_merge:
        if i[0] in all_f and i[1] in all_f:
            final_cs[str(nn)] = CS_data[i[0]]+CS_data[i[1]]
            final_cssum[str(nn)] = i[2]
            all_f.remove(i[0])
            all_f.remove(i[1])
            nn += 1

    for i in all_f:
        final_cs[str(nn)] = CS_data[i]
        final_cssum[str(nn)] = CS[i]
        nn += 1
    print('bijiao')
    print(final_cs)
    CS_data = final_cs
    print(CS_data)
    #final_CS = {}      #CS initial round (first data0)
    # for i in final_cs:
    #     result = compt_stat(i)
    #     final_CS[result[0]] = result[1]
    print('CS-D1')
    CS = final_cssum
    print(CS)
    print(CS_data.keys())
    print(DS_data.keys())
    cs_n = 0
    ds_n = 0
    for i in CS_data:
        cs_n += len(CS_data[i])
    for i in DS_data:
        ds_n += len(DS_data[i])
    print(cs_n)
    print(ds_n)
    if pp+1 != len(path_list):
        intermediate.append((pp+1, len(DS_data.keys()), ds_n, len(CS_data.keys()), cs_n, len(RS)))
    # print(CS_data.keys())
    # print(DS_data.keys())
    # cs_n = 0
    # ds_n = 0
    # for i in CS_data:
    #     cs_n += len(CS_data[i])
    # for i in DS_data:
    #     ds_n += len(DS_data[i])
    # print(cs_n)
    # print(ds_n)
    # print(12)




print('ff')
print(DS_data)
print('finalcsd')
print(CS_data)
print(RS)

# for i in DS_data:
#     for j in DS_data[i]:
#         org[j] = int(i)
# org = sorted(org.items())
# final_org = {}
# for i in org:
#     final_org[str(i[0])] = i[1]
# outputj = json.dumps(final_org)
# with open('outputt.json','w') as f_j:
#     f_j.write(outputj)

def compute_mahafinal(x,d):
    result = []
    for i in d:
        result.append(compute_maha2(x,(i,d[i])))
    result = sorted(result)[0]

    return (result[1],result[2])

def mergefinal(x,d):
    dist = 0
    c = d[1][0]
    s = d[1][1]


    mergeresult = merge_cluster(x,d)
    return (x[0],d[0],mergeresult)

final_ds = {}
final_dssum = {}
print('stat')
print(CS.keys())
print(CS_data.keys())
DS_data['-1'] = []
for i in CS:
    output = compute_maha((i,CS[i][0]),DS)




    counter = []
    all_merge = []
    all_f = []


    #temp = mergefinal((output[1], CS_data[output[1]]),(output[0],DS_data[output[0]]))
    print(output)
    #if output[1] == '-1':
    print(DS_data.keys())

    DS_data[output[0]].extend(CS_data[output[1]])

    #else:
    #    DS_data[output[1]].extend(CS_data[output[0]])
    #final_cssum[temp[1]] = temp[2]
for i in RS:
    print(i)
    output = compute_maha((i, RS[i]), DS)
    print(output)

    DS_data[output[0]].append(i)

    # if output[1] == '-1':
    #     if output[1] in DS_data:
    #         DS_data[output[1]].append(i)
    #     else:
    #         DS_data[output[1]] = [i]
    # else:
    #     DS_data[output[1]].append(i)


print('resu')
print(DS_data)
print(DS_data.keys())
aln = 0
for i in DS_data:
    aln += len(DS_data[i])
print(aln)
intermediate.append((len(path_list), len(DS_data.keys()), aln, 0,0,0))
print(intermediate)

org = {}
for i in DS_data:
    for j in DS_data[i]:
        org[j] = int(i)
org = sorted(org.items())
final_org = {}
for i in org:
    final_org[str(i[0])] = i[1]
outputj = json.dumps(final_org)
with open('output_1.json','w') as f_j:
    f_j.write(outputj)
print(2)
print("Duration: "+str(time.time()-start))





