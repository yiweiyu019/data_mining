import pyspark
import sys
from operator import add
import os
from itertools import combinations
import time
start = time.time()
threshold = int(sys.argv[1])
path_i = sys.argv[2]
path_o = sys.argv[3]
path_o2 = sys.argv[4]
sc = pyspark.SparkContext("local[*]","task11")
data = sc.textFile(path_i)
header = data.first()
data = data.filter(lambda x:x!=header).map(lambda x:x.split(',')).map(lambda x:(x[0],x[1]))
group_info = data.groupByKey().mapValues(list).collect()

edge = group_info
edge_list = {}
node_list = []
for i in combinations(edge,2):
    result = len(set(i[0][1]).intersection(i[1][1]))
    if result >=threshold:
        if i[0][0] in edge_list:
            edge_list[(i[0][0])].append(i[1][0])
        else:
            edge_list[(i[0][0])] = [i[1][0]]

        if i[1][0] in edge_list:
            edge_list[(i[1][0])].append(i[0][0])
        else:
            edge_list[(i[1][0])] = [i[0][0]]

        node_list.append((i[0][0],))
        node_list.append((i[1][0],))

node_list = list(set(node_list))

def find_short(out,inn):
    short_path = {}
    temp = {}
    for i in inn:
        if inn[i] == []:
            temp[i] = 1
            short_path[i] = 1
            break

    for i in out:
        par = i
        for ch in out[par]:
            if ch in short_path:
                short_path[ch] += temp[par]
            else:
                short_path[ch] = temp[par]
            if ch in temp:
                temp[ch] += temp[par]
            else:
                temp[ch] = temp[par]
    return short_path

def bfs(graph,s):
    s = s[0]
    vertex = []
    visit = []
    edge = []
    outnode = {}
    innode = {}
    vertex.append(s)
    visit.append(s)
    innode[s] = []
    outnode[s] = []
    for i in graph[s]:
        edge.append((s,i))
        visit.append(i)
    path = graph[s]
    while 1:
        temp = []
        for n in path:
            vertex.append(n)
            outnode[n] = []
            for j in graph[n]:
                if j not in visit:
                    edge.append((n,j))
                    if j not in temp:
                        temp.append(j)
        visit.extend(temp)
        if temp == []:
            # if len(path) == 2:
            #     edge = edge[:-1]
            break

        path = temp
    for i in edge:
        if i[0] in outnode:
            outnode[i[0]].append(i[1])
        else:
            outnode[i[0]] = [i[1]]
        if i[1] in innode:
            innode[i[1]].append(i[0])
        else:
            innode[i[1]] = [i[0]]
    path_result = find_short(outnode, innode)
    vertex = vertex[::-1]
    bet = {}
    bet_e = {}


    for i in vertex:
        bet[i] = 1
    for i in vertex:
        for k in innode[i]:
            bet_e[tuple(sorted((k,i)))] = bet[i] * (path_result[k]/path_result[i])
            bet[k] += bet[i] * (path_result[k]/path_result[i])
    for i in bet_e:
        yield (i,bet_e[i])





node = sc.parallelize(node_list).flatMap(lambda x:bfs(edge_list,x)).reduceByKey(add).mapValues(lambda x:x/2.0).sortBy(lambda x:(-x[1],x[0][0]))

info = node.collect()

with open(path_o,'w') as f_j:
    for i in info:
        f_j.write(str(i[0]) + ', ' + str(i[1]))
        f_j.write('\n')


