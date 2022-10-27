import pyspark
import sys
from operator import add
import os
from itertools import combinations
import time

from test import bet_e

start = time.time()
threshold = int(sys.argv[1])
path_i = sys.argv[2]
path_o = sys.argv[3]
sc = pyspark.SparkContext("local[*]","task11")
node1 = [1, 2, 4, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 18, 21]
graph2 = {1: [4, 8, 21],
         2: [4, 12,18, 11],
         # 3: [19, 20],
         4: [1, 2, 11, 13, 14],
         5: [8, 12, 15],
         # 6: [19],
         7: [21, 18],
         8: [1, 5, 12, 16],
         9: [13, 16, 18],
         # 10: [19, 22, 17],
         11: [2, 4, 12],
         12: [2, 5, 8, 11],
         13: [4, 9],
         14: [4, 21],
         15: [5, 16, 18],
         16: [8, 9, 15, 18],
         # 17: [10, 19],
         18: [7, 9, 2, 16, 15],
         # 19: [5, 6, 20, 3, 17, 10],
         # 20: [19, 3],
         21: [1, 7, 14],
         # 22: [10]
         }
m = 0
for i in graph2:
    m+= len(graph2[i])
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

    #print(edge)
    #print(outnode)
    #print(innode)
    path_result = find_short(outnode, innode)
    #print(path_result)
    #print(edge)
    #print(outnode)
    #print(innode)
    vertex = vertex[::-1]
    bet = {}
    bet_e = {}


    for i in vertex:
        bet[i] = 1
    #print(bet)
    for i in vertex:
        for k in innode[i]:
            bet_e[tuple(sorted((k,i)))] = bet[i] * (path_result[k]/path_result[i])
            bet[k] += bet[i] * (path_result[k]/path_result[i])
    for i in bet_e:
        yield (i,bet_e[i])




print(2)
node = sc.parallelize(node1).flatMap(lambda x:bfs(graph2,x)).reduceByKey(add).mapValues(lambda x:x/2.0).sortBy(lambda x:(-x[1],x[0][0]))

info = node.collect()

with open(path_o,'w') as f_j:
    for i in info:
        f_j.write(str(i[0]) + ', ' + str(i[1]))
        f_j.write('\n')

print('this is info ')
print(info)
####communities
def bfs_tree(graph,s):

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
    return edge,vertex






def detection(graph,node):
    unvisit = node.copy()
    edgelist = []
    #s = unvisit.pop()
    while unvisit:
        s = unvisit.pop()
        edge,vertext = bfs_tree(graph,s)
        edgelist.append(vertext)
        # if edge == []:
        #     edgelist.append(vertext)
        # else:
        #     edgelist.append(edge)
        for i in vertext:
            if i in unvisit:
                unvisit.remove(i)

    return edgelist

def modularity_cal(commu,graph):
    modul = 0
    for c in commu:
        for i in combinations(c,2):
            ki = graph[i[0]]
            kj = graph[i[1]]
            if i[0] in kj:
                aij = 1
            else:
                aij = 0
            modul += (aij-(len(ki)*len(kj))/(2*m))
    normal_modul = modul/(2*m)
    return normal_modul






def count_degree(graph):
    count = 0
    for i in graph:
        count += len(graph[i])
    if count != 0:
        return True

print(1)
result = {}
max_q = -999
while count_degree(graph2):
    high_bet = []
    hbet_score = info[0][1]
    for i in info:
        if i[1] == hbet_score:
            high_bet.append(i[0])
        if i[1] != hbet_score:
            break
    # print("high_bet")
    # print(high_bet)
    for remove_edge in high_bet:

        graph2[remove_edge[0]].remove(remove_edge[1])
        graph2[remove_edge[1]].remove(remove_edge[0])

    communities = detection(graph2,node1)
    mod_t = modularity_cal(communities,graph2)
    result[mod_t] = communities
    if mod_t > max_q:
        max_q = mod_t
    #(mod_t)
    #print(communities)
    bet_list = sc.parallelize(node1).flatMap(lambda x:bfs(graph2,x)).reduceByKey(add).mapValues(lambda x:x/2.0).sortBy(lambda x:(-x[1],x[0][0]))
    info = bet_list.collect()
    print(info)

print('f-n')
print(result[max_q])

