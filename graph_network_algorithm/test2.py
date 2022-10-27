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
sc = pyspark.SparkContext("local[*]","task11")
# graph2 = {
#     'A': ['B', 'C'],
#     'B': ['A', 'C','D'],
#     'C': ['A', 'B'],
#     'D': ['B','G','E','F'],
#     'E': ['D','F'],
#     'G': ['D','F'],
#     'F': ['D','E','G']
# }
# node = ['A','B','C','D','E','F','G']
# graph = {
#     'A':['B','C','D'],
#     'B':['A','C'],
#     'C':['A','B','D','E'],
#     'D':['A','C','E'],
#     'E':['C','D']
# }

# node = ['A', 'B', 'C', 'D', 'E', 'F', 'H', 'G']
# graph2 = {'A': ['B', 'C', 'D'],
#          'B': ['A', 'C', 'D'],
#          'C': ['A', 'B', 'D'],
#          'D': ['A', 'B', 'C', 'E'],
#          'E': ['D', 'F','G'],
#          'F': ['E', 'H', 'G'],
#          'H': ['F', 'G'],
#          'G': ['E', 'F', 'H']
#          }
node = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
graph2 = {
    'a': ['j', 'b'],
    'b': ['a', 'j', 'c'],
    'c': ['b', 'j', ],
    'd': ['c', 'e'],
    'e': ['d',  'h', 'f', 'g'],
    'f': ['e', 'h', 'g'],
    'g': ['h', 'e', 'f'],
    'h': ['i',  'e', 'f', 'g'],
    'i': ['j',  'h'],
    'j': ['a', 'b', 'c', 'i'],


}
# node = [1,2,3,4,5,6,7]
# graph2 = {
#     1:[2,5],
#     2:[1,3,6],
#     3:[2,4],
#     4:[3,7],
#     5:[1,6,7],
#     6:[2,5],
#     7:[5,4]
# }
# node = [1, 2, 4, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 18, 21]
# graph2 = {1: [4, 8, 21],
#          2: [4, 12, 11],
#          # 3: [19, 20],
#          4: [1, 2, 11, 13, 14],
#          5: [8, 12, 15],
#          # 6: [19],
#          7: [21, 18],
#          8: [1, 5, 12, 16],
#          9: [13, 16, 18],
#          # 10: [19, 22, 17],
#          11: [2, 4, 12],
#          12: [2, 5, 8, 11],
#          13: [4, 9],
#          14: [4, 21],
#          15: [5, 16, 18],
#          16: [8, 9, 15, 18],
#          # 17: [10, 19],
#          18: [7, 9, 2, 16, 15],
#          # 19: [5, 6, 20, 3, 17, 10],
#          # 20: [19, 3],
#          21: [1, 7, 14],
#          # 22: [10]
#          }
# #node = ['A','B','C','D','E']

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
    print(innode)
    vertex = vertex[::-1]
    bet = {}
    bet_e = {}
    for i in vertex:
        if len(innode[i]) == 0:
            bet[i] = 1
        else:
            bet[i] = 1/len(innode[i])

    for i in vertex:
        par = innode[i]

        for p in par:
            if len(innode[p])>=2:
                bet[p] += bet[i]/len(innode[p])
            else:
                bet[p] +=bet[i]
            if tuple(sorted((p,i))) in bet_e:
                bet_e[tuple(sorted((p,i)))] += bet[i]
            else:
                bet_e[tuple(sorted((p, i)))] = bet[i]

    for i in bet_e:
        yield (i,bet_e[i])



print(2)
node = sc.parallelize(node).flatMap(lambda x:bfs(graph2,x)).reduceByKey(add).mapValues(lambda x:x/2.0).sortBy(lambda x:(-x[1],x[0][0]))

info = node.collect()

with open(path_o,'w') as f_j:
    for i in info:
        f_j.write(str(i[0]) + ', ' + str(i[1]))
        f_j.write('\n')

