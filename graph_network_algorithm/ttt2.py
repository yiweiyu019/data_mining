# graph = {
#     'A': ['B', 'C'],
#     'B': ['A', 'C'],
#     'C': ['A', 'B'],
#     'D': ['G','E','F'],
#     'E': ['D','F'],
#     'G': ['D','F'],
#     'F': ['D','E','G']
# }
# node = ['A','B','C','D','E','F','G']
#
#
# def bfs_tree(graph,s):
#
#     vertex = []
#     visit = []
#     edge = []
#     outnode = {}
#     innode = {}
#     vertex.append(s)
#     visit.append(s)
#     innode[s] = []
#     outnode[s] = []
#     for i in graph[s]:
#         edge.append((s,i))
#         visit.append(i)
#     path = graph[s]
#     while 1:
#         temp = []
#         for n in path:
#             vertex.append(n)
#             outnode[n] = []
#             for j in graph[n]:
#                 if j not in visit:
#                     edge.append((n,j))
#                     if j not in temp:
#                         temp.append(j)
#         visit.extend(temp)
#         if temp == []:
#             # if len(path) == 2:
#             #     edge = edge[:-1]
#             break
#
#         path = temp
#     return edge,vertex
#
#
#
#
#
#
# def detection(graph,node):
#     unvisit = node.copy()
#     edgelist = []
#     #s = unvisit.pop()
#     while unvisit:
#         s = unvisit.pop()
#         edge,vertext = bfs_tree(graph,s)
#         edgelist.append(vertext)
#         # if edge == []:
#         #     edgelist.append(vertext)
#         # else:
#         #     edgelist.append(edge)
#         for i in vertext:
#             if i in unvisit:
#                 unvisit.remove(i)
#
#     return edgelist
#
# print(detection(graph,node))
# print(node)

import pyspark
import sys
from operator import add
import os
from itertools import combinations
import time
from copy import deepcopy


sc = pyspark.SparkContext("local[*]","task21")
print(2)
#a = {1:[2,3,4],2:[2,3,4,5]}
a = [(1,2,3)]
rdd = sc.parallelize(a).first()
print(rdd)