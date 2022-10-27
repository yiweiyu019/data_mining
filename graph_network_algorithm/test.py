
graph = {
    'A':[],
    'B':[],
    'C':[],
    'D':[],
    'E':[]
}
node = ['A','B','C','D','E']
#
#
#
# graph2 = {
#     'A': ['B', 'C'],
#     'B': ['A', 'C','D'],
#     'C': ['A', 'B'],
#     'D': ['B','G','E','F'],
#     'E': ['D','F'],
#     'G': ['D','F'],
#     'F': ['D','E','G']
# }
#
# graph3 = {
#     'A': ['A','B', 'C'],
#     'B': ['A', 'B','C','D'],
#     'C': ['A', 'B','C'],
#     'D': ['B','D','G','E','F'],
#     'E': ['D','E','F'],
#     'G': ['D','G','F'],
#     'F': ['D','E','F','G']
# }

node = [1, 2, 4, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 18, 21]
graph2 = {1: [4, 8, 21],
         2: [4, 12, 11],
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

def find_short(out,inn):
    short_path = {}
    temp = {}
    for i in inn:
        if inn[i] == []:
            temp[i] = 1
            short_path[i] = 1
            break

    for i in out:
        #print('i:'+str(i))
        par = i
        #print(temp)
        #print(out[par])
        for ch in out[par]:
            #print(ch)
            if ch in short_path:
                short_path[ch] += temp[par]
            else:
                short_path[ch] = temp[par]
            if ch in temp:
                temp[ch] += temp[par]
            else:
                temp[ch] = temp[par]



    return short_path








def through(graph,s):

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
    score = {}
    for i in edge:
        score[i] = 1
        if i[0] in outnode:
            outnode[i[0]].append(i[1])
        else:
            outnode[i[0]] = [i[1]]
        if i[1] in innode:
            innode[i[1]].append(i[0])
        else:
            innode[i[1]] = [i[0]]
    return edge,outnode,innode,vertex

#edge,outnode,innode = through(graph,'A')
#print(outnode)
#print(innode)
bet_e = {}
for start in ['A']:
    edge,outnode,innode,vertex = through(graph,start)
    path_result = find_short(outnode, innode)
    print(path_result)
    print(edge)
    print(outnode)
    print(innode)
    vertex = vertex[::-1]
    bet = {}



    for i in vertex:
        bet[i] = 1

    for i in vertex:
        for k in innode[i]:
            if tuple(sorted((k,i))) in bet_e:
                bet_e[tuple(sorted((k,i)))] += bet[i] * (path_result[k]/path_result[i])
            else:
                bet_e[tuple(sorted((k, i)))] = bet[i] * (path_result[k] / path_result[i])
            bet[k] += bet[i] * (path_result[k]/path_result[i])

        # if len(innode[i]) == 0:
        #     bet[i] = 1
        # else:
        #     bet[i] = 1/len(innode[i])

    # for i in vertex:
    #     par = innode[i]
    #
    #     for p in par:
    #         if len(innode[p])>=2:
    #             bet[p] += bet[i]/len(innode[p])
    #         else:
    #             bet[p] +=bet[i]
    #         if tuple(sorted((p,i))) in bet_e:
    #             bet_e[tuple(sorted((p,i)))] += bet[i]
    #         else:
    #             bet_e[tuple(sorted((p, i)))] = bet[i]

for i in bet_e:
    bet_e[i] = bet_e[i]/2.0
print(6)
print(bet_e)


# def betweeness(outn,inn,st):
#     result = {}
#     gr = inn
#
#
#     for i in gr:
#         if st!=i:
#             temp = []
#             element = inn[i]
#             for k in element:
#                 for j in k:
#                     temp.append(j)
#                     result[i] = 1/len(inn[i])
#                     if j == st:
#
#         if temp == []:
#             break
#         gr = temp
#     return  result






















def BFS(graph,s):#graph图  s指的是开始结点
    #需要一个队列
    queue=[]
    queue.append(s)
    seen=set()#看是否访问过该结点
    seen.add(s)
    while (len(queue)>0):
        vertex=queue.pop(0)#保存第一结点，并弹出，方便把他下面的子节点接入
        nodes=graph[vertex]#子节点的数组
        for w in nodes:
            if w not in seen:#判断是否访问过，使用一个数组
                queue.append(w)
                seen.add(w)
        print(vertex)


# def bfs(graph, start, end):
#     # maintain a queue of paths
#     all_k = graph.keys()
#     final = []
#     queue = []
#     # push the first path into the queue
#     queue.append([start])
#     while queue:
#         # get the first path from the queue
#         path = queue.pop(0)
#
#
#         # get the last node from the path
#         node = path[-1]
#
#         # path found
#
#         if node == end:
#             return path
#         # enumerate all adjacent nodes, construct a new path and push it into the queue
#         for adjacent in graph.get(node, []):
#             new_path = list(path)
#             new_path.append(adjacent)
#             queue.append(new_path)
# print(111)
# print(bfs(graph2, 4, 15))
# #
