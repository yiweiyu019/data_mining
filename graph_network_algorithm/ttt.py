def bfs(graph, start, end):
    # maintain a queue of paths
    queue = []
    # push the first path into the queue
    queue.append([start])
    final = []
    while queue:
        # get the first path from the queue
        path = queue.pop(0)
        # get the last node from the path
        node = path[-1]
        # path found
        if node == end:
            final.append(path)
            continue
        # enumerate all adjacent nodes, construct a new path and push it into the queue
        for adjacent in graph.get(node, []):
            new_path = list(path)
            new_path.append(adjacent)
            queue.append(new_path)

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
print(bfs(graph2,4,15))