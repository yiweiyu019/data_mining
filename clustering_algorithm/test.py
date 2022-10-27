# import os
# dir = 'data/test1'
# path_list = []
# for i in os.listdir(dir):
#     path_list.append(dir+'/'+i)
#
# print(path_list)
# f = open(path_list[0])
import math
#a = [1,2,3,4,5]
#b = [2,3,4,5,6]
a = [1,2,-1]
b = [0,0,0]
def distance_e(x,y):
    result = 0
    for i in range(0,len(x)):
        result += (x[i] - y[i])**2
    result = math.sqrt(result)
    return result

print(distance_e(a,b))




