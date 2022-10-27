import pyspark
from pyspark.sql import SQLContext
from graphframes import GraphFrame
from graphframes.examples import Graphs
from pyspark.sql.types import StructType,StructField,StringType
import pyspark.sql.functions as F
import sys
from operator import add
import os
from itertools import combinations
import time
start = time.time()
threshold = int(sys.argv[1])
path_i = sys.argv[2]
path_o = sys.argv[3]
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
#sc = pyspark.SparkContext("local[3]","task11")
set_ = pyspark.SparkConf().setMaster('local[3]')
sc = pyspark.SparkContext(conf=set_)
sc.setLogLevel("ERROR")
data = sc.textFile(path_i)
header = data.first()
data = data.filter(lambda x:x!=header).map(lambda x:x.split(',')).map(lambda x:(x[0],x[1]))
group_info = data.groupByKey().mapValues(list).collect()

edge = group_info
edge_list = []
node_list = []
for i in combinations(edge,2):
    result = len(set(i[0][1]).intersection(i[1][1]))
    if result >=threshold:
        edge_list.append((i[0][0],i[1][0]))
        edge_list.append((i[1][0], i[0][0]))
        node_list.append((i[0][0],))
        node_list.append((i[1][0],))

node_list = list(set(node_list))


#node = edge.flatMap(lambda x:x).distinct().map(lambda x:(x,)).collect()
#node = data.map(lambda x:x[0]).distinct().map(lambda x:(x,)).collect()


#print(group_info.take(5))
scs = SQLContext(sc)
#e = edge.toDF(['user_id1','user_id2'])
#n = node.toDF(['user_id'])

v = scs.createDataFrame(node_list,['id'])
e = scs.createDataFrame(edge_list,['src','dst'])
g = GraphFrame(v,e)

result = g.labelPropagation(maxIter=5)
output = result.rdd.map(lambda x:(x[1],x[0])).groupByKey().mapValues(list).map(lambda x:sorted(x[1])).sortBy(lambda x:(len(x),x)).collect()
with open(path_o,'w') as f_j:
    for i in output:
        f_j.write(str(i).replace('[','').replace(']',''))
        f_j.write('\n')

print("Duration: "+str(time.time()-start))
