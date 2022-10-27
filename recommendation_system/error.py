from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import json
import math
import sys
file_name='task3user.predict'
conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)
rdd=sc.textFile('test_review_ratings.json')
print(rdd.first())
# js=rdd.map(lambda x : json.loads(x))
# rdd_training=js.map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
# rdd1=sc.textFile(file_name)
# js1=rdd1.map(lambda x : json.loads(x))
# pre=js1.map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
# diff=rdd_training.join(pre)
# len=diff.count()
# dd=diff.map(lambda x:['1',((x[1][0]-x[1][1])**2)]).reduceByKey(lambda x,y:x+y).collect()
# print(math.sqrt(dd[0][1]/len))