import json
import pyspark
import csv
import pandas as pd
import sys

sc = pyspark.SparkContext("local","data_p")
rdd = sc.textFile(sys.argv[1])
business = rdd.map(lambda x:json.loads(x))
business_NV = business.filter(lambda x:x['state'] == "NV").map(lambda x:(x['business_id'],x['state']))


rdd2 = sc.textFile(sys.argv[2])
review = rdd2.map(lambda x:json.loads(x))
review_f = review.map(lambda x:(x['business_id'],x['user_id']))

data = review_f.join(business_NV).map(lambda x:(x[1][0],x[0]))

#print(data.count())
data_pd = pd.DataFrame(columns = ['user_id','business_id'], data = data.collect())
data_pd.to_csv(sys.argv[3],index = False)

# f_b = pd.read_json(sys.argv[1],lines = True)
# data = f_b[f_b['state'] == 'NV']['business_id']
#
# f_r = pd.read_json(sys.argv[2],lines = True)
# f_r = f_r[['user_id','business_id']]
# final_data = pd.merge(f_r,data, on = 'business_id',how = 'inner')
# print(final_data.head())