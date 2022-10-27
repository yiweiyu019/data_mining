import pyspark
import random

num_samples = 100
sc = pyspark.SparkContext(appName="Pi")

def inside(p):
    x,y = random.random(), random.random()
    return x*x + y*y < 1

count = sc.parallelize(range(0, num_samples)).filter(inside).count()

pi = 4 * count / num_samples
print(pi)
#sc.stop()