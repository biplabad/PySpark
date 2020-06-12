from pyspark import SparkContext, SparkConf
from pyspark import RDD

conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)


def mod(x):
    import numpy as np #installed numpy package
    return (x,np.mod(x,2))
rdd = sc.parallelize(range(12)).map(mod).take(11)
print(rdd)