from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD

# import pyspark class Row from module sql
from pyspark.sql import *

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

#below rdd is is working example
rdd1 = sc.parallelize([(1,2)])
rdd2 = sc.parallelize([(3,4)])

#below rdd is is working example
#rdd1 = sc.parallelize([("a","b")])
#rdd2 = sc.parallelize([("c","d")])

#below rdd is is working example
#rdd1 = sc.parallelize([("a", 1),("a", 2)])
#rdd2 = sc.parallelize([("b", 1),("b", 2)])

df1 = spark.createDataFrame(rdd1)
df2 = spark.createDataFrame(rdd2)

unionDF = df1.union(df2)

hasattr(rdd1, "createDataFrame")
hasattr(rdd2, "createDataFrame")
df1.show()
df2.show()
unionDF.show()
