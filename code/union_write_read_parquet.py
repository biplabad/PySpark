from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD

# import pyspark class Row from module sql
from pyspark.sql import *

# import DBUtils
#pip install DBUtils
#import DBUtils

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)

spark = SparkSession(sc)#if Dataframe is used, this has to be there


#below rdd is is working example
rdd1 = sc.parallelize([(1,2)])
rdd2 = sc.parallelize([(3,4)])


df1 = spark.createDataFrame(rdd1)
df2 = spark.createDataFrame(rdd2)

unionDF = df1.union(df2)

hasattr(rdd1, "createDataFrame")
hasattr(rdd2, "createDataFrame")
#unionDF.show()

# Remove the file if it exists
#dbutils.fs.rm("C:/Users/q831258/Downloads/pyprojects/df5.parquet", True)
#Writing below is a wroking code
unionDF.write.parquet("C:/Users/q831258/Downloads/pyprojects/df52.parquet")

#Reading below is a wroking code
parquetDF = spark.read.parquet("C:/Users/q831258/Downloads/pyprojects/df52.parquet")
parquetDF.show()

