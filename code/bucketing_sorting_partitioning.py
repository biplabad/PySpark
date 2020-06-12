from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD
# import pyspark class Row from module sql
from pyspark.sql import *
# import sql functions
from pyspark.sql.functions import *
# import tyepes
from pyspark.sql.types import *

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)#if Dataframe is used, this has to be there


df_json = spark.read.option("multiline", "true").json("C:/Users/q831258/Downloads/pyprojects/baby1.json")
df_json.show()
df_json.printSchema()

df_json.write.bucketBy(4, "race").sortBy("DOB").saveAsTable("baby_bucketed4")
#spark.sql("select * from baby_bucketed1").show()
print(df_json.rdd.getNumPartitions())
#1

(df_json.write.partitionBy("RACE").bucketBy(4, "DOB").saveAsTable("people_partitioned_bucketed"))
print(df_json.rdd.getNumPartitions())
#1