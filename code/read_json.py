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


df_json = spark.read.option("multiline", "true").json("C:/Users/q831258/Downloads/pyprojects/baby.json")
df_json.show()
df_json.printSchema()

df_json.createOrReplaceTempView("json_view")
spark.sql("select * from json_view").show()

rdd_json = df_json.toJSON()
print(rdd_json.take(2))

#Spark Take Function. In Spark, the take function behaves like an array. It receives an integer value (let say, n) as a parameter and returns an array of first n elements of the dataset.

