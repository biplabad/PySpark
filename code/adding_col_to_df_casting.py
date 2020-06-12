from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD

# import pyspark class Row from module sql
from pyspark.sql import *

# for using explode function
from pyspark.sql.functions import explode

# import sql functions
from pyspark.sql.functions import *

from pyspark.sql import functions as F
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

#+-----+------+----------+---+----+
#|Count|County|First Name|Sex|Year|
#+-----+------+----------+---+----+
#|  272| KINGS|     DAVID|  M|2013|
#|  268| KINGS|    JAYDEN|  M|2013|
#|  219|QUEENS|    JAYDEN|  M|2013|
#|  219| KINGS|     MOSHE|  M|2013|
#|  216|QUEENS|     ETHAN|  M|2013|
#+-----+------+----------+---+----+

add_n = udf(lambda x, y: x + y, IntegerType())

# We register a UDF that adds a column to the DataFrame, and we cast the id column to an Integer type.
df1 = df_json.withColumn('id_offset', add_n(F.lit(10000), df_json.Count.cast(IntegerType())))
#The lit() function creates a Column object out of a literal value. Let's create a DataFrame and use the lit() function to append a spanish_hi column to the DataFrame. The lit() function is especially useful when making boolean comparisons.

df1.show()
#+-----+------+----------+---+----+---------+
#|Count|County|First Name|Sex|Year|id_offset|
#+-----+------+----------+---+----+---------+
#|  272| KINGS|     DAVID|  M|2013|    10272|
#|  268| KINGS|    JAYDEN|  M|2013|    10268|
#|  219|QUEENS|    JAYDEN|  M|2013|    10219|
#|  219| KINGS|     MOSHE|  M|2013|    10219|
#|  216|QUEENS|     ETHAN|  M|2013|    10216|
#+-----+------+----------+---+----+---------+