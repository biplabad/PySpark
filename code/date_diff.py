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

rec1 = Row(end_dt = "2015-10-14 00:00:00", 	st_dt = "2015-09-14 00:00:00")
rec2 = Row(end_dt = "2015-10-15 01:00:20", 	st_dt = "2015-08-14 00:00:00")
rec3 = Row(end_dt = "2015-10-16 02:30:00", 	st_dt = "2015-01-14 00:00:00")
rec4 = Row(end_dt = "2015-10-17 03:00:20", 	st_dt = "2015-02-14 00:00:00")

# creating rdd

rdd1 = sc.parallelize([rec1,rec2,rec3,rec4])
df2 = spark.createDataFrame(rdd1)
print(df2)

df3 = df2.withColumn('date_diff', F.datediff(F.to_date(df2.end_dt), F.to_date(df2.st_dt)))
df3.show()

# any constants used by UDF will automatically pass through to workers
N = 225
last_n_days = udf(lambda x: x < N, BooleanType())

df_filtered = df3.filter(last_n_days(df3.date_diff))
df_filtered.show()