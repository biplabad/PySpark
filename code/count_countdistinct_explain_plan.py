from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD
# import pyspark class Row from module sql
from pyspark.sql import *
# for using explode function
from pyspark.sql.functions import explode
# import sql functions
from pyspark.sql.functions import *
# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)

spark = SparkSession(sc)#if Dataframe is used, this has to be there


# create data for employee

employee1       = Row(firstname = "John", 	    lastname = "Rambo", 		salary = 2500, age = 28)
employee2       = Row(firstname = "Spider", 	lastname = "Man", 			salary = 3500, age = 27)
employee3       = Row(firstname = "Super", 	    lastname = "Man",           salary = 2000, age = 32)
employee4       = Row(firstname = "Hit"  ,      lastname = "Man", 			salary = 4000, age = 30)
employee5       = Row(firstname = "Jocker", 	lastname = "Actual Hero", 	salary = 5000, age = 31)
employee6       = Row(firstname = "Rocky", 	    lastname = "Handsome", 		salary = 4500, age = 29)
employee7       = Row(firstname = "Harley", 	lastname = "Devidson", 		salary = 2500, age = 28)
employee8       = Row(firstname = "Kwasaki", 	lastname = "Ninja", 		salary = 3500, age = 27)
employee9       = Row(firstname = "BMW", 	    lastname = "Sr1000" ,       salary = 2000, age = 32)
employee10      = Row(firstname = "Suzuki" ,    lastname = "Hayabusa", 		salary = 4000, age = 30)
employee11      = Row(firstname = "Jocker", 	lastname = "Actual Hero", 	salary = 5000, age = 31)
employee12      = Row(firstname = "Rocky", 	    lastname = "Handsome", 		salary = 4500, age = 29)


# creating rdd

rdd1 = sc.parallelize([employee1,employee2,employee3,employee4,employee5,employee6,
                       employee7,employee8,employee9,employee10,employee11,employee12])

# creating dataframe from rdd

df1 = spark.createDataFrame(rdd1)

# using hasattr so that create DataFrame does not show error

hasattr(rdd1, "createDataFrame")

df1.show()

# count distinct_last_names

countDF = df1.select("firstname", "lastname")\
  .groupBy("firstname")\
  .agg(count("lastname").alias("count_last_names"))

countDF.show()

# result
#+---------+----------------+
#|firstname|count_last_names|
#+---------+----------------+
#|    Rocky|               2|
#|   Spider|               1|
#|    Super|               1|
#|  Kwasaki|               1|
#|     John|               1|
#|      BMW|               1|
#|   Suzuki|               1|
#|   Jocker|               2|
#|   Harley|               1|
#|      Hit|               1|
#+---------+----------------+

countDistinctDF = df1.select("firstname", "lastname")\
  .groupBy("firstname")\
  .agg(countDistinct("lastname").alias("distinct_last_names"))

countDistinctDF.show()

# result
#+---------+-------------------+
#|firstname|distinct_last_names|
#+---------+-------------------+
#|    Rocky|                  1|
#|   Spider|                  1|
#|    Super|                  1|
#|  Kwasaki|                  1|
#|     John|                  1|
#|      BMW|                  1|
#|   Suzuki|                  1|
#|   Jocker|                  1|
#|   Harley|                  1|
#|      Hit|                  1|
#+---------+-------------------+

# explain plan

countDF.explain()

# result

#== Physical Plan ==
#*(2) HashAggregate(keys=[firstname#1], functions=[count(lastname#2)])
#+- Exchange hashpartitioning(firstname#1, 200)
#   +- *(1) HashAggregate(keys=[firstname#1], functions=[partial_count(lastname#2)])
#      +- *(1) Project [firstname#1, lastname#2]
#         +- Scan ExistingRDD[age#0L,firstname#1,lastname#2,salary#3L]

