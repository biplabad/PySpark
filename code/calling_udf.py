from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
import pandas as pd
import numpy as np
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
#conf = SparkConf().setAppName("window1")
#conf.setMaster('local')
#sc = SparkContext(conf=conf)

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

df1.createOrReplaceGlobalTempView("salary_tv")
# SQL can be run over DataFrames that have been registered as a table.
df_sal = spark.sql("SELECT salary FROM global_temp.salary_tv")
# df_sal.show()
from udf.bool_test import bool_test
for salary in df_sal.rdd.collect():
    print(salary)
    print(bool_test(salary))

