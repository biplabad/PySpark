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

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)

spark = SparkSession(sc)#if Dataframe is used, this has to be there

# create data for department

department1 = Row(did='123456', name='Admin')
department2 = Row(did='789012', name='IT')
department3 = Row(did='345678', name='HR')

# create data for employee

employee1 = Row(firstname = "John", 	lastname = "Rambo", 		salary = 2500, age = 28)
employee2 = Row(firstname = "Spider", 	lastname = "Man", 			salary = 3500, age = 27)
employee3 = Row(firstname = "Super", 	lastname = "Man", 			salary = 2000, age = 32)
employee4 = Row(firstname = "Iron", 	lastname = "Man", 			salary = 4000, age = 30)
employee5 = Row(firstname = "Jocker", 	lastname = "Actual Hero", 	salary = 5000, age = 31)
employee6 = Row(firstname = "Rocky", 	lastname = "Handsome", 		salary = 4500, age = 29)

# linking of employee to department

departmentemployee1 = Row(department = department1, employees = [employee1,employee3])
departmentemployee2 = Row(department = department2, employees = [employee2,employee5])
departmentemployee3 = Row(department = department3, employees = [employee4,employee6])

# creating rdd

rdd1 = sc.parallelize([departmentemployee1,departmentemployee2])
rdd2 = sc.parallelize([departmentemployee3])

# creating dataframe from rdd

df1 = spark.createDataFrame(rdd1)
df2 = spark.createDataFrame(rdd2)

# using hasattr so that create DataFrame does not show error

hasattr(rdd1, "createDataFrame")
hasattr(rdd2, "createDataFrame")

#Union of df1 and df2

unionDF = df1.union(df2)

#explode function

explodeDF = unionDF.select(explode("employees").alias("e"))
explodeDF.show()

# result
#+--------------------+
#|                   e|
#+--------------------+
#|[28, John, Rambo,...|
#|[32, Super, Man, ...|
#|[27, Spider, Man,...|
#|[31, Jocker, Actu...|
#|[30, Iron, Man, 4...|
#|[29, Rocky, Hands...|
#+--------------------+

#flatten the data

flattenDF = explodeDF.selectExpr("e.firstName", "e.lastName", "e.salary", "e.age")
flattenDF.show()

#result
#+---------+-----------+------+---+
#|firstName|   lastName|salary|age|
#+---------+-----------+------+---+
#|     John|      Rambo|  2500| 28|
#|    Super|        Man|  2000| 32|
#|   Spider|        Man|  3500| 27|
#|   Jocker|Actual Hero|  5000| 31|
#|     Iron|        Man|  4000| 30|
#|    Rocky|   Handsome|  4500| 29|
#+---------+-----------+------+---+

#filter

filterDF = flattenDF.filter(flattenDF.firstName == "John")
filterDF.show()

#result
#+---------+--------+------+---+
#|firstName|lastName|salary|age|
#+---------+--------+------+---+
#|     John|   Rambo|  2500| 28|
#+---------+--------+------+---+



# Use `|` instead of `or`

filterDF1 = flattenDF.filter((col("salary") >= 2500) | (col("age") <= 29)).sort(asc("lastName"))
filterDF1.show()

filterDF2 = flattenDF.filter((col("firstName") == "Jocker") & (col("salary") == 5000))
filterDF2.show()

# where is equivalent to filter

whereDF = flattenDF.where((col("salary") >= 2500) & (col("age") <= 28)).sort(asc("lastName"))
whereDF.show()