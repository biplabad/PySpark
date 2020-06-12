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


# define row definition
person = Row('name','age')

# create instance
person1 = person('Alice', 10)
person2 = person('Biplab', 20)

# create RDD
persons = sc.parallelize([person1,person2])

# Create Data Frame
df2 = spark.createDataFrame(persons)

hasattr(persons, "createDataFrame")

df2.show()

# replace the value of a column by null

df3 = df2.withColumn('name', regexp_replace('name','Alice','null'))

df3.show()