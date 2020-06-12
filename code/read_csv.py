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

# import tyepes
from pyspark.sql.types import *

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)#if Dataframe is used, this has to be there

c1=spark.read.option("header","true").option("inferSchema","true").format("csv").load("C:/Users/q831258/Downloads/pyprojects/product_revenue.csv")
c1.printSchema()
#root
 #|-- PRODUCT: string (nullable = true)
 #|-- CATEGORY: string (nullable = true)
 #|-- REVENUE: integer (nullable = true)
 #|-- REVENUE_DIFFERENCE: integer (nullable = true)
 #|-- RANK: integer (nullable = true)
c1.show()
#+-------+--------+-------+------------------+----+
#|PRODUCT|CATEGORY|REVENUE|REVENUE_DIFFERENCE|RANK|
#+-------+--------+-------+------------------+----+
#|    abc|     xyz|     10|                10|  10|
#+-------+--------+-------+------------------+----+


