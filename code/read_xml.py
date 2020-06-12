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

sqlContext = SQLContext(sc)
# not working
x1=sqlContext.read.format('com.databricks.spark.xml').option('inferSchema','true')\
    .option('rootTag','facility').option ('rowTag','metadata')\
    .load('C:/Users/q831258/Downloads/pyprojects/ENTITY_TYPE-FACILITY.xml')
x1.printSchema()
x1.show()

#df = sqlContext.read \
# .format('com.databricks.spark.xml') \
#   .options(rowTag='book') \
#    .load('s3n://######/###/######/books.xml',schema = customSchema)

#x2=spark.read.format("com.databricks.spark.xml").option("inferSchema","true")\
 #   .option("rootTag","patientData").option ("rowTag","dataSet")\
  #  .load("C:/Users/q831258/Downloads/pyprojects/ENTITY_TYPE-RECORD-caselog.xml")
#x2.printSchema()
#x2.show()