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



null_item_schema = StructType([StructField("col1", StringType(), True),
                               StructField("col2", IntegerType(), True)])
null_df = spark.createDataFrame([("Not Null", 1), (None, 2)], null_item_schema)
null_df.filter("col1 IS NOT NULL").show()