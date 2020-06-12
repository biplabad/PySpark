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


df_json = spark.read.option("multiline", "true").json("C:/Users/q831258/Downloads/pyprojects/baby1.json")
df_json.show()
df_json.printSchema()
#working code
#df_json.write.mode("overwrite").saveAsTable("Baby");
#spark.sql("select * from Baby").show()

df_json.select("First_Name").show()
df_json.select(df_json["First_Name"], df_json["dob"], ((df_json["dob"]-1).cast(IntegerType())).alias("changed_dob")).show()
#+----------+----+-----------+
#|First_Name| dob|changed_dob|
#+----------+----+-----------+
#|     DAVID|2013|       2012|
#|    JAYDEN|2013|       2012|
#|      RUBY|2014|       2013|
#|     MOSHE|2012|       2011|
#|     ETHAN|2015|       2014|
#|     EDDIE|2012|       2011|
#|    RACHEL|2014|       2013|
#|     ELENA|2014|       2013|
#|    MIGUEL|2013|       2012|
#|      ROSY|2015|       2014|
#+----------+----+-----------+

#filter
df_json.filter(df_json["FATHER_INCOME"]>3750).show()
#group by and count
df_json.groupBy("DOB").count().show()

#global temp view
df_json.createOrReplaceGlobalTempView("baby_gv")
#sql operation on global temp view
spark.sql("select race from global_temp.baby_gv").show()
#using from new session
spark.newSession().sql("select state from global_temp.baby_gv").show()



