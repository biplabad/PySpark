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


# Load a text file and convert each line to a Row.
lines = sc.textFile("C:/Users/q831258/Downloads/pyprojects/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))#each line is converted to a Row

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)

parts1 = lines.map(lambda l: l.split(","))
people1 = parts1.map(lambda p: (p[0], p[1].strip()))#each line is converted to a tuple

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
print(schema)

# Apply the schema to the RDD.
schemaPeople1 = spark.createDataFrame(people1, schema)

# Creates a temporary view using the DataFrame
schemaPeople1.createOrReplaceTempView("people1")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people1")

results.show()



