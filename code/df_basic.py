from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# import rdd if at all used else can be ignored
from pyspark import RDD

# import pyspark class Row from module sql
from pyspark.sql import *

# common code
conf = SparkConf().setAppName("window1")
conf.setMaster('local')
sc = SparkContext(conf=conf)





# create data for student and department


department1 = Row(sid='123456', name='Computer Science')
department2 = Row(sid='789012', name='Mechanical Engineering')
department3 = Row(sid='345678', name='Theater and Drama')


student1 = Row(firstname = "John", lastname = "Rambo", section = "A")
student2 = Row(firstname = "Spider", lastname = "Man", section = "B")
student3 = Row(firstname = "Super", lastname = "Man", section = "A")
student4 = Row(firstname = "Iron", lastname = "Man", section = "B")
student5 = Row(firstname = "Jocker", lastname = "Actual Hero", section = "C")
student6 = Row(firstname = "Rocky", lastname = "Handsome", section = "C")


departmentstudent1 = Row(department = department1, students = [student1,student3])
departmentstudent2 = Row(department = department2, students = [student2,student5])
departmentstudent3 = Row(department = department3, students = [student4,student6])


spark = SparkSession(sc) #main line of code

# below is a working code demonstrate use of toDF
#rdd = sc.parallelize([departmentstudent1,departmentstudent2,departmentstudent3])
#hasattr(rdd, "toDF") # hasattr() is an inbuilt utility function in Python which is used in many day-to-day programming applications. Its main task is to check if an object has the given named attribute and return true if present, else false.
#rdd.toDF().show()

# below is a working code demonstrate use of createDataFrame
rdd = sc.parallelize([departmentstudent1,departmentstudent2])
df = spark.createDataFrame(rdd)
hasattr(rdd, "createDataFrame")
df.show()
