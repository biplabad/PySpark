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

print(department3)
print(student5)
print(departmentstudent1.students[0].firstname)








