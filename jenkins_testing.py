import sys
from pyspark.sql.functions import countDistinct
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number , max

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
simpleData = (("James", "Sales", 3000,'M'),
              ("Michael", "Sales", 4600,'M'),
              ("Robert", "Sales", 4100,'M'),
              ("Maria", "Finance", 3000,'F'),
              ("James", "Sales", 3000,'F'),
              ("Scott", "Finance", 3300,'M'),
              ("Jen", "Finance", 3900,'F'),
              ("Jeff", "Marketing", 3000,'M'),
              ("Kumar", "Marketing", 2000,'M'),
              ("Saif", "Sales", 4100,'F')
              )

columns = ["employee_name", "department", "salary","Gender"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.write.csv(path='mycsv.csv',mode="overwrite")
