from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark import SparkContext
import csv,sys;

import pandas as pd


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = SparkContext.getOrCreate()
df = spark.read.csv("C:\\Users\\S V S K REDDY\\Downloads\\survey.csv", header=True);
df.show()



##  Save to file - in the end
##df.write.csv("C:\\Users\\S V S K REDDY\\Downloads\\output.csv");

df.createOrReplaceTempView("survey")
## Union
df1 = df.limit(5)
df2 = df.limit(10)
unionDf = df1.unionAll(df2)
unionDf.show()

unionDf.orderBy('Country').show()

## Groupby
df.groupBy("treatment").count().show()

## Aggregate functions

sqlDF = spark.sql("SELECT max(`Age`) FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT avg(`Age`) FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT Sum(`Age`) FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT min(`Age`) FROM survey")
sqlDF.show()
##  duplicates

df.dropDuplicates()
df.show()

##  13th row
df13 = df.take(13)
print(df13)

##  Join operation
joined_df = df1.join(df2, df1.Country == df2.Country)
joined_df.show()

def getdata():
    return pd.read_csv("C:\\Users\\S V S K REDDY\\Downloads\\survey.csv",sep=',')
dfd=spark.udf.register('GATE_TIME',getdata())
dfd.registerTempTable("survey")
df.createOrReplaceTempView("survey")
#df.registerTempTable("survey")









