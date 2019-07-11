import sys
import os

os.environ["SPARK_HOME"] = "C:\\spark\\spark-2.4.2-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"] = "C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 7777)


words = lines.flatMap(lambda line: line.split(" "))


pairs = words.map(lambda word : (len(word), word))
wordCounts = pairs.reduceByKey(lambda x, y: x +","+ y)
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()