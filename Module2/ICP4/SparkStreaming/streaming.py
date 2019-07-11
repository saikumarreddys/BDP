import sys
import os

os.environ["SPARK_HOME"] = "C:\\spark\\spark-2.4.2-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]= "C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext




def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 3)
    lines = ssc.textFileStream('log')
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
