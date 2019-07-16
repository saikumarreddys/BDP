import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD

object SparkGraphFrames {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    //sc = new sparkContext
    /*
        val conf = new SparkConf().setAppName("SparkGraphFrame").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val sqlcontext = new SQLContext(sc)
    */

    val stationdata=spark.read.option("header","true").csv("C:\\BDP\\ICP_%\\Datasets\\Datasets\\201508_station_data.csv")
    val tripdata=spark.read.option("header","true").csv("C:\\BDP\\ICP_%\\Datasets\\Datasets\\201508_trip_data.csv")
    stationdata.printSchema()
    tripdata.printSchema()
    import org.apache.spark.sql.functions.{concat, lit}
    stationdata.select(concat(stationdata("lat"), lit(" "), stationdata("long"))).toDF().show(5, false)

    //creating vertices and removed the duplicates
    val stationVertices = stationdata
      .withColumnRenamed("name", "id")
      .distinct()
    //creating edges
    val tripEdges = tripdata
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
      .withColumnRenamed("Trip ID","tripid")
      .withColumnRenamed("Trip ID","tripid")
      .withColumnRenamed("Start Date","StartDate")
      .withColumnRenamed("End Date","EndDate")
      .withColumnRenamed("End Date","EndDate")
      .withColumnRenamed("Start Terminal","StartTerminal")
      .withColumnRenamed("End Terminal","EndTerminal")
      .withColumnRenamed("Bike #","bike")
      .withColumnRenamed("Subscriber Type","SubscriberType")
      .withColumnRenamed("Zip Code","ZipCode")
    //Creating the graphframe
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    //stationGraph.write.parquet("people.parquet")
    import org.apache.spark.sql.functions.desc
    //
    stationGraph.edges
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)


    stationGraph.edges
      .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)

    val inDeg = stationGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).show(5, false)
    val outDeg = stationGraph.outDegrees
    outDeg.orderBy(desc("outDegree")).show(5, false)

    val motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").show(5, false)

    //bonus1
    stationGraph.degrees.show(5, false)

    //bonus2
    stationGraph.edges
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)

    //bonus3
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    degreeRatio.cache()

    degreeRatio.orderBy(desc("degreeRatio")).limit(10).show(5, false)

    //bonus4
    stationGraph.vertices.write.parquet("vertices")
    stationGraph.edges.write.parquet("edges")

  }
}