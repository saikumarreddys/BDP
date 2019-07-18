import java.util
import org.apache.spark.sql._
import org.graphframes.GraphFrame


object GraphFrames2 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()



    val stationdata = spark.read.option("header", "true").csv("C:\\Users\\S V S K REDDY\\Downloads\\201508_station_data.csv")
    val tripdata = spark.read.option("header", "true").csv("C:\\Users\\S V S K REDDY\\Downloads\\201508_trip_data.csv")
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
      .withColumnRenamed("Trip ID", "tripid")
      .withColumnRenamed("Trip ID", "tripid")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("Start Terminal", "StartTerminal")
      .withColumnRenamed("End Terminal", "EndTerminal")
      .withColumnRenamed("Bike #", "bike")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")
    //Creating the graphframe
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    //2.Triangle count
    val result=stationGraph.triangleCount.run()
    result.select("id","count").show()

    //3.shortest Paths
    val list = new util.ArrayList[Any]
    list.add("2nd at Folsom")
    list.add("California Ave Caltrain Station")
    val results = stationGraph.shortestPaths.landmarks(value = list).run()


    results.select("id","distances").show()

    //4.Apply page rank
    val result4 = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    result4.vertices.select("id", "pagerank").show(10)
    result4.edges.select("src", "dst", "weight").distinct().show(10)



    //5.save graph
    //stationGraph.vertices.write.parquet("vertices")
    //stationGraph.edges.write.parquet("edges")

    //Bonus BFS

    stationGraph.bfs
      .fromExpr("id = 'Townsend at 7th'")
      .toExpr("id = 'Spear at Folsom'")
      .maxPathLength(2).run().show(10)
    // Label Propagation
    val result5 = stationGraph.labelPropagation.maxIter(5).run()
    result5.orderBy("label").show(10)

  }
}
