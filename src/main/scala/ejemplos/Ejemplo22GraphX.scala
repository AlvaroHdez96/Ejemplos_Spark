package ejemplos

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object Ejemplo22GraphX {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local",
      "Ejemplo01Base",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")

    // create vertices RDD with ID and Name
    val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")))
    val vRDD= sc.parallelize(vertices)
    vRDD.take(1)
    // Array((1,SFO))

    // Defining a default vertex called nowhere
    val nowhere = "nowhere"

    // create routes RDD with srcid, destid, distance
    val edges = Array(Edge(1L,2L,1800),Edge(2L,3L,800),Edge(3L,1L,1400))
    val eRDD= sc.parallelize(edges)

    eRDD.take(2)
    // Array(Edge(1,2,1800), Edge(2,3,800))
    // define the graph
    val graph = Graph(vRDD,eRDD, nowhere)
    // graph vertices
    graph.vertices.collect.foreach(println)
    // (2,ORD)
    // (1,SFO)
    // (3,DFW)

    // graph edges
    graph.edges.collect.foreach(println)

    // Edge(1,2,1800)
    // Edge(2,3,800)
    // Edge(3,1,1400)
    // How many airports?
    val numairports = graph.numVertices
    // Long = 3
    // How many routes?
    val numroutes = graph.numEdges
    // Long = 3
    // routes > 1000 miles distance?
    graph.edges.filter { case Edge(src, dst, prop) => prop > 1000 }.collect.foreach(println)
    // Edge(1,2,1800)
    // Edge(3,1,1400)

    // triplets
    graph.triplets.take(3).foreach(println) //((1,SFO),(2,ORD),1800)
    //((2,ORD),(3,DFW),800)
    //((3,DFW),(1,SFO),1400)

    // print out longest routes
    graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
      "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect.foreach(println)

    //Distance 1800 from SFO to ORD.
    //Distance 1400 from DFW to SFO.
    //Distance 800 from ORD to DFW.
  }

}


