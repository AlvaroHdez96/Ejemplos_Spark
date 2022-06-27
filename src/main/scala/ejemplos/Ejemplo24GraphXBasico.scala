package ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


object Ejemplo24GraphXBasico {
  def main(args: Array[String]): Unit = {
    // Assume the SparkContext has already been constructed
    val sc = new SparkContext("local",
      "Ejemplo24GraphXBasico",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    print(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    print(graph.edges.filter(e => e.srcId > e.dstId).count)

    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map( triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph2 = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph2)

  }

}
