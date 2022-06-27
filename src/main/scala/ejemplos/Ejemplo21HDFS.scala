package ejemplos

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Ejemplo21HDFS {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = "8020"
    val path= "/user/admin/"
    val filename = "prueba1.csv"
    import scala.sys.process._
    //"hdfs dfs -rm -r /user/admin/prueba.csv" !
    val sc = new SparkContext("local",
      "Ejemplo01Base",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    // Grabar un RDD en HDFS
    val rdd = sc.parallelize(List(
      (0, 60),
      (0, 56),
      (0, 54),
      (0, 62),
      (0, 61),
      (0, 53),
      (0, 55),
      (0, 62),
      (0, 64),
      (1, 73),
      (1, 78),
      (1, 67),
      (1, 68),
      (1, 78)
    ))
    rdd.saveAsTextFile(
      "hdfs://"+host+":"+port+path+filename)
    rdd.collect
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val df = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ",").load("hdfs://"+host+":"+port+path+filename).toDF()
    val dfc=df.collect()
    println(dfc)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.delete(new Path("file://"+path+filename),true)
    //fs.removeAcl(new Path("file://"+path+filename))
  }

}
