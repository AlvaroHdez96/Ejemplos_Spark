package ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Ejemplos04CSV {
  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Ejemplo04CSV", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val df = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";").load("resources/people.csv").toDF()
    println("Show")
    df.show()
    println("Schema")
    df.printSchema()
    println("col name -> int")
    df.col("name").cast("int")
    println("Renombrando columnas")
    df.withColumnRenamed("name","Nombre")
    df.show()
    df.write.format("csv").option("header", "true").option("delimiter", ";").save("resources/salida")


  }

}

