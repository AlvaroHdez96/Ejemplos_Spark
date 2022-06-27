package ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
object Ejemplo01Base {
  def main(args: Array[String]): Unit = {
    //Creando el contexto del Servidor
    val sc = new SparkContext("local",
      "Ejemplo01Base",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
  }

}

