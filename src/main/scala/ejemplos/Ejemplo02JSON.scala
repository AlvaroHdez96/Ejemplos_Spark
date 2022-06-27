package ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Ejemplo02JSON {
  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local",
      "Ejemplo02JSON",
      System.getenv("SPARK_HOME"))
    //Creando la sesión
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val df = spark.read.json("resources/people.json").toDF()
    // Displays the content of the DataFrame to stdout
    println("Show")
    df.show()
    // Print the schema in a tree format
    println("Schema")
    df.printSchema()
    // Select only the "name" column
    println("Select: name")
    df.select("name").show()
    // Select everybody, but increment the age by 1
    println("Select: 'name','age'")
    df.select("name","age").show()
    // Select people older than 21
    println("select: age > 21")
    df.filter("age > 21").show()
    // Count people by age
    println("groupBy: count")
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")
    println("SQL Select")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()





  }

}

