package ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Ejemplo15mapreduce {
  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local", "Ejemplo15mapreduce", System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val lines=sc.parallelize(List("this","is","an","example"))
    println("Lines:")
    lines.foreach(item => println(item))
    val countLines=lines.map(line => line.length).count()
    println("Count:"+countLines)
    val lengthLines=lines.map(line => line.length).cache()
    println("lengthLines:")
    lengthLines.foreach(item => println(item))
    val numeros=sc.parallelize(List(2,3,4,5))
    println("numeros:")
    numeros.foreach(item => println(item))
    val incrementados=numeros.map(_+1).cache()
    println("incrementados:")
    incrementados.foreach(item => println(item))
    println("reduciendo:")
    val reducidos=incrementados.reduce((a,b) => {
      println("a:"+a)
      println("b:"+b)
      //va sumando un elemento con el anterior
      a+b
    })
    println("reducidos:"+reducidos)


    val lines2=sc.parallelize(List("this is","an example"))
    val aplanados=lines2.flatMap(_.split(" "))
    println("aplanados:")
    aplanados.foreach(println)
    val filtrados=aplanados.filter(word=>word.contains("this"))
    println("filtrados:")
    filtrados.foreach(println)
    val mapeado=filtrados.map((_,1))
    println("mapeado:")
    mapeado.foreach(println)
    val lines3=sc.parallelize(List("this is","an example"))
    val aplanados3=lines3.map(_.split(" "))
    println("aplanados3:")
    aplanados3.foreach(_.foreach(println))
    val filtrados3=aplanados3.filter(word=>word.contains("this"))
    println("filtrados3:")
    filtrados3.foreach(_.foreach(println))
    val mapeado3=filtrados3.map((_,1))
    println("mapeado3:")
    mapeado3.foreach(item => {
      println(item._1.foreach(println))
    })
  }
}
