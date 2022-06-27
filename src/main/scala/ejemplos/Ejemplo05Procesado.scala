package ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Ejemplos05Procesado {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    // Lee el fichero CSV
    val csv = sc.textFile("resources/sample.csv").cache()
    // Divide los datos y los limpia
    val headerAndRows = csv.map(
      line =>
        line.split(",").map(_.trim)
    )
    // Coge la cabecera del fichero (nombres de campos)
    val header = headerAndRows.first
    // Quita la cabecera (eh. comprueba que el primer valor coincide con la cabecera)
    val data = headerAndRows.filter(_(0) != header(0))
    // Cada fila en un map con (nombre_campo,valor)
    val maps = data.map(splits => header.zip(splits).toMap)
    // Filtra el usuario "me" y persiste
    val result = maps.filter(map => map("user") != "me").persist()
    // imprime el resultado
    result.foreach(println)
  }

}

