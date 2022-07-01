package ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
/**
  * Created by AZ on 31.01.2017
  */
//Explicaciçón del proceso en resources/Regression.txt

object Regression {


  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Regression", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("resources/CrabAgePrediction.csv")

    df.show()
    val features = new VectorAssembler()
      .setInputCols(Array("Length","Diameter","Height","Weight","Shucked Weight","Viscera Weight","Shell Weight"))
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0)
      .setElasticNetParam(1)

    val pipeline = new Pipeline().setStages(Array(features, lr))

    val df1 = df.withColumn("label",col("Age").cast("int"))

    // Fit the model
    val model = pipeline.fit(df1)

    val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]
    println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linRegModel.summary.r2}")

    println(s"Model: Y = ${linRegModel.coefficients(0)} * X0 + " +
      s"${linRegModel.coefficients(1)} * X1 + " +
      s"${linRegModel.coefficients(2)} * X2 + " +
      s"${linRegModel.coefficients(3)} * X3 + " +
      s"${linRegModel.coefficients(4)} * X4 + " +
      s"${linRegModel.coefficients(5)} * X5 + " +
      s"${linRegModel.coefficients(6)} * X6 + ${linRegModel.intercept}")



  }

}