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
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline().setStages(Array(features, lr))

    val df1 = df.withColumn("label",col("Age").cast("int"))

    // Fit the model
    val model = pipeline.fit(df1)

    val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]
    println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linRegModel.summary.r2}")

    println(s"Model: Y = ${linRegModel.coefficients(0)} * X + ${linRegModel.intercept}")
    /*
    // Print the coefficients and intercept for linear regression
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    /*


    case class Medidas(petalLength: Float,petalWidth: Float,sepalLength: Float,sepalWidth: Float, feature:String)

    var df = spark.read.format("csv").option("delimiter", ",")
      .load("resources/iris-multiclass.csv").toDF()
    df.show()
    df = df.withColumnRenamed("_c0", "sepalLength")
    df = df.withColumnRenamed("_c1", "sepalWidth")
    df = df.withColumnRenamed("_c2", "petalLength")
    df = df.withColumnRenamed("_c3", "petalWidth")
    df = df.withColumnRenamed("_c4", "features")
    df.show()





    var onlyData = df.drop("features").cache()
    onlyData.show()
    var primero=onlyData.first()
    println(primero)
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(df)
      //.dropRight(1).map(_.toDouble))
       */
    /*
    val numClusters = 3
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    sc.stop()
    */
    */
  }

}