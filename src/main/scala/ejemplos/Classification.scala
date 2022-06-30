package ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.google.common.collect.ImmutableMap


object Classification {

  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Classification", System.getenv("SPARK_HOME"))
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
      .load("resources/exoplanets.csv")

    val df1 = df.withColumn("label",col("koi_disposition").cast("int"))

    val df2 = df1.withColumn("x1",col("koi_duration").cast("float"))
      .withColumn("x2",col("koi_depth").cast("float"))
      .withColumn("x3",col("koi_model_snr").cast("float"))

    val df3 = df2.drop("loc_rowid","koi_disposition","koi_duration","koi_depth","koi_model_snr").na.drop("any")
    df3.show()

    val inputColumns = Array("x1","x2","x3")
    val assembler = new VectorAssembler().setInputCols(inputColumns).setOutputCol("features")

    val featureSet = assembler.transform(df3)

    // split data random in trainingset (70%) and testset (30%)

    val seed = 42
    val trainingAndTestSet = featureSet.randomSplit(Array[Double](0.6, 0.4), seed)
    val trainingSet = trainingAndTestSet(0)
    val testSet = trainingAndTestSet(1)

    // train the algorithm based on a Random Forest Classification Algorithm with default values

    val randomForestClassifier = new RandomForestClassifier().setSeed(seed)
    //randomForestClassifier.setMaxDepth(4)
    val model = randomForestClassifier.fit(trainingSet)
    // test the model against the test set       
    val predictions = model.transform(testSet)

    // evaluate the model
    val evaluator = new MulticlassClassificationEvaluator()

    System.out.println("accuracy: " + evaluator.evaluate(predictions))
    /*
    val dataMapped=data.map( _.toDouble )
    //Dividiendo datos en training y test
    val Array(training, test) = data.randomSplit(Array[Double](0.7, 0.3), 18)
    training.show()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)
    // Print the coefficients and intercept for linear regression
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


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

  }



}
