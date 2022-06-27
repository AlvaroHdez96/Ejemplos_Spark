package ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object Ejemplo10BrezeViz {


  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Ejemplo06Iris", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()


    var df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("resources/iris-multiclass.csv")


    import breeze.linalg._
    import breeze.plot._

    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    p += plot(x, x ^:^ 2.0)
    p += plot(x, x ^:^ 3.0, '.')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    f.saveas("salidas/lines.png") // save current figure as a .png, eps and pdf also supported

    /*
    df = df.withColumnRenamed("_c0", "sepalLength")
    df = df.withColumnRenamed("_c1", "sepalWidth")
    df = df.withColumnRenamed("_c2", "petalLength")
    df = df.withColumnRenamed("_c3", "petalWidth")
    df = df.withColumnRenamed("_c4", "labels")

    // identify the feature colunms

    */
    df.printSchema()

    /*
    val inputColumns = Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")
    val assembler = new VectorAssembler().setInputCols(inputColumns).setOutputCol("features")

    val featureSet = assembler.transform(df)

    // split data random in trainingset (70%) and testset (30%)
    val seed = 5043
    val trainingAndTestSet = featureSet.randomSplit(Array[Double](0.7, 0.3), seed)
    val trainingSet = trainingAndTestSet(0)
    val testSet = trainingAndTestSet(1)

    // train the algorithm based on a Random Forest Classification Algorithm with default values// train the algorithm based on a Random Forest Classification Algorithm with default values

    val randomForestClassifier = new RandomForestClassifier().setSeed(seed)
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
    */
    /*
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
    */
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
    * */
  }

}