package ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object Ejemplo06LinealRegressionV2 {
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


    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "resources/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println(s"Area under ROC = $auROC")

    // Save and load model
    /*
    model.save(sc, "salidas/scalaSVMWithSGDModel2")
    val sameModel = SVMModel.load(sc, "salidas/scalaSVMWithSGDModel2")
    */
    Thread.sleep(200000)
  }

}

