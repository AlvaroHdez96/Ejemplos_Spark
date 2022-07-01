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

//Explicaciçón del proceso en resources/Classification.txt

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




  }



}
