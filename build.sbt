ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Ejemplos_Spark"
  )

val sparkVersion = "3.3.0"
val vegasVersion = "0.3.9"
val bokehVersion = "1.0.4"




// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
// libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"


//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.scalanlp" %% "breeze" % "1.1"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.1"
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
