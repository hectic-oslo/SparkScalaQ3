import scala.collection.Seq
// build.sbt

// Project Name

name := "SparkAssignmentQues2a"

version := "0.1"

scalaVersion := "2.12.10" // Ensure compatibility with your Spark version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)
dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "3.3.4"

// Disable the security manager or set it to null
javaOptions in run += "-Djava.security.manager=null"
