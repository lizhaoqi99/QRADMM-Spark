// https://stackoverflow.com/questions/41372978/unknown-artifact-not-resolved-or-indexed-error-for-scalatest
// sbt 1.3.0+ uses Coursier to implement dependency management. Before that, Apache Ivy is used.
// Coursier does a good job of keeping the compatibility, but some of the feature might be specific to Apache Ivy. In those cases, you can use the following setting to switch back to Ivy:
ThisBuild / useCoursier := false

name := "QRADMM_Spark"

version := "0.1"

// scala, spark package dependency:
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze-viz" % "1.2",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze" % "1.2",

  // Import Spark modules
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)