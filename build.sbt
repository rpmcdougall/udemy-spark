name := "udemy-spark"

version := "0.1"

scalaVersion := "2.11.8"

organization := "com.phatlabs.ninja"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided"
)