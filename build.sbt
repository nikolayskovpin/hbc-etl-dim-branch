organization := "com.hbc"

name := "hbc-etl-dim-branch"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
  "com.databricks" %% "spark-avro" % "4.0.0" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.1" % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.11.179" % "provided"
  ,"com.asterdata.ncluster" % "ncluster-jdbc-driver" % "6.10.0"
)

