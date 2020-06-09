name := "SparkPractice"

version := "0.1"

scalaVersion := "2.12.10"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.515"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"