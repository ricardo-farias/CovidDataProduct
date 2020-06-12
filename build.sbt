//name := "SparkPractice"
//
//version := "0.1"
//
//scalaVersion := "2.11.12"


lazy val root = (project in file(".")).
  settings(
    name := "SparkPractice",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.ricardo.farias.App")
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.515",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.5" % Provided)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}