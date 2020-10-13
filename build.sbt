lazy val root = (project in file(".")).
  settings(
    name := "CovidDataProduct",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.ricardo.farias.App")
  )

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.5" % Provided
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}