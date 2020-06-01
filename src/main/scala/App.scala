import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType, StructField, StructType}

import scala.io.{BufferedSource, Source}
import scala.util.Try

object App {

  def readCsv(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
    val df = sparkSession.read.format("csv")
      .options(
        Map(
          "header"-> "true",
          "dateFormat"-> "MM/dd/yyyy",
          "nullValue"-> "NULL",
          "ignoreTrailingWhiteSpace"->"true",
          "ignoreLeadingWhiteSpace"->"true",
          "columnNameOfCorruptRecord"->"Corrupted"
        ))
      .schema(schema)
      .load(s"resource/${filename}.csv")

    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val df = sparkSession.read.options(
      Map("dateFormat"->"MM/dd/yy",
        "columnNameOfCorruptRecord"->"Corrupted",
        "nullValues"->"NULL",
        "multiline"->"true"))
      .schema(schema)
      .json(f"resource/${filename}.json")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  def readSchemaFromJson() : StructType = {
    val source: BufferedSource = Source.fromFile("resource/TestDataSchema.json")
    val data = source.getLines.toList.mkString("\n")
    source.close()
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster("local").setAppName("Practice")
    val sc = new SparkContext(config)
    val sql = SparkSession.builder().appName("Practice").master("local").getOrCreate()

    val schema : StructType = readSchemaFromJson()

    val csvResult = readCsv(schema, "TestData")(sql)
    csvResult._1.show
    csvResult._2.show

    val jsonResult = readJson(schema, "TestData")(sql)
    jsonResult._1.show()
    jsonResult._2.foreach(row => println(row.get(3)))

  }

}
