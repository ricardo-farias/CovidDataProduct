import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType, StructField, StructType}

import scala.io.{BufferedSource, Source}
import scala.util.Try

object App {

  def goodAndBad(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
    val df = sparkSession.read.format("csv")
      .options(
        Map(
          "header"-> "true",
          "dateFormat"-> "MM/dd/yyyy",
          "nullValue"-> "NULL",
          "ignoreTrailingWhiteSpace"->"true",
          "ignoreLeadingWhiteSpace"->"true",
          "columnNameOfCorruptRecord"->"corrupted"
        ))
      .schema(schema)
      .load(s"resource/${filename}.csv")

    val badDF = df.filter(df.col("corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("corrupted").isNull).toDF
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

    val result = goodAndBad(schema, "TestData")(sql)
    result._1.show
    result._2.show

  }

}
