import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazonaws.services.s3.{AmazonS3ClientBuilder}
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}
import scala.io.{BufferedSource, Source}
import scala.util.Try


object LocalFileSystem {

  val ROOT_DIRECTORY = Constants.directory

  def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val df = sparkSession.read.options(
      Map("dateFormat"->"MM/dd/yy",
        "columnNameOfCorruptRecord"->"Corrupted",
        "nullValues"->"NULL"))
      .schema(schema)
      .json(f"resources/${filename}.json")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

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
      .load(s"resources/${filename}.csv")

    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  def readSchemaFromJson(filename: String) : StructType = {
    val source: BufferedSource = Source.fromFile(s"resources/${filename}.json")
    val data = source.getLines.toList.mkString("\n")
    source.close()
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  def write(path: String, data : DataFrame) : Unit = ???
  def listObjects() : Unit = ???

}

object S3FileSystem{
  private val cred = new ProfileCredentialsProvider("profile sparkapp")
  private val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(cred).build()

  def read(path: String) : (DataFrame, DataFrame) = ???

  def write(path: String, data: DataFrame) : Unit = ???

  def listObjects() : Unit = {
    val result = s3Client.listObjectsV2("data-mesh-covid-us-domain")
    val objects = result.getObjectSummaries
    objects.forEach(println)
  }

}
