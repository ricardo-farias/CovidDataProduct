import java.io.File

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{S3Object, S3ObjectSummary}
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.{BufferedSource, Source}
import scala.util.Try

abstract class FileSystem {
  def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame)
  def readCsv(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame)
  def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType
  def write(path: String, data: DataFrame) : Unit
  def listObjects() : Unit
}

object LocalFileSystem extends FileSystem {

  var ROOT_DIRECTORY = Constants.directory

  override def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession): (DataFrame, DataFrame) = {
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

  override def readCsv(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
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

  override def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType = {
    val source: BufferedSource = Source.fromFile(s"resources/${filename}.json")
    val data = source.getLines.toList.mkString("\n")
    source.close()
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  def setRootDirectory(directory: String) : Unit = ROOT_DIRECTORY = directory

  override def write(path: String, data : DataFrame) : Unit = ???

  override def listObjects() : Unit = {
    val dir = new File(ROOT_DIRECTORY)
    dir.listFiles().foreach(println)
  }

}

object S3FileSystem extends FileSystem {
  private val cred = new ProfileCredentialsProvider("profile sparkapp")
  private val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(cred).build()
  private val bucket = Constants.bucket

  override def readCsv(schema : StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
    val file : S3Object = getObject(filename)
    println(s"s3a://${file.getBucketName}/${file.getKey}")
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
      .load(s"s3a://${Constants.accessKey}:${Constants.secretKey}@${file.getBucketName}/${file.getKey}")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  override def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
    val file : S3Object = getObject(filename)
    val df = sparkSession.read.options(
      Map("dateFormat"->"MM/dd/yy",
        "columnNameOfCorruptRecord"->"Corrupted",
        "nullValues"->"NULL"))
      .schema(schema)
      .json(f"s3://${file.getBucketName}/${file.getKey}")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  override def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType = {
    val file : S3Object = getObject(filename)
    val content = file.getObjectContent
    println(content)
    println(file.getKey)
    val source = sparkContext.textFile(f"s3a://${Constants.accessKey}:${Constants.secretKey}@${file.getBucketName}/${file.getKey}").collect()

    val data = source.toList.mkString("\n")
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  override def write(path: String, data: DataFrame) : Unit = ???

  private def getObject(filename: String) : S3Object = {
    val result = s3Client.getObject(bucket, filename)
    result
  }

  override def listObjects() : Unit = {
    val result = s3Client.listObjectsV2(bucket)
    val objects = result.getObjectSummaries
    objects.forEach(println)
  }

}
