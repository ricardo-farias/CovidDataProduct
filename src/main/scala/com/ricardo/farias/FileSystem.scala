package com.ricardo.farias

import java.io.File

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3Object
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.{BufferedSource, Source}
import scala.util.Try

abstract class FileSystem {
  def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame)
  def readCsv(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame)
  def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType
  def write(filename: String, data: DataFrame)(implicit sparkSession: SparkSession) : Unit
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
      .json(f"${ROOT_DIRECTORY}/${filename}")
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
      .load(s"${ROOT_DIRECTORY}/${filename}")

    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  override def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType = {
    val source: BufferedSource = Source.fromFile(s"${ROOT_DIRECTORY}/${filename}")
    val data = source.getLines.toList.mkString("\n")
    source.close()
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  def setRootDirectory(directory: String) : Unit = ROOT_DIRECTORY = directory

  override def write(filename: String, data : DataFrame)(implicit sparkSession: SparkSession)  : Unit = {
    data.write.mode(SaveMode.Overwrite).parquet(f"${ROOT_DIRECTORY}/${filename}.parquet")
  }

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
          "timestampFormat"->"MM/dd/yyyy hh:mm:ss a",
          "nullValue"-> "NULL",
          "ignoreTrailingWhiteSpace"->"true",
          "ignoreLeadingWhiteSpace"->"true",
          "columnNameOfCorruptRecord"->"Corrupted"
        ))
      .schema(schema)
      .load(s"s3a://${file.getBucketName}/${file.getKey}")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  override def readJson(schema: StructType, filename: String)(implicit sparkSession: SparkSession) : (DataFrame, DataFrame) = {
    val file : S3Object = getObject(filename)
    val df = sparkSession.read.options(
      Map("dateFormat"->"MM/dd/yy",
        "timestampFormat"->"MM/dd/yyyy hh:mm:ss a",
        "columnNameOfCorruptRecord"->"Corrupted",
        "nullValues"->"NULL"))
      .schema(schema)
      .json(f"s3a://${file.getBucketName}/${file.getKey}")
    val badDF = df.filter(df.col("Corrupted").isNotNull).toDF
    val goodDF = df.filter(df.col("Corrupted").isNull).toDF
    (goodDF, badDF)
  }

  override def readSchemaFromJson(filename: String)(implicit sparkContext: SparkContext) : StructType = {
    val file : S3Object = getObject(filename)
    val content = file.getObjectContent
    println(content)
    println(file.getKey)
    val source = sparkContext.textFile(f"s3a://${file.getBucketName}/${file.getKey}").collect()

    val data = source.toList.mkString("\n")
    val schema  = Try(DataType.fromJson(data)).getOrElse(LegacyTypeStringParser.parse(data)) match {
      case t: StructType => t
      case _             => throw new RuntimeException(s"Failed parsing StructType: $data")
    }
    schema
  }

  override def write(filename: String, data: DataFrame)(implicit sparkSession: SparkSession) : Unit = {
    val name = if (filename.contains("/")) filename.split("/")(1) else filename
    data.createOrReplaceTempView(name)
    sparkSession.sql(s"""CREATE TABLE default.${name}
      USING PARQUET
      OPTIONS ('compression'='snappy')
      LOCATION 's3://${bucket}/${filename}'
      SELECT * FROM ${name}""")
    data.write.mode(SaveMode.Overwrite).parquet(f"s3a://${bucket}/${filename}.parquet")
  }

  private def getObject(filename: String) : S3Object = {
    val result = s3Client.getObject(bucket, filename)
    result
  }

  override def listObjects() : Unit = {
    val result = s3Client.listObjectsV2(bucket)
    val objects = result.getObjectSummaries.toArray
    objects.foreach(println)
  }

}
