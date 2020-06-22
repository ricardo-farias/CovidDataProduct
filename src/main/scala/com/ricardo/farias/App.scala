package com.ricardo.farias

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object App {

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster(Constants.master).setAppName(Constants.appName)
    implicit val sparkSession = if (Constants.env == "dev") {
      SparkSession.builder().master(Constants.master).config(config).getOrCreate()
    }else {
      SparkSession.builder().master(Constants.master)
        .config("hive.metastore.connect.retries", 5)
        .config("hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport().getOrCreate()
    }

    val fileStorage : FileSystem = if (Constants.env == "dev") LocalFileSystem
    else {
      val envAuth = new ProfileCredentialsProvider("profile sparkapp")
      sparkSession.sparkContext.hadoopConfiguration.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", envAuth.getCredentials.getAWSAccessKeyId)
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", envAuth.getCredentials.getAWSSecretKey)
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper,com.amazonaws.auth.profile.ProfileCredentialsProvider")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
      sparkSession.sparkContext.hadoopConfiguration.set("hive.metastore.connect.retries", "5")
      sparkSession.sparkContext.hadoopConfiguration.set("hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
      S3FileSystem
    }

    fileStorage.listObjects()

    val schema : StructType = fileStorage.readSchemaFromJson("TestDataSchema.json")(sparkSession.sparkContext)
    println(schema)

    val csvResult = fileStorage.readCsv(schema, "TestData.csv")(sparkSession)
    csvResult._1.show
    csvResult._2.show
    fileStorage.write("TestDataFromCsv", csvResult._1)

    val jsonResult = fileStorage.readJson(schema, "TestData.json")(sparkSession)
    jsonResult._1.show()
    jsonResult._2.foreach(row => println(row.get(3)))
    fileStorage.write("TestDataFromJson", jsonResult._1)

    val italyProvinceSchema = fileStorage.readSchemaFromJson("covid-italy/covid19-italy-province-schema")(sparkSession.sparkContext)
    val covidItalyProvince = fileStorage.readCsv(italyProvinceSchema, "covid-italy/covid19_italy_province.csv")(sparkSession)
    covidItalyProvince._1.show()
    covidItalyProvince._2.foreach(row => println(row.get(3)))
    fileStorage.write("covid-italy/covid19_italy_province", covidItalyProvince._1)

    val italyRegionSchema = fileStorage.readSchemaFromJson("covid-italy/covid19-italy-region-schema.json")(sparkSession.sparkContext)
    val covidItalyRegion = fileStorage.readCsv(italyRegionSchema, "covid-italy/covid19_italy_region.csv")(sparkSession)
    covidItalyRegion._1.show()
    covidItalyRegion._2.foreach(row => println(row.get(3)))
    fileStorage.write("covid-italy/covid19_italy_region", covidItalyRegion._1)

    val usSchema = fileStorage.readSchemaFromJson("covid-us/us-schema.json")(sparkSession.sparkContext)
    val covidUs = fileStorage.readCsv(usSchema, "covid-us/us.csv")(sparkSession)
    covidUs._1.show()
    covidUs._2.foreach(row => println(row.get(3)))
    fileStorage.write("covid-us/covid_us", covidUs._1)(sparkSession)

    val usCountriesSchema = fileStorage.readSchemaFromJson("covid-us/us-counties-schema.json")(sparkSession.sparkContext)
    val covidUsCountries = fileStorage.readCsv(usCountriesSchema, "covid-us/us-counties.csv")(sparkSession)
    covidUsCountries._1.show()
    covidUsCountries._2.foreach(row => println(row.get(3)))
    fileStorage.write("covid-us/covid_us_counties", covidUsCountries._1)(sparkSession)

    val usStatesSchema = fileStorage.readSchemaFromJson("covid-us/us-states-schema.json")(sparkSession.sparkContext)
    val covidUsStates = fileStorage.readCsv(usStatesSchema, "covid-us/us-states.csv")(sparkSession)
    covidUsStates._1.show()
    covidUsStates._2.foreach(row => println(row.get(3)))
    fileStorage.write("covid-us/covid_us_states", covidUsStates._1)(sparkSession)





  }

}
