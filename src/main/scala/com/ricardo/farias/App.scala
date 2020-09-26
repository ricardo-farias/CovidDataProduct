package com.ricardo.farias

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
      sparkSession.sparkContext.hadoopConfiguration.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
      sparkSession.sparkContext.hadoopConfiguration.set("hive.metastore.connect.retries", "5")
      sparkSession.sparkContext.hadoopConfiguration.set("hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
      S3FileSystem
    }

    fileStorage.listObjects()

    val schema : StructType = fileStorage.readSchemaFromJson("raw", "TestDataSchema.json")(sparkSession.sparkContext)
    println(schema)

    val csvResult = fileStorage.readCsv(schema, "raw","TestData.csv")(sparkSession)
    csvResult._1.show
    csvResult._2.show
    fileStorage.write("canonical","TestDataFromCsv", csvResult._1)

    val jsonResult = fileStorage.readJson(schema,"raw", "TestData.json")(sparkSession)
    jsonResult._1.show()
    jsonResult._2.foreach(row => println(row.get(3)))
    fileStorage.write("canonical","TestDataFromJson", jsonResult._1)

    val italyProvinceSchema = fileStorage.readSchemaFromJson("raw","covid-italy/covid19-italy-province-schema")(sparkSession.sparkContext)
    val covidItalyProvinceResults = fileStorage.readCsv(italyProvinceSchema, "raw", "covid-italy/covid19_italy_province.csv")(sparkSession)
    val covidItalyProvince = covidItalyProvinceResults._1
    covidItalyProvince.show()
    val corruptCovidItalyProvince = covidItalyProvinceResults._2
    corruptCovidItalyProvince.show()
    fileStorage.write("canonical","covid-italy/covid19_italy_province", covidItalyProvince)
    fileStorage.write("error","covid-italy/covid19_italy_province_err", corruptCovidItalyProvince)

    val italyRegionSchema = fileStorage.readSchemaFromJson("raw","covid-italy/covid19-italy-region-schema.json")(sparkSession.sparkContext)
    val covidItalyRegionResults = fileStorage.readCsv(italyRegionSchema,"raw", "covid-italy/covid19_italy_region.csv")(sparkSession)
    val covidItalyRegion = covidItalyRegionResults._1
    covidItalyRegion.show()
    val corruptCovidItalyRegion = covidItalyRegionResults._2
    corruptCovidItalyRegion.show()
    fileStorage.write("canonical","covid-italy/covid19_italy_region", covidItalyRegion)
    fileStorage.write("error","covid-italy/covid19_italy_region_err", corruptCovidItalyRegion)

    val usSchema = fileStorage.readSchemaFromJson("raw","covid-us/us-schema.json")(sparkSession.sparkContext)
    val covidUsResults = fileStorage.readCsv(usSchema, "raw","covid-us/us.csv")(sparkSession)
    val covidUs = covidUsResults._1
    covidUs.show()
    val corruptCovidUS = covidUsResults._2
    corruptCovidUS.show()
    fileStorage.write("canonical", "covid-us/covid_us", covidUs)(sparkSession)
    fileStorage.write("error", "covid-us/covid_us_err", covidUs)(sparkSession)

    val usCountriesSchema = fileStorage.readSchemaFromJson("raw","covid-us/us-counties-schema.json")(sparkSession.sparkContext)
    val covidUsCountriesResults = fileStorage.readCsv(usCountriesSchema, "raw","covid-us/us-counties.csv")(sparkSession)
    val covidUsCountries = covidUsCountriesResults._1
    covidUsCountries.show()
    val corruptCovidUsCountries = covidUsCountriesResults._2
    corruptCovidUsCountries.show()
    fileStorage.write("canonical","covid-us/covid_us_counties", covidUsCountries)(sparkSession)
    fileStorage.write("error","covid-us/covid_us_counties_err", corruptCovidUsCountries)(sparkSession)

    val usStatesSchema = fileStorage.readSchemaFromJson("raw","covid-us/us-states-schema.json")(sparkSession.sparkContext)
    val covidUsStatesResults = fileStorage.readCsv(usStatesSchema, "raw", "covid-us/us-states.csv")(sparkSession)
    val covidUsStates = covidUsStatesResults._1
    covidUsStates.show()
    val corruptCovidUsStates = covidUsStatesResults._2
    corruptCovidUsStates.show()
    fileStorage.write("canonical","covid-us/covid_us_states", covidUsStates)(sparkSession)
    fileStorage.write("error","covid-us/covid_us_states", corruptCovidUsStates)(sparkSession)
  }
}
