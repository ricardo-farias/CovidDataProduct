import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object App {

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster("local").setAppName("Practice")
    val sc = new SparkContext(config)
    val sql = SparkSession.builder().appName("Practice").master("local").getOrCreate()

    val schema = StructType(List(
      StructField("City",StringType, false),
      StructField("Population", IntegerType, false),
      StructField("Date Collected", DateType, true),
      StructField("_corrupt_record", StringType, true)
    ))

    val df = sql.read.format("csv")
        .options(
          Map(
            "header"-> "true",
            "dateFormat"-> "MM/dd/yyyy",
            "nullValue"-> "NULL",
            "ignoreTrailingWhiteSpace"->"true",
            "ignoreLeadingWhiteSpace"->"true"
          ))
        .schema(schema)
        .load("resource/TestData.csv")
    df.printSchema()


    // Corrupted values can be found here
    val badRows = df.filter(df.col("_corrupt_record").isNotNull)
    badRows.collect().foreach(row => println(row))





  }

}
