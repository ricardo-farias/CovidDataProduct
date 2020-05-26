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
      StructField("Date Collected", DateType, true)
    ))

    val df = sql.read.format("csv")
        .options(
          Map(
            "header"-> "true",
            "dateFormat"-> "dd-M-yy",
            "nullValue"-> "NULL",
            "ignoreTrailingWhiteSpace"->"true",
            "ignoreLeadingWhiteSpace"->"true"
          ))
        .schema(schema)
        .load("resource/TestData.csv")
    df.printSchema()
    df.show()





  }

}
