import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType}

object App {

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster(Constants.master).setAppName(Constants.appName)
    val sc = new SparkContext(config)
    val sql = SparkSession.builder().appName(Constants.appName).master(Constants.master).getOrCreate()

    if (Constants.env == "dev"){
      val schema : StructType = LocalFileSystem.readSchemaFromJson("TestDataSchema")

      val csvResult = LocalFileSystem.readCsv(schema, "TestData")(sql)
      csvResult._1.show
      csvResult._2.show

      val jsonResult = LocalFileSystem.readJson(schema, "TestData")(sql)
      jsonResult._1.show()
      jsonResult._2.foreach(row => println(row.get(3)))
    }else{
      S3FileSystem.listObjects()
    }


  }

}
