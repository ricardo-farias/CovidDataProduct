import org.apache.spark._
import org.apache.spark.sql._

object App {

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster("local").setAppName("Practice")
    val sc = new SparkContext(config)
    val sql = SparkSession.builder().appName("Practice").master("local").getOrCreate()


    val df = sql.read.format("csv")
        .options(Map("inferSchema"->"true", "header"-> "true", "dateFormat"->"DD-MM-YY"))
        .load("resource/TestData.csv")
    df.show()




  }

}
