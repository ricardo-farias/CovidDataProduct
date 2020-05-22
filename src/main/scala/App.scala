import org.apache.spark._
import org.apache.spark.sql._

object App {

  def main(args : Array[String]): Unit ={
    val config = new SparkConf().setMaster("local").setAppName("ExcelParser")
    val sparkContext = new SparkContext(config)



  }

}
