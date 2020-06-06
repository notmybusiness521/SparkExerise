package spark.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level,Logger}
object DataFrameDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("CreateDataFrame")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.json("E:\\data\\sparktest\\json.txt")
//    df.show()
//    df.printSchema()
//    df.select("name", "age").show()
    df.filter(df("age") isNotNull).groupBy("age").count().show()
  }
}
