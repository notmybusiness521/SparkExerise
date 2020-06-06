package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
class BaseSpark {
  val log = Logger.getLogger("org").setLevel(Level.ERROR)
  def basicSpark : SparkSession = {
    SparkSession
      .builder
      .config(getSparkConf)
      .getOrCreate()
    //.enableHiveSupport()...
  }
  val getSparkConf:SparkConf = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("Test")//conf.set("spark.rdd.compress", "true")....
  }


}
