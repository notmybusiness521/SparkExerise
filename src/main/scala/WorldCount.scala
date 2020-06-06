import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
object WorldCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WC")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("E://data//wc.txt")
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(x => - x._2).foreach(println)
  }
}
