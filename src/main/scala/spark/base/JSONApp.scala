package spark.base

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON
import org.apache.log4j.{Level, Logger}
object JSONApp {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JsonDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val jsonStrs = sc.textFile("E:\\data\\sparktest\\json.txt")
    val res = jsonStrs.map(s => JSON.parseFull(s))
    res.foreach({
      r => r match {
        case Some(map:Map[String, Any]) => println(map)
        case None => println("Parsing faild")
        case other => println("Unknow data structer!")
      }
    })

  }

}
