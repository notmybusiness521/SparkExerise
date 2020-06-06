package spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
object FavTeacher {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile(args(0))
    val teacherAndOne = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      (teacher, 1)
    })

    val reduced = teacherAndOne.reduceByKey(_ + _)
    val sorted = reduced.sortBy(_._2, false)
    val res:Array[(String, Int)] = sorted.collect()
    println(res.toBuffer)
    sc.stop()

  }

}
