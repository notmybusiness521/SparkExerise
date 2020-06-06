package spark.base

import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupFavTeacher {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile(args(0))
    val TopN = args(1).toInt
    val subjectAndTeacher:RDD[((String,String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val host = line.substring(0, index)
      val subject = new URL(host).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    //将学科和老师联合当作key
    val reduced = subjectAndTeacher.reduceByKey(_ + _)
    //分组排序
    val grouped:RDD[(String, Iterable[((String,String), Int)])] = reduced.groupBy(_._1._1)
    //将每一个组拿出来进行操作
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(TopN))
    //收集结果
    val res = sorted.collect()
    println(res.toBuffer)



  }

}
