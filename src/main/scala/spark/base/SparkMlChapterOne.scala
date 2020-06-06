package spark.base

import spark.BaseSpark

object SparkMlChapterOne extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val sc = this.basicSpark.sparkContext
    //文件包含了用户，机型，价格
    val filePath = "E:\\data\\UserPurchaseHistory.csv"
    val data = sc.textFile(filePath).map(
      ele =>{
        val line = ele.split(",")
        (line(0), line(1), line(2))
      })
    data.cache()
    //1求购买总次数
    val totalPurchaseNums = data.count()
    //2客户总个数
    val totalUserNums = data.map{ case(user, product, price)=> user}.distinct().count()
    //3总收入
    val totalIncome = data.map{case(user, product, price)=> price.toDouble}.sum()
    //4最畅销的产品
    val mostPopularProduct = data.map{case(user, product, price)=> (product, 1)}
      .reduceByKey(_+_)
      .collect()
      .sortBy(-_._2)
      .take(1)(0)._1
    //输出结果
    println("1购买总次数:" + totalPurchaseNums)
    println("2客户总个数:" + totalUserNums)
    println("3总收入:" + totalIncome)
    println("4最畅销的产品:" + mostPopularProduct)


  }

}
