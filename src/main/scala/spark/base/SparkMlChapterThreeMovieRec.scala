package spark.base

import spark.BaseSpark
//"""
//Spark机器学习第三章电影推荐
//"""
object SparkMlChapterThreeMovieRec extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val sc = this.basicSpark.sparkContext
    //数据探索
    //1探索用户数据
    val userFilePath = "E:\\data\\ml-100k\\u.user"
    val user_data = sc.textFile(userFilePath)
    println(user_data.first()) //1|24|M|technician|85711
    //统计用户,性别，职业和邮编的数目
    val user_fields = user_data.map(line => line.split("\\|"))
    user_fields.cache()
    val userNums = user_fields.map(e => e(0)).count()
    val genderNums = user_fields.map(e => e(2)).distinct().count()
    val occupationNums = user_fields.map(e => e(3)).distinct().count()
    val zipcodeNums = user_fields.map(e => e(4)).distinct().count()
    println(s"总用户数是：$userNums，性别种类数：$genderNums，职业类别数：$occupationNums，邮编类别数：$zipcodeNums")
    user_fields.unpersist()
    sc.stop()
  }
}
