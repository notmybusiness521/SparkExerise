package spark.base
import spark.BaseSpark
object LogDataProcess extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val log = this.log
    val sc = this.basicSpark.sparkContext
    val logdata = sc.textFile("C:\\Users\\asus\\Desktop\\Desk\\log.txt")

    logdata.map(line =>{
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic=splits(9)
      (DateUDF.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip)
    }).saveAsTextFile("C:\\Users\\asus\\Desktop\\Desk\\logsave.txt")
    sc.stop()
  }

}

