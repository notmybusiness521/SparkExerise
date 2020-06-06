package spark.ml.chapter04
import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._//{StringType,StructType,StructField, IntegerType}
object DataExploreDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DataExpore")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val path = "E:\\data\\ml-100k\\u.user"
    val customSchema = StructType(Array(
      StructField("no", IntegerType, true),
      StructField("age", StringType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("zipCode", StringType, true)
    ))

    val userDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter","|")
      .schema(customSchema)
      .load(path)
    val head5 = userDf.collect().take(5)
    head5.foreach(println)
  }
}
