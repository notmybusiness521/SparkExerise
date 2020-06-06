package spark.ml

import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession
object VectorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VectorDemo").master("local").getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, 1, 5),
      (2, 2, 0),
      (2, 3, 5),
      (1, 4, 0),
      (3, 5, 0)).toDF("a", "b", "c")

    val va = new VectorAssembler() //作用：将多个数值列按顺序汇总成一个向量列
      .setInputCols(Array("a", "b", "c"))
      .setOutputCol("features")

    val df1 = va.transform(df)

    val featureIndex = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures") //索引列，一般以0开始
      .setMaxCategories(3) // 设置为3表示本特征的取值超过3则当作连续特征不做处理
      .fit(df1)

    val df2 = featureIndex.transform(df1)

    df2.show(false)

  }
}
