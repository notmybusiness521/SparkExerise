package spark.ml

import spark.BaseSpark
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import scala.io.Source
object tfIdf extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val log = this.log
    val spark = this.basicSpark
    val sc = spark.sparkContext
    """
      |//创建数据
      |    val sentenceData = spark.createDataFrame(Seq(
      |      (0.0, "Hi I heard about Spark"),
      |      (0.0, "I wish Java could use case classes"),
      |      (1.0, "Logistic regression models are neat")
      |    )).toDF("label", "sentence")
      |    //切分调用transform方法
      |    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      |    val wordsData = tokenizer.transform(sentenceData)
      |    //散列
      |    val hashingTF = new HashingTF()
      |      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
      |    val featurizedData = hashingTF.transform(wordsData)
      |    //逆文档频率
      |    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
      |    val idfModel = idf.fit(featurizedData)
      |
      |    val rescaledData = idfModel.transform(featurizedData)
      |    rescaledData.select("label", "features").show()
    """.stripMargin
    //利用TF-IDF求相似度
    val filePath = "E:\\data\\20news-bydate\\20news-bydate-train\\alt.atheism\\51060"
    val text = sc.parallelize(Source.fromFile(filePath).getLines().filter(_.trim.length>0).toSeq)
      .map(_.toLowerCase.split("\\W+").toSeq)
      .filter(_.length > 0)
      .zipWithIndex()
//    text.foreach(println)
    //特征散列
    val hashingTF = new HashingTF(Math.pow(2, 10).toInt)
    val hashingTFPairs = text.map{
      case (line, idx) => {
        val tf = hashingTF.transform(line)
        (idx, tf)
      }
    }
    //IDF模型
    val idfModel = new IDF().fit(hashingTFPairs.values)
    //转换逆文档频率
    val idfParis = hashingTFPairs.mapValues(value => idfModel.transform(value))
    //广播一份tf-idf向量集
    val idfParisBroadCast = sc.broadcast(idfParis.collect())
    //计算相似度
    import breeze.linalg._
    val cosineSim = idfParis.flatMap{
      case (idx1, idf1) => {
        val sv1 = idf1.asInstanceOf[SV]
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        val idfs = idfParisBroadCast.value.filter(_._1 != idx1)
        idfs.map{
          case (idx2, idf2) => {
            val sv2 = idf2.asInstanceOf[SV]
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
//            val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
//            (idx1, idx2, cosSim)
            if (idx1 < idx2) {
              val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
              (idx1, idx2, cosSim)
            }else{null}
          }
        }
      }
    }.filter(_!=null)
//    cosineSim.foreach(println)
    println(cosineSim.count())
  }
}
