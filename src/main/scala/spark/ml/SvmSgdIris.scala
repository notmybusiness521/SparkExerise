package spark.ml
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
object SvmSgdIris {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SVMwithSGD")
    val sc = new SparkContext(conf)
    //加载数据
    val data = MLUtils.loadLibSVMFile(sc, "C:\\Users\\asus\\Desktop\\Desk\\iris.txt")
    //分割训练集和测试集
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1L)
    val trainData = splits(0).cache()
    val testData = splits(1)
    //训练模型
    val numIterators = 100
    val model = SVMWithSGD.train(trainData, 100)
//    model.clearThreshold()
    //预测得分
    val scoreAndLabel = testData.map(point =>{
      val score = model.predict(point.features)
      (score, point.label)
    })
    val metrics = new BinaryClassificationMetrics(scoreAndLabel)
    val auRoc = metrics.areaUnderROC()
    println(auRoc, model.intercept,model.weights)
    scoreAndLabel.foreach(println)



  }

}
