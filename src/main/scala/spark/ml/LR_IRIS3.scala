package spark.ml
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import spark.BaseSpark

object LR_IRIS3 extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val log = this.log
    val sc = this.basicSpark.sparkContext
    val data = MLUtils.loadLibSVMFile(sc, "C:\\Users\\asus\\Desktop\\Desk\\iris_calss3.txt")

    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), 123L)
    training.foreach(println)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(3).run(training)
    //方式1
//    val prictionAndLabel = test.map(point => {
//      val priction = model.predict(point.features)
//      (priction, point.label)
//    })
    //方式1
    val prictionAndLabel = test.map{case LabeledPoint(label, features) =>
      val priction = model.predict(features)
      (priction, label)
    }
    val metrics = new MulticlassMetrics(prictionAndLabel)
    println(s"precision: ${metrics.precision}, \nintercept: ${model.intercept}, \nweights: ${model.weights}")

  }

}
