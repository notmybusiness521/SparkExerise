package spark.ml
import breeze.linalg.DenseVector
import breeze.linalg.SparseVector
import org.apache.spark.mllib.linalg.Matrices
import breeze.math.Complex
object LinearMathDemo {
  def main(args: Array[String]): Unit = {
//    val i = Complex.i
//    println((1 + 2 * i) + (2 + 3 * i))
//    println((1 + 2 * i) - (2 + 3 * i))
//    println((5 + 10 * i) / (3 - 4 * i))
//    println((1 + 2 * i) * (-3 + 6 * i))
//    println(-(1 + 2 * i))
//    val l = List((1 + 2 * i),(5 + 10 * i),(2 + 3 * i))
//    println(l.sum)
//    println(l.product)
//    println(l.sorted)
    val dMatrix = Matrices.dense(2, 2, Array(1,2,3,4))
    println(dMatrix)

    val sMatrix1 = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(5, 6, 7))
    println(sMatrix1)
    val sMatrix2 = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 1, 2), Array(5, 6, 7))
    println(sMatrix2)



  }
}
