package spark.ml
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.sql.functions.col
import spark.BaseSpark
object LSF extends BaseSpark{
  def main(args: Array[String]): Unit = {
    val spark = this.basicSpark
//    val dfA = spark.createDataFrame(Seq(
//      (0, Vectors.dense(1.0, 1.0)),
//      (1, Vectors.dense(1.0, -1.0)),
//      (2, Vectors.dense(-1.0, -1.0)),
//      (3, Vectors.dense(-1.0, 1.0))
//    )).toDF("id", "features")
//
//    val dfB = spark.createDataFrame(Seq(
//      (4, Vectors.dense(1.0, 0.0)),
//      (5, Vectors.dense(-1.0, 0.0)),
//      (6, Vectors.dense(0.0, 1.0)),
//      (7, Vectors.dense(0.0, -1.0))
//    )).toDF("id", "features")
//
//    val key = Vectors.dense(1.0, 0.0)
//
//    val brp = new BucketedRandomProjectionLSH()
//      .setBucketLength(2.0)
//      .setNumHashTables(3)
//      .setInputCol("features")
//      .setOutputCol("hashes")
//
//    val model = brp.fit(dfA)
//
//    // Feature Transformation
//    println("The hashed dataset where hashed values are stored in the column 'hashes':")
//    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 1.5)`
//    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
//    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
//      .select(col("datasetA.id").alias("idA"),
//        col("datasetB.id").alias("idB"),
//        col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
//    println("Approximately searching dfA for 2 nearest neighbors of the key:")
//    model.approxNearestNeighbors(dfA, key, 2).show()
    ////////////////////////////////////////////////////////////////////////////////////////
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
    println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")
    model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("JaccardDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    // It may return less than 2 rows when not enough approximate near-neighbor candidates are
    // found.
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
  }
}
