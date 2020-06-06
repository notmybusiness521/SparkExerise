package spark.base
import org.apache.spark.sql.Row
import spark.BaseSpark
import org.apache.spark.sql.types._
object RDDOperation extends BaseSpark {
  def main(args: Array[String]): Unit = {
    val log = this.log
    val spark = this.basicSpark
    import spark.implicits._
    //Creating Datasets
//    val caseClassDS = Seq(Person("andy", 32L)).toDS()
//    caseClassDS.show()
//
//    val primitiveDS = Seq(1, 2, 3).toDS()
//    primitiveDS.map(_ + 1).collect().foreach(println)
//
//    val path = "E:\\data\\sparktest\\json.txt"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()

//    val peopleTxt = "E:\\data\\examples\\src\\main\\resources\\people.txt"
//    val peopleDF = spark.sparkContext.textFile(peopleTxt)
//      .map(_.split(","))
//      .map(attribute => Person(attribute(0), attribute(1).trim.toLong))
//      .toDF()
//    peopleDF.createOrReplaceTempView("people")
//    val teenagersDF = spark.sql("select * from people where age between 13 and 19")
//    teenagersDF.map(teenager => "Name:"+teenager(0)).show()
//    teenagersDF.map(teenager => teenager.getAs[String]("name")).show()
//
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println)


//    val peopleRDD = spark.sparkContext.textFile(peopleTxt)
//    val schemaString = "name age"
//    val fields = schemaString.split(" ")
//      .map(fieldName =>StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//
//    val rowRDD = peopleRDD
//      .map(_.split(","))
//      .map(attr => Row(attr(0), attr(1).trim))
//
//    val peopleDF = spark.createDataFrame(rowRDD, schema)
//    peopleDF.createOrReplaceTempView("people")
//    val results = spark.sql("select name from people")
//
//    results.map(attr =>"Name: " + attr(0)).show()
    val input = spark.sparkContext.parallelize(List(1,2,3,4))//Array((1, 2), (2, 3), (3, 4), (4, 5))
//    val res = input.aggregate((0, 0))(
//      (acc, value) => (acc._1 + value, acc._2 + 1),
//      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//    val avg = res._1 / res._2.toDouble
//    println(avg)
//
//    val sums = input.map(x => (x, 1)).fold(0, 0)((x, y) =>(x._1 + y._1, x._2 + y._2))
//    println(sums._1 / sums._2.toDouble)
//    var s = Map("aaa" -> Map("smzdm" -> 2, "ylxb" -> 3, "znh" -> 1, "nhsc" -> 0, "fcwr" -> 1))
//    s += ("bbb"-> Map("smzdm" -> 2, "ylxb" -> 1, "znh" -> 0, "nhsc" -> 1, "fcwr" -> 4))
//    val t = s.get("aaa").get.values.toVector
//    val t1 = s.get("bbb").get.values.toVector
//    val member = t.zip(t1).foreach(println)
    //
//    val words = sc.parallelize(List("a", "b", "a", "c", "c", "c", "d", "d", "e"))
//    val counts = words.map(w => (w, 1)).reduceByKey((a, b) => (a + b))
//    val sort = counts.mapValues(x => x*2).sortByKey(true)//sortBy(- _._2)
//    sort.foreach(println)
    //mapPartitions
    val arr = spark.sparkContext.parallelize(Array(1,5,4,2,7,2,3,4,5,3,2,2,2))
//    val time1=System.currentTimeMillis()
//    val result = arr.mapPartitions(myIter)
//    val time2=System.currentTimeMillis()
//    println(time2 - time1)//result.collect().mkString(", "),
//    val time3=System.currentTimeMillis()
//    val result1 = arr.mapPartitions(ele => new myIter(ele))
//    val time4=System.currentTimeMillis()
//    println(time4 - time3)//result1.collect().mkString(", "),
      //取元素
//    arr.top(2).foreach(println)
//    arr.takeOrdered(2).foreach(println)
//    arr.countByValue().toList.sortBy(-_._2).foreach(println)//.foreach(println)
    //pairRDD operation
    val pairRdd = spark.sparkContext.parallelize(List((1,2),(3,4),(3,6)))
    pairRdd.countByKey().foreach(println(_))
    pairRdd.collectAsMap().foreach(println(_))
    pairRdd.lookup(3).foreach(println(_))

    //Ordere排序
    import scala.util.Sorting
    val pairs = Array(("a", 5, 2), ("c", 3, 1), ("a", 1, 3))
    // 单一字段从原类型到目标类型
    Sorting.quickSort(pairs)(Ordering.by[(String, Int, Int), Int](_._2))
    pairs.foreach(println)
    // 多个字段，只需要指定目标类型
    Sorting.quickSort(pairs)(Ordering[(String, Int)].on(x => (x._1, x._2)))
    pairs.foreach(println)
  }
  def myIter(iter:Iterator[Int]):Iterator[Int] = {
    var res = new scala.collection.mutable.ArrayBuffer[Int]()
    while (iter.hasNext) {
      val cur = iter.next()
      res.append(cur*3)
    }
    res.iterator
  }
}
case class Person(name:String, age:Long)
class myIter(iter:Iterator[Int]) extends Iterator[Int]{
  override def hasNext: Boolean = {iter.hasNext}
  override def next(): Int = {
    val cur = iter.next()
    cur * 3}
}