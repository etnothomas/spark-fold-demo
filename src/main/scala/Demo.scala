import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Demo extends App {

  val spark = SparkSession.builder.appName("demo").master("local").getOrCreate()

  val data = List(Row(1, 100, 101),
    Row(2, 120, 121),
    Row(3, 150, 151))

  val schema = StructType(List(
    StructField("count0", IntegerType, nullable = true),
    StructField("count1", IntegerType, nullable = true),
    StructField("count2", IntegerType, nullable = true)
  ))

  val rdd = spark.sparkContext.parallelize(data)

  val df = spark.createDataFrame(rdd, schema)

  // does not really work as intended
  val result1 = df.rdd.reduce {
    case (Row(prevcount0: Int, _, _), Row(count0: Int, count1: Int, count2: Int)) =>
      Row((prevcount0 * count1 + count2), count1, count2)
  }

  val newDf = df.union(spark.createDataFrame(spark.sparkContext.parallelize(List(result1)), schema))
  newDf.take(10).foreach(println)

  // collects to master and then transforms again
  @scala.annotation.tailrec
  def recFunc(dat: List[Row], prevResult: Int): Int = (dat, prevResult) match {
    case (Nil, prevResult) => prevResult
    case (rowList, previous) => recFunc(dat.tail, previous * rowList.head.getInt(1) + rowList.head.getInt(2))
  }

  val dat = df.collect().toList
  val result2 = recFunc(dat, 0)
  println(result2)
}
