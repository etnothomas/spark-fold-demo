import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Demo extends App {

  val spark = SparkSession.builder.appName("demo").master("local").getOrCreate()

  val data = List(Row(99, 100, 101),
    Row(119, 120, 121),
    Row(149, 150, 151))

  val schema = StructType(List(
    StructField("count0", IntegerType, nullable = true),
    StructField("count1", IntegerType, nullable = true),
    StructField("count2", IntegerType, nullable = true)
  ))

  val rdd = spark.sparkContext.parallelize(data)

  val df = spark.createDataFrame(rdd, schema)

  val result = df.rdd.fold(Row(1, 0, 0)){
    case (Row(prevcount0: Int, _, _), Row(_, count1: Int, count2: Int)) =>
      Row(prevcount0 * count1  +  count2, count1, count2)
  }

  val newDf = df.union(spark.createDataFrame(spark.sparkContext.parallelize(List(result)), schema))

  newDf.take(10).foreach(println)
}
