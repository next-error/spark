package day10

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 两个DataFrame join
 */
object demo11_DataFrameJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()

    //
    val orderDF: DataFrame = spark.read.json("data/exercise/order.json")
    import spark.implicits._
    //orderDF.where("_corrupt_record".isNull)
    orderDF.printSchema()
    orderDF.show()
    spark.stop()
  }
}
