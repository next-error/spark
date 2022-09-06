package day10

import org.apache.spark.sql.SparkSession

/**
 *
 */
object demo05_ReadORC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()

    val df = spark.read.orc("data/orc")
    df.printSchema()
    df.show()
    spark.stop()
  }
}
