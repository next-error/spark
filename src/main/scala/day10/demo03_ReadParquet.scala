package day10

import org.apache.spark.sql.SparkSession

/**
 * 读parquet格式文件,只读了schema信息,不需要像JSON或CSV文件似的要全部读完
 * 方便查询,最常用
 */
object demo03_ReadParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()

    val df = spark.read.parquet("data/parquet")
    df.printSchema()
    df.show()
    spark.stop()
  }
}
