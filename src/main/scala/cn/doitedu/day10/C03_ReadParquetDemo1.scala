package cn.doitedu.day10

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 直接读取parquet文件返回DataFrame
 *
 */
object C03_ReadParquetDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C03_ReadParquetDemo1")
      .master("local[*]")
      .getOrCreate()

    //直接返回DataFrame
    val df: DataFrame = spark.read.parquet("data/parquet")
    df.printSchema()
    df.show()

    Thread.sleep(888888)

  }

}
