package day09

/**
 * SparkSession类型读取JSON文件,直接返回DataFrame类型
 */

import org.apache.spark.sql.{DataFrame, SparkSession}

object demo05_ReadJSON {
  def main(args: Array[String]): Unit = {
    //1.
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadJSON")
      .getOrCreate()
    //2.
    val df: DataFrame = spark.read.json("data/exercise/order.json")
    df.printSchema()
    df.show()

    spark.stop()

  }

}
