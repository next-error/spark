package day10

import org.apache.spark.sql.{DataFrame, SparkSession}

object demo09_DSL_FlowRollup {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()
    val df: DataFrame = spark.read.csv("data/order.txt").toDF("uid", "dt")
  }

}
