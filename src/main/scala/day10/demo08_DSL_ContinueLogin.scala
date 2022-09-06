package day10

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 演示DSL风格API的用法
 */
object demo08_DSL_ContinueLogin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()
    val df: DataFrame = spark.read.csv("data/order.txt").toDF("uid", "dt")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val res = df.distinct()
      .select(
        $"uid",
        $"dt",
        row_number() over (Window.partitionBy($"uid").orderBy($"dt" asc)) as "rn"
      )
      .select(
        'uid,
        'dt,
        date_sub('dt,$"rn") as "date_diff"
      )
      .groupBy(
        "uid",
        "date_diff"
      )
      .agg(
        min("dt") as "min_dt",
        max("dt") as "max_dt",
        count("*") as "counts"
      ).drop("date_diff")
      .where(
      $"counts" >= 3
    )


    res.show()
  }
}