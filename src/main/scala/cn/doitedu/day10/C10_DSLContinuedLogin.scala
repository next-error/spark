package cn.doitedu.day10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
 * 使用DSL风格的API实现统计连续登录用户
 */
object C10_DSLContinuedLogin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C10_DSLContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .csv("data/test1.txt")
      .toDF("uid", "dt")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //去重
    val res = df.distinct()
      .select(
        $"uid",
        $"dt",
        //row_number().over(Window.partitionBy("uid").orderBy($"dt".asc)).as("rn")
        row_number() over(Window.partitionBy($"uid").orderBy($"dt" asc)) as "rn"
      ).select(
      $"uid",
      'dt,
      date_sub($"dt", $"rn") as "date_diff"
    ).groupBy(
      "uid",
      "date_diff"
    ).agg(
      min("dt") as "startTime",
      max("dt") as "endTime",
      count("*") as "counts"
    ).drop(
      "date_diff"
    ).where($"counts" >= 3)


    res.show()


    spark.stop()
  }

}
