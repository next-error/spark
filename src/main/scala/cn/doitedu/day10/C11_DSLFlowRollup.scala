package cn.doitedu.day10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
 * 使用DSL风格的API实现统计流量
 */
object C11_DSLFlowRollup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C11_DSLFlowRollup")
      .master("local[*]")
      .getOrCreate()


    val df = spark.read
      .option("header", "true")
      .csv("data/data.csv")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val res = df.select(
      $"uid",
      $"start_time",
      $"end_time",
      $"flow",
      //lag($"end_time", 1, "start_time") over(Window.partitionBy("uid").orderBy("start_time")) as "lag_time"
      expr("lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time")
    ).select(
      'uid,
      'start_time,
      'end_time,
      'flow,
      expr("if((to_unix_timestamp(start_time) - to_unix_timestamp(lag_time)) / 60 > 10, 1, 0) flag")
    ).select(
      'uid,
      'start_time,
      'end_time,
      'flow,
      sum($"flag") over(Window.partitionBy("uid").orderBy("start_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)) as "sum_flag"
    ).groupBy(
      "uid",
      "sum_flag"
    ).agg(
      min("start_time") as "start_time",
      max("end_time") as "end_time",
      sum("flow") as "total_flow"
    ).drop(
      "sum_flag"
    )


    res.show()

    spark.stop()
  }

}
