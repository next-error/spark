package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

object SQLFlowRollupDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val df = spark.read
        .option("header", "true")
      .csv("data/data.csv")

    df.createTempView("v_flow")

    val res = spark.sql(
      """
        |select
        |  uid,
        |  min(start_time) start_time,
        |  max(end_time) end_time,
        |  sum(flow) total_flow
        |from
        |(
        |  select
        |    uid,
        |    start_time,
        |    end_time,
        |    flow,
        |    sum(flag) over(partition by uid order by start_time asc) sum_flag
        |  from
        |  (
        |    select
        |      uid,
        |      start_time,
        |      end_time,
        |      flow,
        |      if((to_unix_timestamp(start_time) - to_unix_timestamp(lag_time)) / 60 > 10, 1, 0) flag
        |    from
        |    (
        |      select
        |        uid,
        |        start_time,
        |        end_time,
        |        flow,
        |        lag(end_time, 1) over(partition by uid order by start_time) lag_time
        |      from v_flow
        |    ) t1
        |  ) t2
        |) t3 group by uid, sum_flag
        |
        |
        |""".stripMargin)

    res.show()
    //Thread.sleep(10000000)
  }
}
