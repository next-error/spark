package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

object C08_ContinuedLogin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C08_ContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.csv("data/test1.txt")
    val df2 = df.toDF("uid", "dt")

    df2.createTempView("v_login")

    val df3 = spark.sql(
      """
        |select
        |  uid,
        |  dt,
        |  row_number() over(partition by uid order by dt asc) rn
        |from
        |(
        |  select distinct
        |    uid, dt
        |  from
        |    v_login
        |)
        |""".stripMargin)

    df3.createTempView("v_tmp1")

    val maxDt = 3

    val df4 = spark.sql(
      s"""
        |select
        |  uid,
        |  min(dt) start_time,
        |  max(dt) end_time,
        |  count(*) counts
        |from
        |(
        |  select
        |    uid,
        |    dt,
        |    date_sub(dt, rn) date_dif
        |  from
        |    v_tmp1
        |)
        |group by
        |  uid, date_dif
        |having counts >= $maxDt
        |""".stripMargin)



    df4.show()







  }

}
