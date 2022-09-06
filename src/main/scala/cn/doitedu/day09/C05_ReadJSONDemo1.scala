package cn.doitedu.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * 使用SparkSQL统一访问数据的方式（各种数据类型，比如JSON、CSV、Parquet）创建DataFram
 *
 */

object C05_ReadCsvDemo1 {

  def main(args: Array[String]): Unit = {


    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("C05_ReadJSONDemo1")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read.csv("data/data.txt")
    val df2 = df.toDF("uid", "startTime", "endTime", "down_flow")
    //df.printSchema()

    df2.createTempView("v_user_flow")

    spark.sql(
      s"""
        |select
        |  uid,
        |  startTime
        |from v_user_flow
        |
        |""".stripMargin)


    df.show()

    Thread.sleep(88888888)
    //spark.stop()


  }

}
