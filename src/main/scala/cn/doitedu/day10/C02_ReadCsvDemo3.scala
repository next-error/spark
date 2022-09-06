package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

object C02_ReadCsvDemo3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C02_ReadCsvDemo2")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段类型
      .option("delimiter", "|")  //指定字段的分隔符，默认是逗号
      .csv("data/person2.csv")

    df.printSchema()

    df.write
      .option("header", "true")
      .option("delimiter", ";")
      .csv("data/csv")

    //df.show()

    //Thread.sleep(88888888)


  }

}
