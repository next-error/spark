package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

/**
 * 读取CSV格式的数据，获取表头字段名称和推断数据类型
 */
object C01_ReadCsvDemo2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C01_ReadCsvDemo2")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段的类型，必须读取数据中的每一条
      .csv("data/person.csv")
    df.printSchema()

    df.show()

    Thread.sleep(88888888)


  }

}
