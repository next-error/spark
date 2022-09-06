package day10

/**
 * 读取csv数据,指定表头和数据类型,字段分隔符,默认为逗号
 */

import org.apache.spark.sql.{DataFrame, SparkSession}

object demo01_ReadCSV {
  def main(args: Array[String]): Unit = {
    //1.
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()
    //2.
    val df: DataFrame = spark.read
      .option("header","true")  //指定第一行表头
      .option("inferSchema","true")//会读取所有数据,然后推断类型
      .csv("data/exercise/stu.csv")
    df.printSchema()
    df.show()

    spark.stop()

  }

}
