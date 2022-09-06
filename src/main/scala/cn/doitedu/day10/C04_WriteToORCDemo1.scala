package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

/**
 * ROC文件格式也是列式存储，支持压缩，也有schema，也支持映射下推和谓词下推
 *
 */
object C04_WriteToORCDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C04_WriteToORCDemo1")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段类型
      .option("delimiter", "|")  //指定字段的分隔符，默认是逗号
      .csv("data/person2.csv")

    df.printSchema()

    //将数据保存成Parquet格式
    df.write.orc("data/orc")

  }

}
