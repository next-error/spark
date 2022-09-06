package cn.doitedu.day10

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 读写数据时，加入一些参数
 *
 */
object C06_WriteModeDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C05_WriteToJDBCDemo")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段类型
      .option("delimiter", "|") //指定字段的分隔符，默认是逗号
      .format("csv").load("data/person2.csv")

    df.printSchema()

    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    val url = "jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=UTF-8"


    //将数据保存到MySQL中
    //写入的时候指定一些参数
    df.write
      //默认的saveMode是SaveMode: ErrorIfExists.
      //.mode(SaveMode.Append) //写入的方式：追加
      //  .mode(SaveMode.Overwrite) //覆盖，清空表，然后再重新写入数据
      .mode(SaveMode.Ignore) //忽略
      .jdbc(url, "t_person", props)

  }

}
