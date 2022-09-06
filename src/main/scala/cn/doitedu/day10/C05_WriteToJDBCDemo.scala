package cn.doitedu.day10

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * 读取csv文件，然后将数据写入到MySQL中
 *
 */
object C05_WriteToJDBCDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C05_WriteToJDBCDemo")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段类型
      .option("delimiter", "|")  //指定字段的分隔符，默认是逗号
      .csv("data/person2.csv")

    df.printSchema()

    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    val url = "jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=UTF-8"


    //将数据保存到MySQL中
    //根据DataFrame中的schema信息，先创建表，再写入数据
    df.write.jdbc(url, "t_person", props)

  }

}
