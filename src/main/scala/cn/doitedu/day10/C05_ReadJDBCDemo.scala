package cn.doitedu.day10

import java.util.Properties

import org.apache.spark.sql.SparkSession

object C05_ReadJDBCDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C05_ReadJDBCDemo")
      .master("local[*]")
      .getOrCreate()

    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    val url = "jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=UTF-8"

    //jdbc方法不会触发Action，那么如何获取到schema信息呢？
    //调用spark.read.jdbc在Driver端会连接一次MySQL，获取到MySQL的对应表的schema信息
    val df = spark.read.jdbc(url, "t_order_count", props)

    df.printSchema()

    //触发Action后，会生成Task去读取数据
    df.show()

    Thread.sleep(8888888)

  }

}
