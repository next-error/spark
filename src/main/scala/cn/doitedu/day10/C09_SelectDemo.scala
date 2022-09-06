package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

/**
 * 演示SparkSQL的DSL风格的API的用法
 */
object C09_SelectDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C08_ContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.json("data/order.json")
    //df.printSchema()

    //第一种用法，调用DSL风格API的方法，传入列的名称
    df.select("oid", "cid")

    //第二种方法，传入Column，即将列名转成Column对象，使用$符号，并且必须导入隐式转换
    import spark.implicits._
    df.select($"oid", $"cid")

    //第三种方式，单引号，后面直接跟列的名称
    df.select('oid, 'cid)

    //不能混写
    //df.select("oid", $"cid")



  }

}
