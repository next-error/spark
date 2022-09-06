package day10

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 演示DSL风格API的用法
 */
object demo07_DSL_Select {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()
    val df: DataFrame = spark.read.json("data/exercise/order.json")
    //下面三种方式不可以混搭
    //第一种用法,调用DSL风格API,传入列的名称
    df.select("oid","cid")
    //第二种,传入Column,即将列名转成Column对象,使用$符,需要导入隐士转换
    import spark.implicits._
    df.select($"oid",$"cid")
    //第三种,单引号直接跟列的名称
    df.select('oid, 'cid)


  }}
