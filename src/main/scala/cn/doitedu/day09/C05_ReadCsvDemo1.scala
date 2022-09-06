package cn.doitedu.day09

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * 使用SparkSQL统一访问数据的方式（各种数据类型，比如JSON、CSV、Parquet）创建DataFram
 *
 */

object C05_ReadJsonDemo1 {

  def main(args: Array[String]): Unit = {


    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("C05_ReadJSONDemo1")
      .master("local[*]")
      .getOrCreate()

    //直接读取Json创建DataFrame，调用spark.read.json方法直接返回DataFrame
    //DataFrame = RDD + Scheam
    //RDD带着普通的描述信息，这个RDD以后从哪里读取数据
    //Schema（额外的描述信息），数据中的字段名称、类型
    //json方法，触发一次Action，触发Action的目的，是为了获取数据中字段名称和类型
    val df: DataFrame = spark.read.json("data/order.json")

    df.printSchema()

    //df.createTempView("v_order")
    //val df2 = spark.sql("select oid, cid, money from v_order where _corrupt_record is null")

    //import spark.implicits._
    //val df2 = df.where($"_corrupt_record" isNull).select($"oid", $"cid", $"money")

    df.show()

    Thread.sleep(88888888)
    //spark.stop()




  }

}
