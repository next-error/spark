package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

/**
 * 演示两个DataFrame进行join
 *
 * spark sql 内置函数  https://spark.apache.org/docs/latest/api/sql/index.html
 *
 */
object C12_DataFrameJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    //读取订单表，创建DataFrame
    val orderDF = spark.read.json("data/order.json")

    import spark.implicits._
    val filteredDf = orderDF
      .where(
        //$"_corrupt_record".isNull and $"cid".isNotNull
        conditionExpr = "_corrupt_record is null and cid is not null"
      ).drop(
      "_corrupt_record",
      "ttt"
    )

    val categoryDf = spark.read
      .option("header", "true")
      .csv("data/category.txt")

    //默认情况，使用的是inner join
    //val joinDf = filteredDf.join(categoryDf, $"cid" === $"id")

    //实现左外连接，需要在第三个参数指定join的类型
    val joinDf = filteredDf.join(categoryDf, $"cid" === $"id", "left_outer")

    joinDf.show()
  }

}
