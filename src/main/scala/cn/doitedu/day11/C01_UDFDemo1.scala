package cn.doitedu.day11

import org.apache.spark.sql.SparkSession

/**
 * Spark SQL的UDF（User Defined Function）
 *
 * UDF功能：内置的函数无法满足业务的要求，而且还不想在sql中混搭DSL、RDD的程序
 *
 * 根据业务需求，自己定义函数，以后可以在SQL中调用自己定义的函数
 *
 * UDF分类：
 *   1. UDF ： 1进1出
 *   2. UDAF： 多进1出（一个组返回一个结果）
 *   3. UDTF： 1进多出（explode炸裂函数）
 *
 */
object C01_UDFDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("C01_UDFDemo1").master("local[*]").getOrCreate()


    val df = spark.read.csv("data/user2.txt")
    val df2 = df.toDF("name", "age", "fv", "province", "city")

    df2.createTempView("v_log")

    //concat_ws是spark-sql自带的函数，build-in function（内置函数）
//    val df3 = spark.sql(
//      """
//        |
//        |select
//        |  name,
//        |  age,
//        |  fv,
//        |  concat_ws('-', province, city) location
//        |from
//        |  v_log
//        |""".stripMargin)

    //自己写一个类似concat_ws的函数，叫my_concat_ws,可以在sql中调用

    //1.先注册一个自定义函数
    spark.udf.register("my_concat_ws", MyFunctions.myConcatWs)

    //2.调用
    val df3 = spark.sql(
      """
        |
        |select
        |  name,
        |  age,
        |  fv,
        |  my_concat_ws('-', province, city) location
        |from
        |  v_log
        |""".stripMargin)


    //select ip2Location(ip)  就返回  辽宁省大连市

    df3.show()

  }

}
