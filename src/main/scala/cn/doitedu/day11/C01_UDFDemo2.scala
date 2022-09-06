package cn.doitedu.day11

import cn.doitedu.day05.IpUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 实现一个自定义函数，实现根据ip地址计算归属地
 *
 * select ip2Location(ip) location from v_user_access
 *
 *
 */
object C01_UDFDemo2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("C01_UDFDemo1").master("local[*]").getOrCreate()


    val df = spark.read
        .option("delimiter", "|")
      .csv("data/ipaccess.txt")
    val df2 = df.toDF("time", "ip", "site", "url", "ex_info", "temp1", "temp2")

    df2.createTempView("v_user_access")

    val ipDf = spark.read
      .option("delimiter", "|")
      .csv("data/ip.txt")

    import spark.implicits._
    val ipRuleDF: DataFrame = ipDf.select(
      $"_c2" as "startNum",
      $"_c3" as "endNum",
      $"_c6" as "province",
      $"_c7" as "city"
    )
    //0.将加工好的数据，先广播出去
    val ipRulesInDriver: Array[(Long, Long, String, String)] = ipRuleDF.map(row => {
      val startNum = row.getString(0).toLong
      val endNum = row.getString(1).toLong
      val province = row.getString(2)
      val city = row.getString(3)
      (startNum, endNum, province, city)
    }).collect()
    //0.将ip规则广播出去(将广播后的数据描述信息返回到Driver端)
    val broadcastRef = spark.sparkContext.broadcast(ipRulesInDriver)

    //1.注册一个函数，函数的名字叫ip2Location，传入ip字段
    spark.udf.register("ip2Location", (ip: String) => {
      //1.将IP地址转成十进制
      val ipNum = IpUtils.ip2Long(ip)
      //2.二分法查找
      val index = IpUtils.binarySearch(broadcastRef.value, ipNum)
      var location = "未知"
      if(index >= 0) {
        val tp = broadcastRef.value(index)
        location = tp._3 + tp._4
      }
      location
    })

    val df3 = spark.sql(
      """
        |select
        |  time,
        |  ip,
        |  site,
        |  url,
        |  ip2Location(ip) location
        |from v_user_access
        |""".stripMargin)


    df3.show(10)

  }

}
