package day11

import Utils.IpUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object demo03_UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()
    val df1 = spark.read
      .option("delimiter", "|")
      .csv("data/ipaccess.txt")
    val df2 = df1.toDF("time", "ip", "site", "url", "ex_info", "temp1", "temp2")
    df2.createTempView("v_user_access")

    //读取ip归属地文件
    val ipdf1 = spark.read
      .option("delimiter", "|")
      .csv("data/ip.txt")
    val ipdf2 = ipdf1.toDF()
    //ipdf2.show
    import spark.implicits._
    val ipRuleDF: DataFrame = ipdf2.select(
      $"_c2" as "start_ip",
      $"_c3" as "end_ip",
      $"_c6" as "province",
      $"_c7" as "city"
    )
    //将加工好的数据传到driver端并广播出去
    val ipRuleInDriver: Array[(Long, Long, String, String)] = ipRuleDF.map(row => {
      val start_ip = row.getString(0).toLong
      val end_ip = row.getString(1).toLong
      val province = row.getString(2)
      val city = row.getString(3)
      (start_ip, end_ip, province, city)
    }).collect()
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = spark.sparkContext.broadcast(ipRuleInDriver)
    //注册函数
    spark.udf.register("ipLocation",(ip:String)=> {
      //1.注册一个函数，函数的名字叫ip2Location，传入ip字段
        //1.将IP地址转成十进制
        val ipNum = IpUtils.ip2Long(ip)
        //2.二分法查找
        val index = IpUtils.binarySearch(broadcastRef.value, ipNum)
        var location = "未知"
        if (index >= 0) {
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
          |  ipLocation(ip) location
          |from v_user_access
          |""".stripMargin)
      df3.show()
      spark.stop()
    }

}
