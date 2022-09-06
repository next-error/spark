package day11

import org.apache.spark.sql.SparkSession

object demo02_IPLocation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()
    val df1 = spark.read
      .option("delimiter", "|")
      .csv("data/ipaccess.txt")
    val df2 = df1.toDF("time", "ip", "site", "url", "ex_info", "temp1", "temp2")
    df2.createTempView("v_user_access")
    //广播
    //val sc = spark.sparkContext
   // sc.textFile("data/ip.txt")
    //.broadcast(df2)

    val ipdf1 = spark.read
      .option("delimiter", "|")
      .csv("data/ip.txt")
    val ipdf2 = ipdf1.toDF()

    //注册函数
    spark.udf.register("ipLocation",(ip:String)=>{
      ip
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
