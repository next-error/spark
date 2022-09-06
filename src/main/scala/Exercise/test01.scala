package Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object test01 {
  def main(args: Array[String]): Unit = {
    //创建spark入口
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SumMoneyByCategories")
    val sc: SparkContext = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile("data/exercise/test01.txt")
    //处理数据,获取商品大类信息及成交额
    val value: RDD[(String, Double)] = lines.map(x => {
      val sp1: Array[String] = x.split(",")
      val sp2: Array[String] = sp1.flatMap(_.split(":"))
      val categories: String = sp2(3).trim
      val money: Double = sp2(5).toDouble
      (categories, money)
    })
    //value.foreach(println)
    val value1: RDD[(String, Double)] = value.reduceByKey(_ + _)
    var commodity_name:String = "a"
    val value2: RDD[(String, String, Double)] = value1.map(x => {
      x._1 match {
        case "1" => commodity_name = "家具"
        case "2" => commodity_name = "手机"
        case "3" => commodity_name = "服装"
        case _ => commodity_name = "没有匹配成功"
      }

      (x._1, commodity_name, x._2)
    })

    //value2.foreach(println)

    //将结果写入MySQL
    value2.foreachPartition(dataTOMySql)
    sc.stop()


  }

val dataTOMySql =(it:Iterator[(String,String,Double)]) =>{
  //创建MySQL链接
  var conn = DriverManager.getConnection("jdbc:mysql://linux01:3306/mySpark","root","root")
  //sql执行语句,需要传入参数
  val statement = conn.prepareStatement("insert into t_result values (null,?,?,?)")
  //sql传参
it.foreach(t => {
  statement.setString(1,t._1)
  statement.setString(2,t._2)
  statement.setDouble(3,t._3)
  //批量写入
  statement.addBatch()
})
  //执行
  statement.executeBatch()
  statement.close()
  conn.close()
}
}
