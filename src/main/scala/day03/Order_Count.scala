package day03

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.lang
import java.sql.DriverManager

object Order_Count {
  /**
   * 使用Spark读取数据并解析JSON,然后将数据聚合,写入mysql
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //1.创建sparkcontext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //调用sc的source方法,创建RDD
    val lines = sc.textFile("data/exercise/order.json")
    //3.调用transformation(s)
    val cidAndMoney: RDD[(Int, Double)] = lines.map(x => {
      var tp: (Int, Double) = null
      try {
        val jSONObject = JSON.parseObject(x)
        val cid: Integer = jSONObject.getInteger("cid").toInt
        val money: lang.Double = jSONObject.getDouble("money")
        tp = (cid, money)
      } catch {
        case e: Exception => {
          //记录异常信息
        }

      }
      tp
    })

    //过滤为null的数据
    val filtered: RDD[(Int, Double)] = cidAndMoney.filter(_ != null)
    val reduced = filtered.reduceByKey(_ + _)
    val idToName = Map((1,"家具"), (2,"收据"), (3,"服装"))

    //聚合后关联维度数据
    val res: RDD[(Int, String, Double)] = reduced.map(t => {
      val name = idToName.getOrElse(t._1, "未知")
      (t._1, name, t._2)
    })
    //4.调用Action,传入一个函数
    //指出问题:在客户端建立的链接,在Excutor中执行

    //建立mysql链接
    DriverManager.getConnection("jdbc:mysql")
    res.foreach(t =>{

    })
    //5.释放资源
    sc.stop()


  }

}
