package cn.doitedu.day03

import java.lang
import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用spark读取数据，并解析json，然后将数据进行聚合，最后将数据写入到MySQL中
 */
object C01_OrderCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //2.调用sc的source方法，创建RDD
    val lines: RDD[String] = sc.textFile("data/order.json")
    //3.调用Transformation(s)
    val cidAndMoney: RDD[(Int, Double)] = lines.map(line => {

      var tp: (Int, Double) = null
      try {
        val jsonObj = JSON.parseObject(line)
        val cid = jsonObj.getInteger("cid").toInt
        val money = jsonObj.getDouble("money").toDouble
        tp = (cid, money)
      } catch {
        case e: Exception => {
          //记录一下异常信息
        }
      }
      tp
    })
    //过滤，过滤为null的数据

    val filtered = cidAndMoney.filter(_ != null)

    val reduced = filtered.reduceByKey(_ + _)
    val idToName = Map((1, "家具"), (2, "手机"), (3, "服装"))
    //在聚合后关联维度数据
    val res: RDD[(Int, String, Double)] = reduced.map(t => {
      val name = idToName.getOrElse(t._1, "未知")
      (t._1, name, t._2)
    })

    //在客户端定义的mysql连接
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456")
    //4.调用Action，将数据写入到MySQL中
    //写入数据多第一种方式：将数据收集到Driver端后再写入（可以使用客户端创建的连接了）
    //如果返回数据太多，会导致数据丢失或内存溢出，并且一个客户端写入，效率太低
    //只适用于少量数据
    val arr: Array[(Int, String, Double)] = res.collect()
    //调用的是数组的foreach
    arr.foreach(t => {
      val pstm = connection.prepareStatement("insert into t_order_count (cid, name, money) values (?, ?, ?)")
      pstm.setInt(1, t._1)
      pstm.setString(2, t._2)
      pstm.setDouble(3, t._3)
      pstm.executeUpdate()
      pstm.close()
    })
    connection.close()

    //res.foreach(println)
//    res.foreach(t => {
//      //数据的写入是要在Executor中完成
//      //建立连接
//      val pstm = connection.prepareStatement("insert into t_order_count values (null,?, ?, ?)")
//      pstm.setInt(1, t._1)
//      pstm.setString(2, t._2)
//      pstm.setDouble(3, t._3)
//      pstm.executeUpdate()
//      pstm.close()
//    })


    //5.释放资源
    sc.stop()


  }

}
