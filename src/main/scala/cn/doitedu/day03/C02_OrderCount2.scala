package cn.doitedu.day03

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用spark读取数据，并解析json，然后将数据进行聚合，最后将数据写入到MySQL中
 */
object C02_OrderCount2 {

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

    //4.调用Action，将数据写入到MySQL中
    //写入数据多第二种方式：在Executor中直接将数据写入到MySQL
    //适合处理大量数据，可以多个Executor中的多个Task同时写入

    //调用RDD的foreach方法，该方法是一个Action算子，会生成job，将Task提交到集群中运行
    //如果要写入的数据量非常大，foreach算子传入的函数，每一条数据会创建一个mysql的连接，频繁创建MySQL连接，效率低下
    //    res.foreach(t => {
    //      //数据的写入是要在Executor中完成
    //      //建立连接
    //      val connection: Connection = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456")
    //      val pstm = connection.prepareStatement("insert into t_order_count values (null,?, ?, ?)")
    //      pstm.setInt(1, t._1)
    //      pstm.setString(2, t._2)
    //      pstm.setDouble(3, t._3)
    //      pstm.executeUpdate()
    //      pstm.close()
    //      connection.close()
    //    })

    //写入大量数据最优的方式
    //1.在Executor中写入
    //2.一个分区中的多条数据使用一个连接
    //foreachPartition是一个Action算子，数据是以一个分区的形式进行处理，传入到我们定义的函数中的
    res.foreachPartition(it => {
      //先创建一个连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456")
      //将一个分区中的多条数据，使用同一个连接写入
      it.foreach(t => {
        val pstm = connection.prepareStatement("insert into t_order_count values (null,?, ?, ?)")
        pstm.setInt(1, t._1)
        pstm.setString(2, t._2)
        pstm.setDouble(3, t._3)
        pstm.executeUpdate()
        pstm.close()
      })
      connection.close()

    })


    //5.释放资源
    sc.stop()


  }

}
