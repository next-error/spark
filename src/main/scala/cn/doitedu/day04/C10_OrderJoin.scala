package cn.doitedu.day04

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用LeftOuterjoin关联维度数据
 */
object C10_OrderJoin {

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

    val reduced: RDD[(Int, Double)] = filtered.reduceByKey(_ + _)

    val idToName = Array((1, "家具"), (2, "手机"), (3, "服装"))
    val idToNameRdd: RDD[(Int, String)] = sc.parallelize(idToName)

    //聚合后关联维度数据，使用RDD的join进行关联
    val joined: RDD[(Int, (Double, Option[String]))] = reduced.leftOuterJoin(idToNameRdd)

    val res = joined.map{case (cid, (money, op)) => {
      (cid, money, op.getOrElse("未知"))
    }}.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
