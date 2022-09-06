package day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 读取IP规则数据,然后将IP规则进行广播
 * 再读取用户行为日志,关联缓存的广播数据
 */
object IPLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile("data/order.txt")
    //读取数据

    //将/driver端的ip规则广播出去
    //broadcast方法是阻塞的
    // 广播后,将广播到的每个/executor的数据返回给driver,broadcastRef保存了数据储存在/executor的内存位置

    sc.stop()
  }

}
