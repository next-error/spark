package cn.doitedu.day06

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object C02_TaskThreadDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/date.txt")

    //不使用Driver端初始化的object
    //val dateUtils = DateUtils

    //使用Driver初始化的class实例
    val dateUtils = new DateUtilsClass

    val rdd2 = lines.map(line => {
      val time = dateUtils.parse(line)
      time
    })

    val res = rdd2.collect()

    println(res.toBuffer)
    sc.stop()


  }
}
