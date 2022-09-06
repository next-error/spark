package day04

import org.apache.spark.{SparkConf, SparkContext}

object demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("login")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("data/order.txt")
    val uidAndDt = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).distinct()


    uidAndDt.foreach(println)
    sc.stop()
  }

}
