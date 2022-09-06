package cn.doitedu.day06

import org.apache.spark.{SparkConf, SparkContext}

object C02_TaskThreadDemo3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/date.txt")


    val rdd2 = lines.mapPartitions(it => {
      val dateUtils = new DateUtilsClassNotSer
      it.map(line => {
        dateUtils.parse(line)
      })
    })


    val res = rdd2.collect()

    println(res.toBuffer)
    sc.stop()


  }
}
