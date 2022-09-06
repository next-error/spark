package cn.doitedu.day06

import org.apache.spark.{SparkConf, SparkContext}

object C02_TaskThreadDemo2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/date.txt")



    val rdd2 = lines.map(line => {
      //也会有线程安全的问题
      //DateUtilsNotSer在Exutor中初始化的，但是它是个object，单例的，一个进程中只有一个实例
      val time = DateUtilsNotSer.parse(line)
      time
    })

    val res = rdd2.collect()

    println(res.toBuffer)
    sc.stop()


  }
}
