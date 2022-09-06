package day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object demo_MapPartitions {
  /**
   * 与map类型,但在一些场景下,比map算子效率更高
   *
   * 将RDD中对应的数据以分区的形式(一个分区就是一个迭代器) 取出
   * map 将数据一条条传入
   * MapPartitions 以迭代器的形式传入函数中
   */

  def main(args: Array[String]): Unit = {
    //创建SparkContext的Source方法,创建RDD(Spark上下文,用于创建原始RDD)
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapDemo")
    val sc = new SparkContext(conf)


    //
    val arr = Array(1, 2, 3, 45, 6, 78, 9, 64)
    val rdd1: RDD[Int] = sc.makeRDD(arr,11)//指定分区数量
    //将RDD中对应的数据以分区的形式取出,

    //访问外部数据库首先建立链接,这个链接不能定义在外面(Driver客户端),因为函数中的逻辑会生成Task在Executor中执行,而链接不支持序列化,所以不能再Driver端初始化
    //  先创建好一个链接(在Executor中),然后在函数中使用已经定义好了的链接
    //处理同一个分区的数据,可以使用同一个链接,这样更加高效
    val rdd2 = rdd1.mapPartitions(it => {
      it.map(_ * 10)
    })
    //val res: Array[Int] = rdd2.collect()
   rdd2.saveAsTextFile("out3")



    sc.stop()
  }


}
