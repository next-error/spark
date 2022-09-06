package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo_MapPartitionsWithIndex {
  /**
   * 与map类型,但在一些场景下,比map算子效率更高
   *
   * 将RDD中对应的数据以分区的形式(一个分区就是一个迭代器) 取出
   * map 将数据一条条传入
   * MapPartitions 以迭代器的形式传入函数中
   *在有外部链接的情况下,一个链接可以一口气处理迭代器中的多条数据,效率更高
   * MapPartitionsWithIndex,取出分区对应迭代器的同时,取出了分区编号
   */

  def main(args: Array[String]): Unit = {
    //创建SparkContext的Source方法,创建RDD(Spark上下文,用于创建原始RDD)
    val conf = new SparkConf().setMaster("local[5]").setAppName("mapDemo")
    val sc = new SparkContext(conf)


    //
    val arr = Array(1, 2, 3, 45, 6, 78, 9, 64)
    val rdd1: RDD[Int] = sc.makeRDD(arr,7)
    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map(e => {
        (index, e)
      })
    })
    rdd2.saveAsTextFile("out2")
    //val res: Array[(Int, Int)] = rdd2.collect()
    //println(res.toBuffer)


    sc.stop()
  }


}
