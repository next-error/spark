package cn.doitedu.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd的min和max，不是将rdd的数据全局排序，取出最大的或最小的
 */
object C15_MinMaxDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array(2,4,1,   3,8,5,   9,6,7)
    val rdd1 = sc.parallelize(arr, 3)

    //val res: Int = rdd1.min()
    //在每个分区内先两个比较大小，然后将每个分区的结果在返回到客户端，在比较大小
    val res = rdd1.reduce((a, b) => Math.min(a,b))


    println(res)

    sc.stop()
  }

}
