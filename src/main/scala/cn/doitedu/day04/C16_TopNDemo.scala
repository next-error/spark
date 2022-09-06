package cn.doitedu.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * top方法是Action，用于计算RDD中的对应的数据最大的前n个
 */
object C16_TopNDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array(2,4,1,   3,8,5,   9,6,7)
    val rdd1 = sc.parallelize(arr, 3)

    //先在每个分区内计算出topN，然后将每个分区返回的结果，再计算topN
    val res: Array[Int] = rdd1.top(2)

    println(res.toBuffer)

    sc.stop()
  }

}
