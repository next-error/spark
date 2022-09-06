package cn.doitedu.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregate是Action算子，对RDD中的数据进行聚合,可以传入两个函数，局部聚合和全局聚合的逻辑可以不一样
 */
object C14_AggregateDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd2 = sc.parallelize(List("a","b","c","d","e","f"),2)
    val res = rdd2.aggregate("#")(_ + _, _ + _)

    println(res)

    sc.stop()
  }

}
