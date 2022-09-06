package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sum对RDD中的数据求和
 */
object C13_SumDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array(1,2,3   ,4,5,6,   7,8,9)
    val rdd1 = sc.parallelize(arr, 3)

    //sum底层调用的是rdd的fold方法，先在每个分区内求和，然后将没法分区计算完的结果返回到Driver，在进行全局求和
    val res: Double = rdd1.sum

    println(res)

    sc.stop()
  }

}
