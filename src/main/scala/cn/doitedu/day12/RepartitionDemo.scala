package cn.doitedu.day12

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD重新分区
 */
object RepartitionDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)

    //repartition方法一定shuffle
    //不论将分区数量变多、变少、或不变，都shuffle
    val rdd2 = rdd1.repartition(3)

    rdd2.collect

  }
}
