package cn.doitedu.day12

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Coalesce可以shuffle，也可以不shuffle
 */
object CoalesceDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 4)

    //coalesce 第二个参数 shuffle = true，等价于repartition方法
    val rdd2 = rdd1.coalesce(3, true)

    //如果分区数量减少或不变时，可以将shuffle = false
    val rdd3 = rdd1.coalesce(2, false)

    //如果分区区别增大，并且shuffle=false，什么都不做（没有效果）

    rdd2.collect

  }
}
