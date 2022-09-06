package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 使用的是左外连接，即左表的数据（第一个RDD），没有join上，也会显式出来
 *
 */
object C07_LeftOuterJoinDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    //通过并行化的方式创建一个RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    //通过并行化的方式再创建一个RDD
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))

    //在scala中，option有两个实现，一个是Some，一个None
    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)


    //5.释放资源
    sc.stop()





  }

}
