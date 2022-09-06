package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 求两个RDD的差集集，统计第二天未登录的用户有哪些
 * 前一天注册的用户有(a, b, c, d, e)
 * 第二个登录的用户  (a, c, f)
 */
object C03_SubtractDemo {

  def main(args: Array[String]): Unit = {


    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "e", "e", "c"), 2)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "c", "f"), 2)

    //计算两个RDD的差集，从第一个RDD中减去在第二个RDD中也出现的数据
    //如果一个元素在第一个RDD中出现对此，并且在第二个rdd中也出现了，那么第一个RDD中的多个相同的元素都会被差掉
    val rdd3: RDD[String] = rdd1.subtract(rdd2)

    val res = rdd3.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()
  }

}
