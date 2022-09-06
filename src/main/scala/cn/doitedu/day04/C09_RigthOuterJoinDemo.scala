package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 使用的是右外连接，即右表的数据（第二个RDD），没有join上，也会显式出来
 *
 */
object C09_RigthOuterJoinDemo {

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

    //第二个RDD中的数据一定会返回，第一RDD中的数据，可能有，可能没有，所有是Option
    val rdd3: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

    val res = rdd3.collect()
    println(res.toBuffer)

    //5.释放资源
    sc.stop()





  }

}
