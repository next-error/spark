package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 使用的是全外连接，将左表和右表的数据计算没有join上也显示出来
 *
 */
object C08_FullOuterJoinDemo {

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

    //实现full outer join
    val rdd3: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)

    val res = rdd3.collect

    println(res.toBuffer)

    //5.释放资源
    sc.stop()





  }

}
